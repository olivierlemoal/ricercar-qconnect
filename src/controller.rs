//! Qobuz Connect controller side: what the server tells controllers about
//! the account's session (renderers, the active one, the cloud queue, each
//! renderer's playback), and the messages that drive a renderer.
//!
//! Pure state: `plugin::outputs` feeds it the session's inbound messages and
//! sends what it builds over the session's WebSocket.

use std::collections::{BTreeMap, HashMap};

use crate::lan::normalize_uuid;
use crate::msgtype::{msg, playing_state, renderer_status, volume_remote_control};
use crate::proto::{
    CtrlSrvrAskForQueueState, CtrlSrvrAskForRendererState, CtrlSrvrMuteVolume, CtrlSrvrQueueAddTracks,
    CtrlSrvrQueueLoadTracks, CtrlSrvrQueueRemoveTracks, CtrlSrvrSetActiveRenderer, CtrlSrvrSetAutoplayMode,
    CtrlSrvrSetLoopMode, CtrlSrvrSetPlayerState, CtrlSrvrSetPlayerStateQueueItem, CtrlSrvrSetVolume, DeviceInfo,
    QConnectMessage, QueueTrackRef, QueueVersion,
};

/// A renderer of the session.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Renderer {
    pub id: i32,
    /// Normalised device uuid (`lan::normalize_uuid`), the output id.
    pub uuid: Option<String>,
    pub name: String,
    pub brand: String,
    pub model: String,
    /// The device refuses remote volume changes.
    pub fixed_volume: bool,
    pub volume: Option<u32>,
    pub muted: Option<bool>,
    pub player: Option<RemotePlayer>,
    /// Left the session cleanly (`ACTIVE_DISCONNECTED`).
    pub disconnected: bool,
    /// Announced since the last (re)join.
    pub seen: bool,
}

impl Renderer {
    fn set_info(&mut self, info: &DeviceInfo) {
        if let Some(uuid) = info.device_uuid.as_deref().and_then(|b| uuid::Uuid::from_slice(b).ok()) {
            self.uuid = normalize_uuid(&uuid.to_string());
        }
        if let Some(name) = &info.friendly_name {
            self.name = name.clone();
        }
        if let Some(brand) = &info.brand {
            self.brand = brand.clone();
        }
        if let Some(model) = &info.model {
            self.model = model.clone();
        }
        if let Some(caps) = &info.capabilities {
            self.fixed_volume = caps.volume_remote_control == Some(volume_remote_control::NOT_ALLOWED);
        }
    }

    /// "Brand Model", without repeating a brand the model already starts with.
    pub fn brand_model(brand: &str, model: &str) -> String {
        let (brand, model) = (brand.trim(), model.trim());
        if model.to_lowercase().starts_with(&brand.to_lowercase()) {
            model.to_string()
        } else {
            format!("{brand} {model}").trim().to_string()
        }
    }
}

/// A renderer's playback, from `SrvrCtrlRendererStateUpdated`.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct RemotePlayer {
    pub playing_state: i32,
    pub buffer_state: i32,
    /// Position when the update arrived.
    pub position_ms: u64,
    /// Local clock (ms epoch) of that update: positions are extrapolated from
    /// it, the renderer's own timestamp may come from a skewed clock.
    pub at_ms: u64,
    pub duration_ms: Option<u64>,
    /// `None`: no item (-1).
    pub queue_item: Option<i32>,
}

impl RemotePlayer {
    /// Position now, extrapolated while playing, within the duration.
    pub fn position_at(&self, now_ms: u64) -> u64 {
        let mut pos = self.position_ms;
        if self.playing_state == playing_state::PLAYING {
            pos += now_ms.saturating_sub(self.at_ms);
        }
        match self.duration_ms {
            Some(d) => pos.min(d),
            None => pos,
        }
    }
}

/// Copy of the cloud queue: queue item id → track id, in order.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Queue {
    pub version: Option<QueueVersion>,
    pub items: Vec<(i32, u32)>,
    /// Tracks autoplay appended after the queue.
    pub autoplay: Vec<(i32, u32)>,
    pub autoplay_mode: Option<bool>,
    /// A full `QueueState` arrived since the last (re)join.
    pub known: bool,
}

impl Queue {
    pub fn track_of(&self, queue_item: i32) -> Option<u32> {
        self.items.iter().chain(&self.autoplay).find(|(q, _)| *q == queue_item).map(|(_, t)| *t)
    }

    /// The item following `queue_item`.
    pub fn after(&self, queue_item: i32) -> Option<(i32, u32)> {
        let i = self.items.iter().position(|(q, _)| *q == queue_item)?;
        self.items.get(i + 1).copied()
    }

    fn set_version(&mut self, version: Option<QueueVersion>) {
        if version.is_some() {
            self.version = version;
        }
    }

    fn insert_after(&mut self, after: Option<i32>, items: Vec<(i32, u32)>) {
        let at = match after {
            Some(q) if q >= 0 => self.items.iter().position(|(i, _)| *i == q).map_or(self.items.len(), |i| i + 1),
            _ => 0,
        };
        self.items.splice(at..at, items);
    }
}

/// The queue entries of a server message. Proto3 servers omit a zero queue
/// item id, so absent means 0 (the first item).
fn entries(tracks: &[QueueTrackRef]) -> Vec<(i32, u32)> {
    tracks.iter().filter_map(|t| Some((t.queue_item_id.unwrap_or(0), t.track_id?))).collect()
}

/// Outcome of a queue action, matched by its action uuid.
#[derive(Debug, Clone, PartialEq)]
pub enum Ack {
    /// Applied; the entries it created (loads, additions).
    Done(Vec<(i32, u32)>),
    Refused(String),
}

/// A renderer's failure on a queue item (`PlaybackError`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ItemError {
    pub queue_item: i32,
    pub kind: i32,
}

#[derive(Debug, Default)]
pub struct CtrlState {
    own_uuid: Option<String>,
    pub own_renderer: Option<i32>,
    pub connected: bool,
    /// Session uuid (string form), from `SrvrCtrlSessionState`.
    pub session_uuid: Option<String>,
    pub renderers: BTreeMap<i32, Renderer>,
    pub active: Option<i32>,
    pub loop_mode: Option<i32>,
    pub queue: Queue,
    /// Last playback error of the active renderer.
    pub item_error: Option<ItemError>,
    /// Queue actions in flight, by action uuid.
    pending: HashMap<Vec<u8>, Option<Ack>>,
    /// The queue copy is unreliable: ask for the whole queue again.
    pub resync: bool,
}

impl CtrlState {
    pub fn new(own_uuid: &str) -> Self {
        Self { own_uuid: normalize_uuid(own_uuid), ..Default::default() }
    }

    /// A (re)join: everything is announced again. Renderers stay listed
    /// until [`CtrlState::forget_unseen`], so a reconnection does not make
    /// them blink.
    pub fn on_connected(&mut self) {
        self.connected = true;
        self.own_renderer = None;
        self.session_uuid = None;
        self.queue = Queue::default();
        for r in self.renderers.values_mut() {
            r.seen = false;
        }
        for ack in self.pending.values_mut() {
            *ack = Some(Ack::Refused("Qobuz Connect reconnected".into()));
        }
    }

    pub fn on_disconnected(&mut self) {
        self.connected = false;
        for ack in self.pending.values_mut() {
            *ack = Some(Ack::Refused("Qobuz Connect disconnected".into()));
        }
    }

    /// Drop renderers not announced again since the last join.
    pub fn forget_unseen(&mut self) {
        self.renderers.retain(|_, r| r.seen);
    }

    /// Renderers other than ours.
    pub fn others(&self) -> impl Iterator<Item = &Renderer> {
        self.renderers.values().filter(|r| Some(r.id) != self.own_renderer && (r.uuid.is_none() || r.uuid != self.own_uuid))
    }

    pub fn renderer_by_uuid(&self, uuid: &str) -> Option<&Renderer> {
        self.others().find(|r| r.uuid.as_deref() == Some(uuid))
    }

    /// Register a queue action; its uuid goes in the message.
    pub fn begin_action(&mut self) -> Vec<u8> {
        let uuid = uuid::Uuid::new_v4().as_bytes().to_vec();
        self.pending.insert(uuid.clone(), None);
        uuid
    }

    /// The outcome of an action, once known. Forgets the action.
    pub fn take_ack(&mut self, action: &[u8]) -> Option<Ack> {
        if self.pending.get(action)?.is_none() {
            return None;
        }
        self.pending.remove(action).flatten()
    }

    pub fn cancel_action(&mut self, action: &[u8]) {
        self.pending.remove(action);
    }

    pub fn apply(&mut self, m: &QConnectMessage, now_ms: u64) {
        if let Some(s) = &m.srvr_ctrl_session_state {
            if let Some(u) = s.session_uuid.as_deref().and_then(|u| uuid::Uuid::from_slice(u).ok()) {
                self.session_uuid = Some(u.to_string());
            }
            if s.active_renderer_id.is_some() {
                self.active = s.active_renderer_id.filter(|&id| id >= 0);
            }
            if s.loop_mode.is_some() {
                self.loop_mode = s.loop_mode;
            }
        }
        if let Some(a) = &m.srvr_ctrl_add_renderer {
            self.upsert(a.renderer_id, a.device_info.as_ref());
        }
        if let Some(u) = &m.srvr_ctrl_update_renderer {
            self.upsert(u.renderer_id, u.device_info.as_ref());
        }
        if let Some(id) = m.srvr_ctrl_remove_renderer.as_ref().and_then(|r| r.renderer_id) {
            self.renderers.remove(&id);
        }
        if let Some(c) = &m.srvr_ctrl_active_renderer_changed {
            self.active = c.active_renderer_id.filter(|&id| id >= 0);
            self.item_error = None;
        }
        if let Some(v) = &m.srvr_ctrl_volume_changed {
            if let Some(r) = v.renderer_id.and_then(|id| self.renderers.get_mut(&id)) {
                r.volume = Some(v.volume.unwrap_or(0).min(100));
            }
        }
        if let Some(v) = &m.srvr_ctrl_volume_muted {
            if let Some(r) = v.renderer_id.and_then(|id| self.renderers.get_mut(&id)) {
                r.muted = Some(v.value.unwrap_or(false));
            }
        }
        if let Some(s) = &m.srvr_ctrl_renderer_state_updated {
            if let Some(r) = s.renderer_id.and_then(|id| self.renderers.get_mut(&id)) {
                r.disconnected = s.status == Some(renderer_status::ACTIVE_DISCONNECTED);
                if let Some(ps) = &s.player_state {
                    let prev = r.player.unwrap_or_default();
                    // Proto3 server: an absent scalar is 0. An unknown playing
                    // state keeps the previous one.
                    let queue_item = Some(ps.current_queue_item_id.unwrap_or(0)).filter(|&q| q >= 0);
                    let same_item = r.player.is_some() && queue_item == prev.queue_item;
                    let position_ms = match &ps.current_position {
                        Some(p) => p.value.unwrap_or(0) as u64,
                        None if same_item => prev.position_at(now_ms),
                        None => 0,
                    };
                    r.player = Some(RemotePlayer {
                        playing_state: match ps.playing_state.unwrap_or(0) {
                            playing_state::UNKNOWN => prev.playing_state,
                            other => other,
                        },
                        buffer_state: ps.buffer_state.unwrap_or(0),
                        position_ms,
                        at_ms: now_ms,
                        duration_ms: match ps.duration.filter(|&d| d > 0) {
                            Some(d) => Some(d as u64),
                            None if same_item => prev.duration_ms,
                            None => None,
                        },
                        queue_item,
                    });
                }
            }
        }
        if let Some(e) = &m.playback_error {
            self.item_error = Some(ItemError { queue_item: e.queue_item_id.unwrap_or(0), kind: e.error_type.unwrap_or(0) });
        }
        if let Some(l) = &m.srvr_ctrl_loop_mode_set {
            self.loop_mode = l.loop_mode;
        }
        self.apply_queue(m);
    }

    fn upsert(&mut self, id: Option<i32>, info: Option<&DeviceInfo>) {
        let Some(id) = id else { return };
        let r = self.renderers.entry(id).or_insert_with(|| Renderer { id, ..Default::default() });
        if let Some(info) = info {
            r.set_info(info);
        }
        r.seen = true;
        let uuid = r.uuid.clone();
        if uuid.is_some() && uuid == self.own_uuid {
            self.own_renderer = Some(id);
        }
        // The same device under a new id (it joined again): one entry.
        if uuid.is_some() {
            self.renderers.retain(|&other, r| other == id || r.uuid != uuid);
        }
    }

    fn apply_queue(&mut self, m: &QConnectMessage) {
        let q = &mut self.queue;
        let pending = &mut self.pending;
        if let Some(s) = &m.srvr_ctrl_queue_state {
            q.set_version(s.queue_version);
            q.items = entries(&s.tracks);
            q.autoplay = entries(&s.autoplay_tracks);
            q.autoplay_mode = s.autoplay_mode.or(Some(false));
            q.known = true;
            self.resync = false;
            ack(pending, &s.action_uuid, Ack::Done(q.items.clone()));
        }
        if let Some(s) = &m.srvr_ctrl_queue_tracks_loaded {
            q.set_version(s.queue_version);
            q.items = entries(&s.tracks);
            if s.autoplay_reset == Some(true) {
                q.autoplay.clear();
            }
            ack(pending, &s.action_uuid, Ack::Done(q.items.clone()));
        }
        if let Some(s) = &m.srvr_ctrl_queue_tracks_added {
            q.set_version(s.queue_version);
            let added = entries(&s.tracks);
            q.items.extend(added.iter().copied());
            ack(pending, &s.action_uuid, Ack::Done(added));
        }
        if let Some(s) = &m.srvr_ctrl_queue_tracks_inserted {
            q.set_version(s.queue_version);
            let added = entries(&s.tracks);
            q.insert_after(s.insert_after, added.clone());
            ack(pending, &s.action_uuid, Ack::Done(added));
        }
        if let Some(s) = &m.srvr_ctrl_queue_tracks_removed {
            q.set_version(s.queue_version);
            q.items.retain(|(i, _)| !s.queue_item_ids.contains(i));
            ack(pending, &s.action_uuid, Ack::Done(vec![]));
        }
        if let Some(s) = &m.srvr_ctrl_queue_tracks_reordered {
            q.set_version(s.queue_version);
            let moved: Vec<(i32, u32)> =
                s.queue_item_ids.iter().filter_map(|id| q.items.iter().find(|(i, _)| i == id).copied()).collect();
            q.items.retain(|(i, _)| !s.queue_item_ids.contains(i));
            q.insert_after(s.insert_after, moved);
            ack(pending, &s.action_uuid, Ack::Done(vec![]));
        }
        if let Some(s) = &m.srvr_ctrl_queue_cleared {
            q.set_version(s.queue_version);
            q.items.clear();
            ack(pending, &s.action_uuid, Ack::Done(vec![]));
        }
        if let Some(s) = &m.srvr_ctrl_shuffle_mode_set {
            // Shuffle reorders playback, not the item ids this copy maps.
            q.set_version(s.queue_version);
            ack(pending, &s.action_uuid, Ack::Done(vec![]));
        }
        if let Some(s) = &m.srvr_ctrl_autoplay_mode_set {
            q.set_version(s.queue_version);
            q.autoplay_mode = s.autoplay_mode.or(Some(false));
            ack(pending, &s.action_uuid, Ack::Done(vec![]));
        }
        if let Some(s) = &m.srvr_ctrl_autoplay_tracks_loaded {
            q.set_version(s.queue_version);
            q.autoplay = entries(&s.tracks);
            ack(pending, &s.action_uuid, Ack::Done(vec![]));
        }
        if let Some(s) = &m.srvr_ctrl_autoplay_tracks_removed {
            q.set_version(s.queue_version);
            q.autoplay.retain(|(i, _)| !s.queue_item_ids.contains(i));
            ack(pending, &s.action_uuid, Ack::Done(vec![]));
        }
        if let Some(s) = &m.srvr_ctrl_queue_tracks_added_from_autoplay {
            // Autoplay entries moved into the queue: positions unknown.
            q.set_version(s.queue_version);
            self.resync = true;
        }
        if let Some(e) = &m.srvr_ctrl_queue_error_message {
            let error = e.error.as_ref();
            let text = error.and_then(|e| e.message.clone().or(e.code.clone())).unwrap_or_else(|| "queue error".into());
            tracing::info!("Qobuz Connect refused a queue change: {text}");
            if e.action_uuid.is_some() {
                ack(pending, &e.action_uuid, Ack::Refused(text));
            } else {
                // Version mismatches come without action uuid: every action
                // in flight failed.
                for slot in pending.values_mut() {
                    *slot = Some(Ack::Refused(text.clone()));
                }
            }
            self.resync = true;
        }
    }
}

fn ack(pending: &mut HashMap<Vec<u8>, Option<Ack>>, action: &Option<Vec<u8>>, outcome: Ack) {
    if let Some(slot) = action.as_deref().and_then(|a| pending.get_mut(a)) {
        *slot = Some(outcome);
    }
}

// ---------- controller → server messages ----------

fn message(message_type: i32) -> QConnectMessage {
    QConnectMessage { message_type: Some(message_type), ..Default::default() }
}

pub fn set_active_renderer(renderer: i32) -> QConnectMessage {
    QConnectMessage {
        ctrl_srvr_set_active_renderer: Some(CtrlSrvrSetActiveRenderer { renderer_id: Some(renderer) }),
        ..message(msg::CTRL_SRVR_SET_ACTIVE_RENDERER)
    }
}

pub fn ask_for_renderer_state(renderer: i32) -> QConnectMessage {
    QConnectMessage {
        ctrl_srvr_ask_for_renderer_state: Some(CtrlSrvrAskForRendererState { renderer_id: Some(renderer) }),
        ..message(msg::CTRL_SRVR_ASK_FOR_RENDERER_STATE)
    }
}

pub fn ask_for_queue_state(version: Option<QueueVersion>, action: Vec<u8>) -> QConnectMessage {
    QConnectMessage {
        ctrl_srvr_ask_for_queue_state: Some(CtrlSrvrAskForQueueState {
            queue_version_ref: Some(version.unwrap_or_default()),
            action_uuid: Some(action),
        }),
        ..message(msg::CTRL_SRVR_ASK_FOR_QUEUE_STATE)
    }
}

/// Replace the queue with `tracks`, starting at the first one; autoplay
/// entries are dropped.
pub fn load_tracks(version: Option<QueueVersion>, action: Vec<u8>, tracks: &[u32]) -> QConnectMessage {
    QConnectMessage {
        ctrl_srvr_queue_load_tracks: Some(CtrlSrvrQueueLoadTracks {
            queue_version_ref: Some(version.unwrap_or_default()),
            action_uuid: Some(action),
            track_ids: tracks.to_vec(),
            queue_position: Some(0),
            shuffle_seed: None,
            shuffle_pivot_index: Some(0),
            shuffle_mode: Some(false),
            context_uuid: Some(uuid::Uuid::new_v4().as_bytes().to_vec()),
            autoplay_reset: Some(true),
            autoplay_loading: Some(false),
        }),
        ..message(msg::CTRL_SRVR_QUEUE_LOAD_TRACKS)
    }
}

/// Append `tracks` at the end of the queue.
pub fn add_tracks(version: Option<QueueVersion>, action: Vec<u8>, tracks: &[u32]) -> QConnectMessage {
    QConnectMessage {
        ctrl_srvr_queue_add_tracks: Some(CtrlSrvrQueueAddTracks {
            queue_version_ref: Some(version.unwrap_or_default()),
            action_uuid: Some(action),
            track_ids: tracks.to_vec(),
            shuffle_seed: None,
            context_uuid: Some(uuid::Uuid::new_v4().as_bytes().to_vec()),
            autoplay_reset: Some(false),
            autoplay_loading: Some(false),
        }),
        ..message(msg::CTRL_SRVR_QUEUE_ADD_TRACKS)
    }
}

pub fn remove_tracks(version: Option<QueueVersion>, action: Vec<u8>, queue_items: &[i32]) -> QConnectMessage {
    QConnectMessage {
        ctrl_srvr_queue_remove_tracks: Some(CtrlSrvrQueueRemoveTracks {
            queue_version_ref: Some(version.unwrap_or_default()),
            action_uuid: Some(action),
            queue_item_ids: queue_items.to_vec(),
            autoplay_reset: Some(false),
            autoplay_loading: Some(false),
        }),
        ..message(msg::CTRL_SRVR_QUEUE_REMOVE_TRACKS)
    }
}

pub fn set_autoplay_mode(version: Option<QueueVersion>, action: Vec<u8>, on: bool) -> QConnectMessage {
    QConnectMessage {
        ctrl_srvr_set_autoplay_mode: Some(CtrlSrvrSetAutoplayMode {
            queue_version_ref: Some(version.unwrap_or_default()),
            action_uuid: Some(action),
            autoplay_mode: Some(on),
            autoplay_reset: Some(false),
            autoplay_loading: Some(false),
        }),
        ..message(msg::CTRL_SRVR_SET_AUTOPLAY_MODE)
    }
}

pub fn set_loop_mode(mode: i32) -> QConnectMessage {
    QConnectMessage {
        ctrl_srvr_set_loop_mode: Some(CtrlSrvrSetLoopMode { loop_mode: Some(mode) }),
        ..message(msg::CTRL_SRVR_SET_LOOP_MODE)
    }
}

/// `SetPlayerState`. A bare `playing_state` is play/pause (some renderers
/// take anything more as a seek and resume); a position with the current
/// item and no playing state is a seek.
pub fn set_player_state(
    playing: Option<i32>,
    position_ms: Option<u64>,
    item: Option<(Option<QueueVersion>, i32)>,
) -> QConnectMessage {
    QConnectMessage {
        ctrl_srvr_set_player_state: Some(CtrlSrvrSetPlayerState {
            playing_state: playing,
            current_position: position_ms.map(|p| p.min(i32::MAX as u64) as i32),
            current_queue_item: item.map(|(queue_version, id)| CtrlSrvrSetPlayerStateQueueItem { queue_version, id: Some(id) }),
        }),
        ..message(msg::CTRL_SRVR_SET_PLAYER_STATE)
    }
}

pub fn set_volume(renderer: i32, percent: u32) -> QConnectMessage {
    QConnectMessage {
        ctrl_srvr_set_volume: Some(CtrlSrvrSetVolume {
            renderer_id: Some(renderer),
            volume: Some(percent.min(100) as i32),
            volume_delta: None,
        }),
        ..message(msg::CTRL_SRVR_SET_VOLUME)
    }
}

pub fn mute_volume(renderer: i32, muted: bool) -> QConnectMessage {
    QConnectMessage {
        ctrl_srvr_mute_volume: Some(CtrlSrvrMuteVolume { renderer_id: Some(renderer), value: Some(muted) }),
        ..message(msg::CTRL_SRVR_MUTE_VOLUME)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{
        DeviceCapabilities, Position, SrvrCtrlActiveRendererChanged, SrvrCtrlAddRenderer, SrvrCtrlQueueErrorMessage,
        SrvrCtrlQueueState, SrvrCtrlQueueTracksAdded, SrvrCtrlQueueTracksLoaded, SrvrCtrlQueueTracksRemoved,
        SrvrCtrlRendererPlayerState, SrvrCtrlRendererStateUpdated, SrvrCtrlSessionState, SrvrCtrlVolumeChanged,
    };
    use prost::Message as _;

    const OWN: &str = "6f1c2b9e-1111-4222-8333-944455556666";
    const AMP: [u8; 16] = [0xab; 16];

    fn track(q: i32, t: u32) -> QueueTrackRef {
        QueueTrackRef { queue_item_id: Some(q), track_id: Some(t), context_uuid: None }
    }

    fn v(major: u64, minor: i32) -> Option<QueueVersion> {
        Some(QueueVersion { major: Some(major), minor: Some(minor) })
    }

    fn add(id: i32, uuid: &[u8], name: &str, volume_control: Option<i32>) -> QConnectMessage {
        QConnectMessage {
            srvr_ctrl_add_renderer: Some(SrvrCtrlAddRenderer {
                renderer_id: Some(id),
                device_info: Some(DeviceInfo {
                    device_uuid: Some(uuid.to_vec()),
                    friendly_name: Some(name.into()),
                    brand: Some("Marantz".into()),
                    model: Some("MODEL 40n".into()),
                    capabilities: Some(DeviceCapabilities { volume_remote_control: volume_control, ..Default::default() }),
                    ..Default::default()
                }),
            }),
            ..Default::default()
        }
    }

    /// Field number `tag` with a length-delimited payload, as it starts the
    /// encoded `QConnectMessage` after `message_type`.
    fn field_tag(tag: i32) -> Vec<u8> {
        let mut v = Vec::new();
        prost::encoding::encode_varint(((tag << 3) | 2) as u64, &mut v);
        v
    }

    #[test]
    fn controller_messages_use_the_official_field_numbers() {
        let cases = [
            (set_player_state(Some(2), None, None), 62),
            (set_active_renderer(3), 63),
            (set_volume(3, 40), 64),
            (load_tracks(None, vec![0; 16], &[1]), 66),
            (add_tracks(None, vec![0; 16], &[1]), 68),
            (remove_tracks(None, vec![0; 16], &[1]), 69),
            (set_loop_mode(1), 72),
            (mute_volume(3, true), 73),
            (ask_for_queue_state(None, vec![0; 16]), 76),
            (ask_for_renderer_state(3), 77),
            (set_autoplay_mode(None, vec![0; 16], false), 78),
        ];
        for (m, tag) in cases {
            assert_eq!(m.message_type, Some(tag));
            let bytes = m.encode_to_vec();
            // message_type (field 1, varint), then the payload's field.
            assert_eq!(bytes[0], 0x08);
            assert_eq!(&bytes[2..2 + field_tag(tag).len()], field_tag(tag).as_slice(), "message {tag}");
        }
    }

    #[test]
    fn queue_load_layout() {
        let m = load_tracks(v(4, 1), vec![7; 16], &[287_336_643, 5]);
        let body = m.ctrl_srvr_queue_load_tracks.unwrap().encode_to_vec();
        // queueVersionRef {major 4, minor 1}, actionUuid (16 bytes).
        assert_eq!(&body[..6], &[0x0A, 0x04, 0x08, 0x04, 0x10, 0x01]);
        assert_eq!(&body[6..8], &[0x12, 16]);
        // trackIds: packed fixed32 (field 3, length 8), little endian.
        assert_eq!(&body[24..26], &[0x1A, 8]);
        assert_eq!(&body[26..30], &287_336_643u32.to_le_bytes());
        // queuePosition 0 is sent: the server reads proto3 defaults anyway.
        assert_eq!(&body[34..36], &[0x20, 0x00]);
    }

    #[test]
    fn player_state_messages() {
        // Pause: playing state only.
        let pause = set_player_state(Some(playing_state::PAUSED), None, None).ctrl_srvr_set_player_state.unwrap();
        assert_eq!(pause.encode_to_vec(), vec![0x08, 0x03]);
        // Seek: position and item, no playing state; item 0 is sent.
        let seek = set_player_state(None, Some(61_000), Some((v(2, 0), 0))).ctrl_srvr_set_player_state.unwrap();
        assert_eq!(seek.playing_state, None);
        assert_eq!(seek.current_position, Some(61_000));
        let item = seek.current_queue_item.unwrap();
        assert_eq!((item.id, item.queue_version), (Some(0), v(2, 0)));
        assert!(item.encode_to_vec().ends_with(&[0x10, 0x00]));
        let remove = remove_tracks(None, vec![0; 16], &[3, 4]).ctrl_srvr_queue_remove_tracks.unwrap();
        // Unpacked int32 queue item ids.
        assert!(remove.encode_to_vec().windows(4).any(|w| w == [0x18, 3, 0x18, 4]));
    }

    #[test]
    fn renderer_table_skips_ourselves_and_dedups_by_uuid() {
        let own = uuid::Uuid::parse_str(OWN).unwrap();
        let mut st = CtrlState::new(OWN);
        st.on_connected();
        st.apply(&add(1, own.as_bytes(), "ricercar", Some(2)), 0);
        st.apply(&add(2, &AMP, "Salon", Some(1)), 0);
        assert_eq!(st.own_renderer, Some(1));
        let others: Vec<&str> = st.others().map(|r| r.name.as_str()).collect();
        assert_eq!(others, ["Salon"]);
        let amp_uuid = uuid::Uuid::from_bytes(AMP).to_string();
        let amp = st.renderer_by_uuid(&amp_uuid).unwrap();
        assert!(amp.fixed_volume);
        assert_eq!(Renderer::brand_model(&amp.brand, &amp.model), "Marantz MODEL 40n");
        assert_eq!(Renderer::brand_model("Marantz", "Marantz MODEL 40n"), "Marantz MODEL 40n");
        // Rejoined under a new id: a single entry.
        st.apply(&add(5, &AMP, "Salon", Some(2)), 0);
        assert_eq!(st.others().map(|r| r.id).collect::<Vec<_>>(), [5]);
        // A reconnection keeps the list until the renderers are announced again.
        st.on_connected();
        assert_eq!(st.others().count(), 1);
        st.forget_unseen();
        assert_eq!(st.others().count(), 0);
    }

    #[test]
    fn active_renderer_volume_and_player_state() {
        let mut st = CtrlState::new(OWN);
        st.apply(&add(2, &AMP, "Salon", None), 0);
        st.apply(
            &QConnectMessage {
                srvr_ctrl_session_state: Some(SrvrCtrlSessionState {
                    session_uuid: Some(vec![1; 16]),
                    active_renderer_id: Some(-1),
                    ..Default::default()
                }),
                ..Default::default()
            },
            0,
        );
        assert_eq!(st.session_uuid.as_deref(), Some("01010101-0101-0101-0101-010101010101"));
        assert_eq!(st.active, None);
        st.apply(
            &QConnectMessage {
                srvr_ctrl_active_renderer_changed: Some(SrvrCtrlActiveRendererChanged { active_renderer_id: Some(2) }),
                srvr_ctrl_volume_changed: Some(SrvrCtrlVolumeChanged { renderer_id: Some(2), volume: Some(35) }),
                ..Default::default()
            },
            0,
        );
        assert_eq!(st.active, Some(2));
        let state = |ps: SrvrCtrlRendererPlayerState| QConnectMessage {
            srvr_ctrl_renderer_state_updated: Some(SrvrCtrlRendererStateUpdated {
                renderer_id: Some(2),
                status: Some(1),
                player_state: Some(ps),
            }),
            ..Default::default()
        };
        // Item 0 is omitted on the wire.
        st.apply(
            &state(SrvrCtrlRendererPlayerState {
                playing_state: Some(2),
                buffer_state: Some(2),
                current_position: Some(Position { timestamp: Some(1), value: Some(10_000) }),
                duration: Some(200_000),
                current_queue_item_id: None,
            }),
            1_000,
        );
        let r = &st.renderers[&2];
        assert_eq!(r.volume, Some(35));
        let p = r.player.unwrap();
        assert_eq!(p.queue_item, Some(0));
        assert_eq!(p.position_at(3_500), 12_500);
        assert_eq!(p.position_at(1_000_000), 200_000, "never past the end");
        // A sparse update keeps what it does not repeat.
        st.apply(&state(SrvrCtrlRendererPlayerState { current_queue_item_id: Some(0), ..Default::default() }), 2_000);
        let p = st.renderers[&2].player.unwrap();
        assert_eq!((p.playing_state, p.position_ms, p.duration_ms), (2, 11_000, Some(200_000)));
        // -1: no item.
        st.apply(&state(SrvrCtrlRendererPlayerState { playing_state: Some(1), current_queue_item_id: Some(-1), ..Default::default() }), 3_000);
        let p = st.renderers[&2].player.unwrap();
        assert_eq!((p.playing_state, p.queue_item, p.duration_ms), (1, None, None));
    }

    #[test]
    fn queue_copy_and_action_acks() {
        let mut st = CtrlState::new(OWN);
        st.apply(
            &QConnectMessage {
                srvr_ctrl_queue_state: Some(SrvrCtrlQueueState {
                    queue_version: v(1, 0),
                    tracks: vec![track(0, 10), track(1, 11)],
                    ..Default::default()
                }),
                ..Default::default()
            },
            0,
        );
        assert!(st.queue.known);
        assert_eq!(st.queue.autoplay_mode, Some(false));
        assert_eq!(st.queue.track_of(1), Some(11));

        let load = st.begin_action();
        assert_eq!(st.take_ack(&load), None, "nothing yet");
        st.apply(
            &QConnectMessage {
                srvr_ctrl_queue_tracks_loaded: Some(SrvrCtrlQueueTracksLoaded {
                    queue_version: v(2, 0),
                    action_uuid: Some(load.clone()),
                    tracks: vec![QueueTrackRef { queue_item_id: None, track_id: Some(77), context_uuid: None }],
                    ..Default::default()
                }),
                ..Default::default()
            },
            0,
        );
        assert_eq!(st.take_ack(&load), Some(Ack::Done(vec![(0, 77)])));
        assert_eq!(st.take_ack(&load), None, "taken once");
        assert_eq!(st.queue.version, v(2, 0));

        let add_action = st.begin_action();
        st.apply(
            &QConnectMessage {
                srvr_ctrl_queue_tracks_added: Some(SrvrCtrlQueueTracksAdded {
                    queue_version: v(2, 1),
                    action_uuid: Some(add_action.clone()),
                    tracks: vec![track(1, 78)],
                    ..Default::default()
                }),
                ..Default::default()
            },
            0,
        );
        assert_eq!(st.take_ack(&add_action), Some(Ack::Done(vec![(1, 78)])));
        assert_eq!(st.queue.after(0), Some((1, 78)));
        assert_eq!(st.queue.track_of(1), Some(78));

        st.apply(
            &QConnectMessage {
                srvr_ctrl_queue_tracks_removed: Some(SrvrCtrlQueueTracksRemoved {
                    queue_version: v(2, 2),
                    queue_item_ids: vec![0],
                    ..Default::default()
                }),
                ..Default::default()
            },
            0,
        );
        assert_eq!(st.queue.items, vec![(1, 78)]);

        // A refusal without action uuid fails every action in flight.
        let stale = st.begin_action();
        st.apply(
            &QConnectMessage {
                srvr_ctrl_queue_error_message: Some(SrvrCtrlQueueErrorMessage::default()),
                ..Default::default()
            },
            0,
        );
        assert!(matches!(st.take_ack(&stale), Some(Ack::Refused(_))));
        assert!(st.resync);
    }
}
