//! Qobuz Connect devices as host outputs (capability `outputs`).
//!
//! The plugin drives them as a Qobuz Connect controller, over the session's
//! WebSocket (`session::CtrlLink`); the device fetches and decodes the audio
//! itself. Devices on the LAN that have not joined the account's session yet
//! are found with mDNS and paired when selected.
//!
//! The remote queue is kept as `[current, next]`: `output.play` replaces it
//! with the one track, `output.next` appends the next track and removes every
//! other item, so the device moves on by itself at the end (gapless) and
//! stops after the last track ricercar gave it.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::watch;
use tokio::task::AbortHandle;

use super::rpc::{self, Out, RpcError, RpcResult};
use crate::api::{now_ms, ApiClient};
use crate::controller::{self as ctrl, Ack, CtrlState, ItemError, Queue, Renderer};
use crate::lan::{self, LanEvent, LanToken, Probed};
use crate::msgtype::{buffer_state, loop_mode, playback_error, playing_state};
use crate::proto::{QConnectMessage, QueueVersion};
use crate::session::CtrlLink;
use crate::ws::WsEvent;

/// For the session to be joined after connecting.
const JOIN_WAIT: Duration = Duration::from_secs(15);
/// For the device to show up in the session or on the LAN.
const FIND_WAIT: Duration = Duration::from_secs(10);
/// For a paired device to join the session.
const PAIR_WAIT: Duration = Duration::from_secs(20);
/// For the server to confirm the active renderer.
const ACTIVATE_WAIT: Duration = Duration::from_secs(10);
/// For the server to answer a queue action.
const ACK_WAIT: Duration = Duration::from_secs(5);
/// For the device to stop before it is released.
const STOP_WAIT: Duration = Duration::from_secs(2);
/// A play order shows as `loading` until the device reports the track, at
/// most this long.
const LOADING_MAX_MS: u64 = 30_000;
/// After a reconnection, renderers not announced again by then are gone.
const REJOIN_SETTLE_MS: u64 = 5_000;
const TICK: Duration = Duration::from_secs(1);
/// `output.state` about every second while playing.
const STATE_PERIOD_MS: u64 = 900;
/// A position further than this from the expected one is a jump (seek).
const POSITION_JUMP_MS: u64 = 1_500;

// ---------- outputs.changed ----------

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OutputInfo {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    pub badge: &'static str,
}

/// How the host labels every device of ours (a brand name: not translated).
const BADGE: &str = "Qobuz Connect";

/// `outputs.changed`: only once the `initialize` response is out, and only
/// when the list changes.
pub struct Publisher {
    out: Out,
    inner: Mutex<PubState>,
}

#[derive(Default)]
struct PubState {
    open: bool,
    sent: Option<Vec<OutputInfo>>,
    current: Vec<OutputInfo>,
}

impl Publisher {
    pub fn new(out: Out) -> Self {
        Self { out, inner: Mutex::default() }
    }

    /// The `initialize` response is out: send the list (even empty).
    pub fn open(&self) {
        let mut p = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        p.open = true;
        self.flush(&mut p);
    }

    pub fn set(&self, list: Vec<OutputInfo>) {
        let mut p = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        p.current = list;
        self.flush(&mut p);
    }

    fn flush(&self, p: &mut PubState) {
        if p.open && p.sent.as_ref() != Some(&p.current) {
            self.out.notify("outputs.changed", json!({"outputs": p.current}));
            p.sent = Some(p.current.clone());
        }
    }
}

/// Usable devices: the session's renderers but ours, then LAN devices not in
/// the session, one entry per device uuid.
fn output_list(ctrl: &CtrlState, lan: &HashMap<String, Probed>) -> Vec<OutputInfo> {
    let mut list: Vec<OutputInfo> = ctrl
        .others()
        .filter_map(|r| {
            let id = r.uuid.clone()?;
            let name = [r.name.trim().to_string(), Renderer::brand_model(&r.brand, &r.model)]
                .into_iter()
                .find(|n| !n.is_empty())
                .unwrap_or_else(|| "Qobuz Connect".into());
            Some(OutputInfo { id, name, detail: detail(&r.brand, &r.model), badge: BADGE })
        })
        .collect();
    for (uuid, p) in lan {
        if list.iter().any(|o| &o.id == uuid) {
            continue;
        }
        let d = &p.display;
        let instance = p.candidate.fullname.split("._qobuz-connect").next().unwrap_or_default();
        let name = [d.friendly_name.trim(), instance].into_iter().find(|n| !n.is_empty()).unwrap_or("Qobuz Connect");
        list.push(OutputInfo {
            id: uuid.clone(),
            name: name.to_string(),
            detail: detail(&d.brand_display_name, &d.model_display_name),
            badge: BADGE,
        });
    }
    list.sort_by(|a, b| (&a.name, &a.id).cmp(&(&b.name, &b.id)));
    list
}

fn detail(brand: &str, model: &str) -> Option<String> {
    Some(Renderer::brand_model(brand, model)).filter(|bm| !bm.is_empty())
}

// ---------- output.state ----------

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OutputState {
    pub status: &'static str,
    #[serde(rename = "ref", skip_serializing_if = "Option::is_none")]
    pub reference: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pos_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dur_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub muted: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<StateError>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StateError {
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<i64>,
}

/// A play order the device has not reported yet.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Loading {
    track: u32,
    /// Once the server queued it.
    queue_item: Option<i32>,
    start_ms: u64,
    since_ms: u64,
}

fn track_ref(track: u32) -> String {
    format!("track/{track}")
}

fn item_error(name: &str, kind: i32) -> StateError {
    let (what, code) = match kind {
        playback_error::TRACK_NOT_FOUND => ("track not found", Some(rpc::NOT_FOUND)),
        playback_error::TRACK_NOT_STREAMABLE => ("track not available for streaming", Some(rpc::UNAVAILABLE)),
        playback_error::NETWORK_ERROR => ("network error", Some(rpc::NETWORK)),
        _ => ("cannot play this track", None),
    };
    StateError { message: format!("{name}: {what}"), code }
}

/// `output.state` of the selected device. `r`: its session entry; `active`:
/// it is the session's active renderer; `last`: the state sent before.
fn output_state(
    r: Option<&Renderer>,
    active: bool,
    queue: &Queue,
    loading: Option<&Loading>,
    error: Option<ItemError>,
    last: Option<&OutputState>,
    now: u64,
) -> OutputState {
    let mut s = OutputState {
        status: "stopped",
        reference: None,
        pos_ms: None,
        dur_ms: None,
        volume: r.and_then(|r| r.volume),
        muted: r.and_then(|r| r.muted),
        error: None,
    };
    let keep_last = |s: &mut OutputState| {
        if let Some(last) = last {
            s.reference = last.reference.clone();
            s.pos_ms = last.pos_ms;
            s.dur_ms = last.dur_ms;
        }
    };
    let Some(r) = r.filter(|r| active && !r.disconnected) else {
        // Gone, or another device took over: stopped where it was.
        keep_last(&mut s);
        return s;
    };
    let p = r.player.unwrap_or_default();
    let failure = |qid: Option<i32>| -> Option<StateError> {
        let qid = qid?;
        match error {
            Some(e) if e.queue_item == qid => Some(item_error(&r.name, e.kind)),
            _ if p.queue_item == Some(qid) && p.buffer_state == buffer_state::ERROR => {
                Some(item_error(&r.name, playback_error::OTHER))
            }
            _ => None,
        }
    };
    if let Some(l) = loading {
        s.reference = Some(track_ref(l.track));
        s.pos_ms = Some(l.start_ms);
        match failure(l.queue_item) {
            Some(e) => s.error = Some(e),
            None => s.status = "loading",
        }
        return s;
    }
    s.reference = p.queue_item.and_then(|q| queue.track_of(q)).map(track_ref);
    s.pos_ms = Some(p.position_at(now));
    s.dur_ms = p.duration_ms;
    s.status = match p.playing_state {
        playing_state::PLAYING => "playing",
        playing_state::PAUSED => "paused",
        _ => "stopped",
    };
    if let Some(e) = failure(p.queue_item) {
        s.status = "stopped";
        s.error = Some(e);
    } else if s.status == "stopped" {
        // Some devices drop the item or rewind when they reach the end:
        // stopped on the track that was playing, where it was.
        if let Some(last) = last.filter(|l| l.reference.is_some() && (s.reference.is_none() || s.reference == l.reference)) {
            s.reference = last.reference.clone();
            s.pos_ms = s.pos_ms.max(last.pos_ms);
            s.dur_ms = s.dur_ms.or(last.dur_ms);
        }
    }
    s
}

/// Whether `next` must go out: any change but the position, a position
/// jump, or periodically (`tick`) while playing or loading.
fn worth_sending(last: Option<(&OutputState, u64)>, next: &OutputState, now: u64, tick: bool) -> bool {
    let Some((last, at)) = last else { return true };
    let same = OutputState { pos_ms: next.pos_ms, ..last.clone() };
    if same != *next {
        return true;
    }
    let elapsed = now.saturating_sub(at);
    if tick && matches!(next.status, "playing" | "loading") && elapsed >= STATE_PERIOD_MS {
        return true;
    }
    let expected = last.pos_ms.map(|p| if last.status == "playing" { p + elapsed } else { p });
    match (expected, next.pos_ms) {
        (Some(e), Some(p)) => e.abs_diff(p) > POSITION_JUMP_MS,
        (e, p) => e.is_some() != p.is_some(),
    }
}

// ---------- requests ----------

#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    Select,
    Release,
    Play { track: u32, start_ms: u64 },
    Next(Option<u32>),
    Pause,
    Resume,
    Stop,
    Seek(u64),
    Volume { percent: Option<u32>, muted: Option<bool> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct OutputRequest {
    pub output: String,
    pub op: Op,
}

const METHODS: [&str; 9] = [
    "output.select",
    "output.release",
    "output.play",
    "output.next",
    "output.pause",
    "output.resume",
    "output.stop",
    "output.seek",
    "output.volume",
];

/// Check an `output.*` request before anything is sent to a device.
pub fn parse(method: &str, params: Value) -> Result<OutputRequest, RpcError> {
    #[derive(Deserialize)]
    struct P {
        output: String,
        #[serde(rename = "ref", default)]
        reference: Option<String>,
        #[serde(default)]
        start_ms: Option<u64>,
        #[serde(default)]
        ms: Option<u64>,
        #[serde(default)]
        percent: Option<f64>,
        #[serde(default)]
        muted: Option<bool>,
    }
    if !METHODS.contains(&method) {
        return Err(RpcError::new(rpc::METHOD_NOT_FOUND, format!("unknown method {method}")));
    }
    let p: P = serde_json::from_value(params).map_err(|e| RpcError::invalid_params(e.to_string()))?;
    let track = |r: &str| {
        r.strip_prefix("track/")
            .and_then(|id| id.parse::<u32>().ok())
            .ok_or_else(|| RpcError::invalid_params(format!("Qobuz Connect devices play Qobuz tracks only, not {r}")))
    };
    let op = match method {
        "output.select" => Op::Select,
        "output.release" => Op::Release,
        "output.play" => {
            let r = p.reference.as_deref().ok_or_else(|| RpcError::invalid_params("ref is required"))?;
            Op::Play { track: track(r)?, start_ms: p.start_ms.unwrap_or(0) }
        }
        "output.next" => Op::Next(p.reference.as_deref().map(track).transpose()?),
        "output.pause" => Op::Pause,
        "output.resume" => Op::Resume,
        "output.stop" => Op::Stop,
        "output.seek" => Op::Seek(p.ms.ok_or_else(|| RpcError::invalid_params("ms is required"))?),
        _ => Op::Volume { percent: p.percent.map(|v| v.round().clamp(0.0, 100.0) as u32), muted: p.muted },
    };
    Ok(OutputRequest { output: p.output, op })
}

/// A request's answer; answers an error if dropped unanswered (Qobuz Connect
/// stopped meanwhile).
struct Reply {
    out: Out,
    id: Option<Value>,
}

impl Reply {
    fn send(mut self, result: RpcResult) {
        if let Some(id) = self.id.take() {
            self.out.respond(id, result);
        }
    }
}

impl Drop for Reply {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            self.out.respond(id, Err(unavailable("Qobuz Connect stopped")));
        }
    }
}

fn unavailable(message: impl Into<String>) -> RpcError {
    RpcError::new(rpc::UNAVAILABLE, message)
}

// ---------- running controller ----------

/// The controller side of a running Qobuz Connect session. Stops on drop.
pub struct Outputs {
    shared: Arc<Shared>,
    requests: UnboundedSender<(OutputRequest, Reply)>,
    tasks: Vec<AbortHandle>,
    _browser: Option<lan::Browser>,
}

impl Drop for Outputs {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

struct Shared {
    st: Mutex<St>,
    /// Bumped after every change of `st`, for the requests waiting on it.
    changed: watch::Sender<u64>,
    wire: UnboundedSender<Vec<QConnectMessage>>,
    out: Out,
    publisher: Arc<Publisher>,
    api: ApiClient,
    http: Option<reqwest::Client>,
}

struct St {
    ctrl: CtrlState,
    /// Probed LAN devices, by device uuid.
    lan: HashMap<String, Probed>,
    rejoined_at: Option<u64>,
    sel: Option<Selected>,
}

/// The output the host selected.
struct Selected {
    uuid: String,
    loading: Option<Loading>,
    sent: Option<(OutputState, u64)>,
}

impl Outputs {
    /// Start the controller side. `own_uuid`: our renderer's device uuid,
    /// never listed. Returns the link the session needs.
    pub fn start(own_uuid: &str, api: ApiClient, out: Out, publisher: Arc<Publisher>) -> (Self, CtrlLink) {
        let (events_tx, events_rx) = unbounded_channel();
        let (wire_tx, wire_rx) = unbounded_channel();
        let http = lan::http_client().inspect_err(|e| tracing::warn!("no LAN HTTP client ({e:#})")).ok();
        let shared = Arc::new(Shared {
            st: Mutex::new(St { ctrl: CtrlState::new(own_uuid), lan: HashMap::new(), rejoined_at: None, sel: None }),
            changed: watch::channel(0).0,
            wire: wire_tx,
            out,
            publisher,
            api,
            http,
        });
        let (lan_tx, lan_rx) = unbounded_channel();
        let browser = match lan::browse(lan_tx) {
            Ok(b) => Some(b),
            Err(e) => {
                tracing::warn!("mDNS discovery unavailable ({e:#}): only devices already in Qobuz Connect are listed");
                None
            }
        };
        let (requests, requests_rx) = unbounded_channel();
        let tasks = vec![
            tokio::spawn(run_events(shared.clone(), events_rx, lan_rx, lan::normalize_uuid(own_uuid))).abort_handle(),
            tokio::spawn(run_requests(shared.clone(), requests_rx)).abort_handle(),
        ];
        let outputs = Self { shared, requests, tasks, _browser: browser };
        (outputs, CtrlLink { events: events_tx, outbound: wire_rx })
    }

    /// Queue a checked request; requests run one at a time, in order.
    pub fn submit(&self, id: Value, request: OutputRequest) {
        let reply = Reply { out: self.shared.out.clone(), id: Some(id) };
        // On failure the reply answers on drop.
        let _ = self.requests.send((request, reply));
    }

    /// An output is selected: the host plays on a device.
    pub fn in_use(&self) -> bool {
        self.shared.lock().sel.is_some()
    }
}

async fn run_requests(shared: Arc<Shared>, mut rx: UnboundedReceiver<(OutputRequest, Reply)>) {
    while let Some((request, reply)) = rx.recv().await {
        let result = shared.handle(&request.output, request.op).await;
        if let Err(e) = &result {
            tracing::info!("output request failed: {}", e.message);
        }
        reply.send(result);
    }
}

async fn run_events(
    shared: Arc<Shared>,
    mut ws: UnboundedReceiver<WsEvent>,
    mut lan_rx: UnboundedReceiver<LanEvent>,
    own_uuid: Option<String>,
) {
    let (probed_tx, mut probed_rx) = unbounded_channel::<(String, anyhow::Result<Probed>)>();
    let mut probing: HashSet<String> = HashSet::new();
    let mut by_fullname: HashMap<String, String> = HashMap::new();
    let mut tick = tokio::time::interval(TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            ev = ws.recv() => match ev {
                Some(ev) => shared.on_ws(ev),
                None => break,
            },
            Some(ev) = lan_rx.recv() => match ev {
                LanEvent::Found(c) => {
                    let known = shared.lock().lan.get(&c.device_uuid).is_some_and(|p| p.candidate == c);
                    if known || own_uuid.as_ref() == Some(&c.device_uuid) || !probing.insert(c.device_uuid.clone()) {
                        continue;
                    }
                    let Some(http) = shared.http.clone() else { continue };
                    let tx = probed_tx.clone();
                    tokio::spawn(async move {
                        let uuid = c.device_uuid.clone();
                        let _ = tx.send((uuid, lan::probe(&http, c).await));
                    });
                }
                LanEvent::Lost(fullname) => {
                    if let Some(uuid) = by_fullname.remove(&fullname) {
                        tracing::info!("Qobuz Connect device {fullname} left the network");
                        shared.update(|st| {
                            st.lan.remove(&uuid);
                        });
                    }
                }
            },
            Some((uuid, probed)) = probed_rx.recv() => {
                probing.remove(&uuid);
                match probed {
                    Ok(p) => {
                        tracing::info!("Qobuz Connect device on the network: {:?} ({uuid})", p.display.friendly_name);
                        by_fullname.insert(p.candidate.fullname.clone(), uuid.clone());
                        shared.update(|st| {
                            st.lan.insert(uuid, p);
                        });
                    }
                    Err(e) => tracing::info!("Qobuz Connect device {uuid} did not answer: {e:#}"),
                }
            }
            _ = tick.tick() => shared.on_tick(),
        }
    }
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, St> {
        self.st.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Change the state, then publish what follows from it.
    fn update(&self, f: impl FnOnce(&mut St)) {
        let mut st = self.lock();
        f(&mut st);
        self.after_change(&mut st, false);
        drop(st);
        self.changed.send_modify(|v| *v = v.wrapping_add(1));
    }

    fn on_ws(&self, ev: WsEvent) {
        let now = now_ms();
        self.update(|st| {
            match ev {
                WsEvent::Connected => {
                    st.ctrl.on_connected();
                    st.rejoined_at = Some(now);
                    st.ctrl.resync = true;
                }
                WsEvent::Disconnected => st.ctrl.on_disconnected(),
                WsEvent::Batch(batch) => {
                    for m in &batch.messages {
                        st.ctrl.apply(m, now);
                    }
                }
            }
            if st.ctrl.resync && st.ctrl.connected {
                st.ctrl.resync = false;
                let m = queue_request(st);
                let _ = self.wire.send(vec![m]);
            }
        });
    }

    fn on_tick(&self) {
        let now = now_ms();
        let mut st = self.lock();
        if st.rejoined_at.is_some_and(|t| now.saturating_sub(t) >= REJOIN_SETTLE_MS) {
            st.rejoined_at = None;
            st.ctrl.forget_unseen();
        }
        self.after_change(&mut st, true);
    }

    /// Publish the output list and the selected output's state.
    fn after_change(&self, st: &mut St, tick: bool) {
        self.publisher.set(output_list(&st.ctrl, &st.lan));
        let now = now_ms();
        let St { ctrl, sel, .. } = st;
        let Some(sel) = sel else { return };
        let r = ctrl.renderer_by_uuid(&sel.uuid);
        let active = r.is_some_and(|r| ctrl.active == Some(r.id));
        if let (Some(l), Some(r)) = (sel.loading, r) {
            let p = r.player.unwrap_or_default();
            let started = l.queue_item.is_some()
                && p.queue_item == l.queue_item
                && matches!(p.playing_state, playing_state::PLAYING | playing_state::PAUSED)
                && p.buffer_state != buffer_state::BUFFERING;
            if started || now.saturating_sub(l.since_ms) > LOADING_MAX_MS {
                sel.loading = None;
            }
        }
        let last = sel.sent.as_ref().map(|(s, at)| (s, *at));
        let state = output_state(r, active, &ctrl.queue, sel.loading.as_ref(), ctrl.item_error, last.map(|l| l.0), now);
        if state.error.is_some() {
            sel.loading = None;
        }
        if worth_sending(last, &state, now, tick) {
            let mut params = serde_json::to_value(&state).unwrap_or_default();
            params["output"] = json!(sel.uuid);
            self.out.notify("output.state", params);
            sel.sent = Some((state, now));
        }
    }

    /// Wait until `f` gives a value, at most `timeout`.
    async fn wait<T>(&self, timeout: Duration, mut f: impl FnMut(&mut St) -> Option<T>) -> Option<T> {
        let mut rx = self.changed.subscribe();
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let found = f(&mut self.lock());
            if found.is_some() {
                return found;
            }
            match tokio::time::timeout_at(deadline, rx.changed()).await {
                Ok(Ok(())) => {}
                _ => return None,
            }
        }
    }

    fn send(&self, messages: Vec<QConnectMessage>) -> Result<(), RpcError> {
        if !self.lock().ctrl.connected {
            return Err(unavailable("Qobuz Connect is not connected"));
        }
        self.wire.send(messages).map_err(|_| unavailable("Qobuz Connect stopped"))
    }

    fn name_of(&self, uuid: &str) -> String {
        let st = self.lock();
        let name = match st.ctrl.renderer_by_uuid(uuid) {
            Some(r) => r.name.clone(),
            None => st.lan.get(uuid).map(|p| p.display.friendly_name.clone()).unwrap_or_default(),
        };
        if name.trim().is_empty() { "The Qobuz Connect device".into() } else { name }
    }

    async fn handle(&self, uuid: &str, op: Op) -> RpcResult {
        match op {
            Op::Select => self.select(uuid).await.map(|_| Value::Null),
            Op::Release => self.release(uuid).await,
            Op::Play { track, start_ms } => self.play(uuid, track, start_ms).await,
            Op::Next(track) => self.next(uuid, track).await,
            Op::Pause | Op::Resume => {
                self.active(uuid)?;
                // Bare: anything more makes some devices seek and resume.
                let state = if op == Op::Pause { playing_state::PAUSED } else { playing_state::PLAYING };
                self.send(vec![ctrl::set_player_state(Some(state), None, None)])?;
                Ok(Value::Null)
            }
            Op::Stop => {
                self.active(uuid)?;
                let (item, position) = self.current_item(uuid);
                self.send(vec![ctrl::set_player_state(Some(playing_state::STOPPED), position, item)])?;
                self.update(|st| {
                    if let Some(sel) = &mut st.sel {
                        sel.loading = None;
                    }
                });
                Ok(Value::Null)
            }
            Op::Seek(ms) => {
                self.active(uuid)?;
                let (item, _) = self.current_item(uuid);
                let item = item.ok_or_else(|| unavailable("nothing is playing"))?;
                self.send(vec![ctrl::set_player_state(None, Some(ms), Some(item))])?;
                Ok(Value::Null)
            }
            Op::Volume { percent, muted } => {
                let rid = self.active(uuid)?;
                let fixed = self.lock().ctrl.renderers.get(&rid).is_some_and(|r| r.fixed_volume);
                if fixed {
                    tracing::info!("{} does not accept volume changes from controllers", self.name_of(uuid));
                    return Ok(Value::Null);
                }
                let mut messages = Vec::new();
                if let Some(p) = percent {
                    messages.push(ctrl::set_volume(rid, p));
                }
                if let Some(m) = muted {
                    messages.push(ctrl::mute_volume(rid, m));
                }
                if !messages.is_empty() {
                    self.send(messages)?;
                }
                Ok(Value::Null)
            }
        }
    }

    /// The selected device's renderer id, if it is still the active one.
    fn active(&self, uuid: &str) -> Result<i32, RpcError> {
        let st = self.lock();
        if st.sel.as_ref().map(|s| s.uuid.as_str()) != Some(uuid) {
            return Err(unavailable("this output is not selected"));
        }
        let r = st.ctrl.renderer_by_uuid(uuid).ok_or_else(|| unavailable("the device left Qobuz Connect"))?;
        if st.ctrl.active != Some(r.id) {
            return Err(unavailable(format!("{} is no longer the active Qobuz Connect device", r.name)));
        }
        Ok(r.id)
    }

    /// (queue version, item) the device is on (or is loading), and its position.
    #[allow(clippy::type_complexity)]
    fn current_item(&self, uuid: &str) -> (Option<(Option<QueueVersion>, i32)>, Option<u64>) {
        let st = self.lock();
        let player = st.ctrl.renderer_by_uuid(uuid).and_then(|r| r.player);
        let loading = st.sel.as_ref().and_then(|s| s.loading);
        let qid = loading.and_then(|l| l.queue_item).or(player.and_then(|p| p.queue_item));
        let position = match loading {
            Some(l) => Some(l.start_ms),
            None => player.map(|p| p.position_at(now_ms())),
        };
        (qid.map(|q| (st.ctrl.queue.version, q)), position)
    }

    /// Make the device the session's active renderer, pairing it first when
    /// it is only on the LAN. Returns its renderer id.
    async fn select(&self, uuid: &str) -> Result<i32, RpcError> {
        self.wait(JOIN_WAIT, |st| st.ctrl.session_uuid.as_ref().map(|_| ()))
            .await
            .ok_or_else(|| unavailable("Qobuz Connect is not connected"))?;
        enum Found {
            Session,
            Lan(Box<Probed>),
        }
        let found = self
            .wait(FIND_WAIT, |st| match (st.ctrl.renderer_by_uuid(uuid), st.lan.get(uuid)) {
                // Left the cloud but still on the LAN: pair it again.
                (Some(r), Some(p)) if r.disconnected => Some(Found::Lan(Box::new(p.clone()))),
                (Some(_), _) => Some(Found::Session),
                (None, Some(p)) => Some(Found::Lan(Box::new(p.clone()))),
                (None, None) => None,
            })
            .await
            .ok_or_else(|| RpcError::not_found("this Qobuz Connect device is neither in Qobuz Connect nor on the network"))?;
        if let Found::Lan(device) = found {
            let name = self.name_of(uuid);
            tracing::info!("pairing {name} with the account's Qobuz Connect session");
            self.pair(&device, &name).await?;
            self.wait(PAIR_WAIT, |st| st.ctrl.renderer_by_uuid(uuid).filter(|r| !r.disconnected).map(|_| ()))
                .await
                .ok_or_else(|| unavailable(format!("{name} did not join Qobuz Connect")))?;
        }
        let (rid, active) = {
            let st = self.lock();
            let r = st.ctrl.renderer_by_uuid(uuid).ok_or_else(|| unavailable("the device left Qobuz Connect"))?;
            (r.id, st.ctrl.active == Some(r.id))
        };
        let rid = if active {
            rid
        } else {
            self.send(vec![ctrl::set_active_renderer(rid)])?;
            self.wait(ACTIVATE_WAIT, |st| {
                let r = st.ctrl.renderer_by_uuid(uuid)?;
                (st.ctrl.active == Some(r.id)).then_some(r.id)
            })
            .await
            .ok_or_else(|| unavailable(format!("{} did not become the active Qobuz Connect device", self.name_of(uuid))))?
        };
        self.send(vec![ctrl::ask_for_renderer_state(rid)])?;
        self.update(|st| {
            if st.sel.as_ref().map(|s| s.uuid.as_str()) != Some(uuid) {
                tracing::info!("playing on Qobuz Connect renderer {rid}");
                st.sel = Some(Selected { uuid: uuid.to_string(), loading: None, sent: None });
            }
        });
        Ok(rid)
    }

    /// Hand a LAN device the session and delegated tokens; it then joins the
    /// session by itself.
    async fn pair(&self, device: &Probed, name: &str) -> Result<(), RpcError> {
        let http = self.http.as_ref().ok_or_else(|| unavailable("no HTTP client for the local network"))?;
        let session_id = self.lock().ctrl.session_uuid.clone().ok_or_else(|| unavailable("Qobuz Connect is not connected"))?;
        let (api, qws) = self.api.delegate_qconnect_auth(&device.connect.app_id).await?;
        let qws_endpoint = match qws.endpoint {
            Some(e) => e,
            None => self.api.get_ws_token(false).await?.map(|t| t.endpoint).ok_or_else(|| unavailable("no Qobuz Connect endpoint"))?,
        };
        let body = lan::HandoffBody {
            session_id,
            jwt_api: LanToken { endpoint: api.endpoint.unwrap_or_else(|| self.api.base().to_string()), exp: api.exp, jwt: api.jwt },
            jwt_qconnect: LanToken { endpoint: qws_endpoint, exp: qws.exp, jwt: qws.jwt },
            become_active: true,
        };
        lan::hand_off(http, device, &body).await.map_err(|e| unavailable(format!("{name} refused the pairing: {e:#}")))
    }

    /// Stop the device and give the session back to our own renderer.
    async fn release(&self, uuid: &str) -> RpcResult {
        let target = {
            let mut st = self.lock();
            if st.sel.as_ref().map(|s| s.uuid.as_str()) != Some(uuid) {
                return Ok(Value::Null);
            }
            st.sel = None;
            st.ctrl.renderer_by_uuid(uuid).filter(|r| st.ctrl.active == Some(r.id)).map(|r| {
                let busy = r.player.is_some_and(|p| matches!(p.playing_state, playing_state::PLAYING | playing_state::PAUSED));
                (r.id, busy, st.ctrl.own_renderer.unwrap_or(-1))
            })
        };
        let Some((rid, busy, own)) = target else { return Ok(Value::Null) };
        tracing::info!("releasing Qobuz Connect renderer {rid}");
        if busy && self.send(vec![ctrl::set_player_state(Some(playing_state::STOPPED), None, None)]).is_ok() {
            // Switch only once stopped: the new active renderer takes over
            // the playing state.
            self.wait(STOP_WAIT, |st| {
                let p = st.ctrl.renderers.get(&rid)?.player?;
                (!matches!(p.playing_state, playing_state::PLAYING | playing_state::PAUSED)).then_some(())
            })
            .await;
        }
        let _ = self.send(vec![ctrl::set_active_renderer(own)]);
        Ok(Value::Null)
    }

    async fn play(&self, uuid: &str, track: u32, start_ms: u64) -> RpcResult {
        self.select(uuid).await?;
        self.update(|st| {
            st.ctrl.item_error = None;
            if let Some(sel) = &mut st.sel {
                sel.loading = Some(Loading { track, queue_item: None, start_ms, since_ms: now_ms() });
            }
        });
        let result = self.load_and_start(track, start_ms).await;
        if result.is_err() {
            self.update(|st| {
                if let Some(sel) = &mut st.sel {
                    sel.loading = None;
                }
            });
        }
        result.map(|_| Value::Null)
    }

    async fn load_and_start(&self, track: u32, start_ms: u64) -> Result<(), RpcError> {
        self.ensure_queue().await?;
        // Neither autoplay nor repeat: the device plays what ricercar gives.
        let (autoplay, repeat) = {
            let st = self.lock();
            (st.ctrl.queue.autoplay_mode, st.ctrl.loop_mode)
        };
        if autoplay != Some(false) {
            if let Err(e) = self.queue_action(|v, a| ctrl::set_autoplay_mode(v, a, false)).await {
                tracing::info!("Qobuz Connect autoplay not turned off: {}", e.message);
            }
        }
        if matches!(repeat, Some(loop_mode::REPEAT_ONE | loop_mode::REPEAT_ALL)) {
            self.send(vec![ctrl::set_loop_mode(loop_mode::OFF)])?;
        }
        let loaded = self.queue_action(|v, a| ctrl::load_tracks(v, a, &[track])).await?;
        let qid = loaded
            .iter()
            .find(|(_, t)| *t == track)
            .map(|(q, _)| *q)
            .ok_or_else(|| unavailable("Qobuz Connect did not queue the track"))?;
        let version = {
            let mut st = self.lock();
            if let Some(l) = st.sel.as_mut().and_then(|s| s.loading.as_mut()).filter(|l| l.track == track) {
                l.queue_item = Some(qid);
            }
            st.ctrl.queue.version
        };
        self.send(vec![ctrl::set_player_state(Some(playing_state::PLAYING), Some(start_ms), Some((version, qid)))])
    }

    /// Keep the remote queue as `[current, next]`: append `next` (unless it
    /// already follows), then remove every other item.
    async fn next(&self, uuid: &str, next: Option<u32>) -> RpcResult {
        let (current, follows) = {
            let st = self.lock();
            let sel = st.sel.as_ref().filter(|s| s.uuid == uuid).ok_or_else(|| unavailable("this output is not selected"))?;
            let player = st.ctrl.renderer_by_uuid(uuid).and_then(|r| r.player);
            let current = sel.loading.and_then(|l| l.queue_item).or(player.and_then(|p| p.queue_item));
            (current, current.and_then(|c| st.ctrl.queue.after(c)))
        };
        let Some(current) = current else {
            tracing::debug!("output.next while nothing is queued: ignored");
            return Ok(Value::Null);
        };
        let mut keep = follows.filter(|(_, t)| Some(*t) == next).map(|(q, _)| q);
        if let (Some(track), None) = (next, keep) {
            let added = self.queue_action(|v, a| ctrl::add_tracks(v, a, &[track])).await?;
            keep = added.iter().find(|(_, t)| *t == track).map(|(q, _)| *q);
        }
        let remove: Vec<i32> = {
            let st = self.lock();
            st.ctrl.queue.items.iter().map(|(q, _)| *q).filter(|q| *q != current && Some(*q) != keep).collect()
        };
        if !remove.is_empty() {
            self.queue_action(|v, a| ctrl::remove_tracks(v, a, &remove)).await?;
        }
        Ok(Value::Null)
    }

    /// The queue copy is known (ask for it otherwise).
    async fn ensure_queue(&self) -> Result<(), RpcError> {
        if self.lock().ctrl.queue.known {
            return Ok(());
        }
        let m = queue_request(&mut self.lock());
        self.send(vec![m])?;
        self.wait(ACK_WAIT, |st| st.ctrl.queue.known.then_some(()))
            .await
            .ok_or_else(|| unavailable("Qobuz Connect did not send its queue"))
    }

    /// Send a queue message stamped with the current version and a fresh
    /// action uuid, and wait for the server's answer. A refusal (stale
    /// version) is retried once, after a fresh copy of the queue.
    async fn queue_action(
        &self,
        build: impl Fn(Option<QueueVersion>, Vec<u8>) -> QConnectMessage,
    ) -> Result<Vec<(i32, u32)>, RpcError> {
        let mut retried = false;
        loop {
            let (m, action) = {
                let mut st = self.lock();
                let action = st.ctrl.begin_action();
                (build(st.ctrl.queue.version, action.clone()), action)
            };
            if let Err(e) = self.send(vec![m]) {
                self.lock().ctrl.cancel_action(&action);
                return Err(e);
            }
            match self.wait(ACK_WAIT, |st| st.ctrl.take_ack(&action)).await {
                Some(Ack::Done(entries)) => return Ok(entries),
                Some(Ack::Refused(reason)) if !retried => {
                    tracing::info!("Qobuz Connect refused a queue change ({reason}); retrying with a fresh queue");
                    retried = true;
                    self.ensure_queue().await?;
                }
                Some(Ack::Refused(reason)) => return Err(unavailable(format!("Qobuz Connect refused: {reason}"))),
                None => {
                    self.lock().ctrl.cancel_action(&action);
                    let m = queue_request(&mut self.lock());
                    let _ = self.send(vec![m]);
                    return Err(unavailable("Qobuz Connect did not answer"));
                }
            }
        }
    }
}

/// `AskForQueueState`; the copy is unknown until the answer.
fn queue_request(st: &mut St) -> QConnectMessage {
    st.ctrl.queue.known = false;
    ctrl::ask_for_queue_state(st.ctrl.queue.version, uuid::Uuid::new_v4().as_bytes().to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::RemotePlayer;
    use crate::lan::{Candidate, ConnectInfo, DisplayInfo};

    fn renderer(state: i32, queue_item: Option<i32>) -> Renderer {
        Renderer {
            id: 2,
            uuid: Some("6f1c2b9e-1111-4222-8333-944455556666".into()),
            name: "Salon".into(),
            brand: "Marantz".into(),
            model: "MODEL 40n".into(),
            volume: Some(35),
            muted: Some(false),
            player: Some(RemotePlayer {
                playing_state: state,
                buffer_state: buffer_state::OK,
                position_ms: 61_000,
                at_ms: 1_000,
                duration_ms: Some(215_000),
                queue_item,
            }),
            ..Default::default()
        }
    }

    fn queue() -> Queue {
        Queue { items: vec![(0, 123), (1, 124)], known: true, ..Default::default() }
    }

    #[test]
    fn parse_checks_refs_and_methods() {
        let p = |m: &str, v: Value| parse(m, v);
        assert_eq!(
            p("output.play", json!({"output": "x", "ref": "track/123", "start_ms": 5})).unwrap(),
            OutputRequest { output: "x".into(), op: Op::Play { track: 123, start_ms: 5 } }
        );
        assert_eq!(p("output.play", json!({"output": "x", "ref": "album/1"})).unwrap_err().code, rpc::INVALID_PARAMS);
        assert_eq!(p("output.next", json!({"output": "x", "ref": null})).unwrap().op, Op::Next(None));
        assert_eq!(p("output.next", json!({"output": "x", "ref": "track/9"})).unwrap().op, Op::Next(Some(9)));
        assert_eq!(p("output.seek", json!({"output": "x"})).unwrap_err().code, rpc::INVALID_PARAMS);
        assert_eq!(
            p("output.volume", json!({"output": "x", "percent": 120.4})).unwrap().op,
            Op::Volume { percent: Some(100), muted: None }
        );
        assert_eq!(p("output.dance", json!({})).unwrap_err().code, rpc::METHOD_NOT_FOUND);
        assert_eq!(p("output.stop", json!({})).unwrap_err().code, rpc::INVALID_PARAMS);
    }

    #[test]
    fn list_merges_session_and_lan_by_uuid() {
        let mut st = CtrlState::new("00000000-0000-4000-8000-000000000001");
        st.renderers.insert(2, renderer(1, None));
        let probed = |uuid: &str, name: &str| Probed {
            candidate: Candidate {
                device_uuid: uuid.into(),
                path: String::new(),
                fullname: format!("{name}._qobuz-connect._tcp.local."),
                addresses: vec![],
                port: 80,
            },
            address: "192.168.1.2".parse().unwrap(),
            display: DisplayInfo { friendly_name: name.into(), brand_display_name: "Bluesound".into(), model_display_name: "NODE".into() },
            connect: ConnectInfo { app_id: "1".into() },
        };
        let lan = HashMap::from([
            ("6f1c2b9e-1111-4222-8333-944455556666".to_string(), probed("6f1c2b9e-1111-4222-8333-944455556666", "Salon LAN")),
            ("aaaaaaaa-1111-4222-8333-944455556666".to_string(), probed("aaaaaaaa-1111-4222-8333-944455556666", "Bureau")),
        ]);
        assert_eq!(
            serde_json::to_value(output_list(&st, &lan)).unwrap(),
            json!([
                {"id": "aaaaaaaa-1111-4222-8333-944455556666", "name": "Bureau", "detail": "Bluesound NODE", "badge": "Qobuz Connect"},
                {"id": "6f1c2b9e-1111-4222-8333-944455556666", "name": "Salon", "detail": "Marantz MODEL 40n", "badge": "Qobuz Connect"},
            ])
        );
    }

    #[test]
    fn state_maps_the_remote_player() {
        let q = queue();
        let r = renderer(playing_state::PLAYING, Some(0));
        let s = output_state(Some(&r), true, &q, None, None, None, 3_000);
        assert_eq!(
            serde_json::to_value(&s).unwrap(),
            json!({"status": "playing", "ref": "track/123", "pos_ms": 63_000, "dur_ms": 215_000, "volume": 35, "muted": false})
        );
        let paused = renderer(playing_state::PAUSED, Some(1));
        let s = output_state(Some(&paused), true, &q, None, None, None, 3_000);
        assert_eq!((s.status, s.reference.as_deref(), s.pos_ms), ("paused", Some("track/124"), Some(61_000)));

        // While loading: the new track, whatever the device still reports.
        let l = Loading { track: 124, queue_item: Some(1), start_ms: 5_000, since_ms: 0 };
        let s = output_state(Some(&r), true, &q, Some(&l), None, None, 3_000);
        assert_eq!((s.status, s.reference.as_deref(), s.pos_ms), ("loading", Some("track/124"), Some(5_000)));

        // The device failed on the loading item.
        let err = ItemError { queue_item: 1, kind: playback_error::TRACK_NOT_STREAMABLE };
        let s = output_state(Some(&r), true, &q, Some(&l), Some(err), None, 3_000);
        assert_eq!(s.status, "stopped");
        assert_eq!(s.error.unwrap().code, Some(rpc::UNAVAILABLE));

        // Another device took over: stopped where it was.
        let playing = output_state(Some(&r), true, &q, None, None, None, 3_000);
        let s = output_state(Some(&r), false, &q, None, None, Some(&playing), 4_000);
        assert_eq!((s.status, s.reference.as_deref(), s.pos_ms), ("stopped", Some("track/123"), Some(63_000)));

        // End of queue reported as stopped on no item, rewound: still the
        // last track, at its end.
        let near_end = OutputState { pos_ms: Some(214_000), ..playing.clone() };
        let mut ended = renderer(playing_state::STOPPED, None);
        ended.player.as_mut().unwrap().position_ms = 0;
        let s = output_state(Some(&ended), true, &q, None, None, Some(&near_end), 5_000);
        assert_eq!((s.status, s.reference.as_deref(), s.pos_ms, s.dur_ms), ("stopped", Some("track/123"), Some(214_000), Some(215_000)));
    }

    #[test]
    fn state_is_sent_on_change_and_every_second_while_playing() {
        let playing = OutputState {
            status: "playing",
            reference: Some("track/1".into()),
            pos_ms: Some(10_000),
            dur_ms: Some(100_000),
            volume: Some(20),
            muted: Some(false),
            error: None,
        };
        assert!(worth_sending(None, &playing, 0, false));
        let later = OutputState { pos_ms: Some(10_500), ..playing.clone() };
        assert!(!worth_sending(Some((&playing, 0)), &later, 500, false), "position only");
        assert!(!worth_sending(Some((&playing, 0)), &later, 500, true), "too soon");
        let tick = OutputState { pos_ms: Some(11_000), ..playing.clone() };
        assert!(worth_sending(Some((&playing, 0)), &tick, 1_000, true));
        let seek = OutputState { pos_ms: Some(50_000), ..playing.clone() };
        assert!(worth_sending(Some((&playing, 0)), &seek, 500, false), "seek");
        let louder = OutputState { volume: Some(30), ..playing.clone() };
        assert!(worth_sending(Some((&playing, 0)), &louder, 100, false));
        let paused = OutputState { status: "paused", ..playing.clone() };
        assert!(!worth_sending(Some((&paused, 0)), &paused, 5_000, true), "paused: on change only");
    }
}
