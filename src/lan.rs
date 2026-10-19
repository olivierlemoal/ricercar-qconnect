//! Qobuz Connect devices on the local network: mDNS discovery
//! (`_qobuz-connect._tcp`), their HTTP API (`get-display-info`,
//! `get-connect-info`) and pairing (`connect-to-qconnect`), as the official
//! controllers do it. A paired device joins the account's session itself.

use std::net::IpAddr;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use mdns_sd::{ServiceDaemon, ServiceEvent};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;

pub const SERVICE_TYPE: &str = "_qobuz-connect._tcp.local.";
/// Official controllers ignore devices announcing an older SDK.
const MIN_SDK_VERSION: (u32, u32, u32) = (0, 9, 5);
/// Per request, as the official controllers.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// One `_qobuz-connect._tcp` announcement.
#[derive(Debug, Clone, PartialEq)]
pub struct Candidate {
    /// Normalised (see [`normalize_uuid`]).
    pub device_uuid: String,
    /// The announced path, without surrounding `/`.
    pub path: String,
    pub fullname: String,
    /// IPv4 first.
    pub addresses: Vec<IpAddr>,
    pub port: u16,
}

impl Candidate {
    /// `None` without `device_uuid` and `sdk_version`, or with an SDK too old.
    pub fn new(txt: impl Fn(&str) -> Option<String>, fullname: &str, addresses: &[IpAddr], port: u16) -> Option<Self> {
        let device_uuid = normalize_uuid(&txt("device_uuid")?)?;
        if !sdk_version_ok(txt("sdk_version")?.trim()) {
            return None;
        }
        let path = txt("path").map(|p| p.trim().trim_matches('/').to_string()).unwrap_or_default();
        Some(Self { device_uuid, path, fullname: fullname.to_string(), addresses: ordered_addresses(addresses), port })
    }
}

/// Device uuids as one string form, so the LAN announcement (TXT, free form)
/// and the session (16 bytes) match: lowercase and hyphenated when it parses
/// as a uuid, else trimmed and lowercased. `None` if unusable as an output id.
pub fn normalize_uuid(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if let Ok(u) = uuid::Uuid::parse_str(raw) {
        return Some(u.to_string());
    }
    let ok = !raw.is_empty()
        && raw.len() <= 64
        && raw.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | ':' | '-'));
    ok.then(|| raw.to_ascii_lowercase())
}

/// A version that does not parse as `x.y.z` is accepted, as the Android app
/// does (BluOS announces `bluos`).
fn sdk_version_ok(version: &str) -> bool {
    let mut parts = version.split('.').map(|p| p.trim().parse::<u32>());
    let (Some(Ok(major)), Some(Ok(minor)), Some(Ok(patch))) = (parts.next(), parts.next(), parts.next()) else {
        return true;
    };
    (major, minor, patch) >= MIN_SDK_VERSION
}

/// IPv4 before IPv6, without loopback or duplicates.
fn ordered_addresses(addresses: &[IpAddr]) -> Vec<IpAddr> {
    let mut out: Vec<IpAddr> = Vec::new();
    for a in addresses.iter().filter(|a| a.is_ipv4()).chain(addresses.iter().filter(|a| a.is_ipv6())) {
        if !a.is_loopback() && !out.contains(a) {
            out.push(*a);
        }
    }
    out
}

/// `http://{host}:{port}/{path}/{endpoint}`, IPv6 hosts bracketed, an empty
/// path left out.
pub fn endpoint_url(address: &IpAddr, port: u16, path: &str, endpoint: &str) -> String {
    let host = match address {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => format!("[{v6}]"),
    };
    let path = path.trim_matches('/');
    let endpoint = endpoint.trim_matches('/');
    if path.is_empty() {
        format!("http://{host}:{port}/{endpoint}")
    } else {
        format!("http://{host}:{port}/{path}/{endpoint}")
    }
}

/// `GET get-display-info`. Every field is optional: devices differ.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct DisplayInfo {
    #[serde(default)]
    pub friendly_name: String,
    #[serde(default)]
    pub brand_display_name: String,
    #[serde(default)]
    pub model_display_name: String,
}

/// `GET get-connect-info`.
#[derive(Debug, Clone, Deserialize)]
pub struct ConnectInfo {
    /// The device's app id, for `qws/delegateAuth`.
    pub app_id: String,
}

/// A device that answered both requests, on `address`.
#[derive(Debug, Clone)]
pub struct Probed {
    pub candidate: Candidate,
    pub address: IpAddr,
    pub display: DisplayInfo,
    pub connect: ConnectInfo,
}

/// A delegated token as the device expects it.
#[derive(Debug, Serialize)]
pub struct LanToken {
    pub endpoint: String,
    pub exp: i64,
    pub jwt: String,
}

/// `POST connect-to-qconnect` body. The QWS token goes as `jwt_qconnect`.
#[derive(Serialize)]
pub struct HandoffBody {
    /// The session's uuid (string form).
    pub session_id: String,
    pub jwt_api: LanToken,
    pub jwt_qconnect: LanToken,
    /// Devices refuse `false`.
    pub become_active: bool,
}

/// Plain HTTP client for devices on the LAN: no proxy, `Connection: close`.
pub fn http_client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder().timeout(REQUEST_TIMEOUT).no_proxy().build()?)
}

/// `get-display-info` then `get-connect-info`, on the first address where
/// both answer.
pub async fn probe(http: &reqwest::Client, candidate: Candidate) -> Result<Probed> {
    let mut last = anyhow!("no address");
    for address in candidate.addresses.clone() {
        let url = |endpoint| endpoint_url(&address, candidate.port, &candidate.path, endpoint);
        let display = match get_json::<DisplayInfo>(http, &url("get-display-info")).await {
            Ok(d) => d,
            Err(e) => {
                last = e;
                continue;
            }
        };
        match get_json::<ConnectInfo>(http, &url("get-connect-info")).await {
            Ok(connect) => return Ok(Probed { candidate, address, display, connect }),
            Err(e) => last = e,
        }
    }
    Err(last)
}

/// Hand the device the session and its tokens. Only HTTP 200 counts.
pub async fn hand_off(http: &reqwest::Client, device: &Probed, body: &HandoffBody) -> Result<()> {
    let c = &device.candidate;
    let url = endpoint_url(&device.address, c.port, &c.path, "connect-to-qconnect");
    let response = http
        .post(&url)
        .header(reqwest::header::CONNECTION, "close")
        .json(body)
        .send()
        .await?;
    if response.status().as_u16() != 200 {
        bail!("{url}: HTTP {}", response.status());
    }
    Ok(())
}

async fn get_json<T: for<'de> Deserialize<'de>>(http: &reqwest::Client, url: &str) -> Result<T> {
    let response = http.get(url).header(reqwest::header::CONNECTION, "close").send().await?;
    if response.status().as_u16() != 200 {
        bail!("{url}: HTTP {}", response.status());
    }
    Ok(serde_json::from_str(&response.text().await?)?)
}

#[derive(Debug)]
pub enum LanEvent {
    Found(Candidate),
    /// Fullname of a service that went away.
    Lost(String),
}

/// Running mDNS browser; stops on drop.
pub struct Browser {
    daemon: ServiceDaemon,
}

impl Drop for Browser {
    fn drop(&mut self) {
        let _ = self.daemon.shutdown();
    }
}

/// Browse `_qobuz-connect._tcp` until the returned [`Browser`] is dropped.
pub fn browse(events: UnboundedSender<LanEvent>) -> Result<Browser> {
    let daemon = ServiceDaemon::new()?;
    let receiver = daemon.browse(SERVICE_TYPE)?;
    std::thread::Builder::new().name("qconnect-mdns".into()).spawn(move || {
        // Ends when the daemon shuts down (channel closed) or nobody listens.
        while let Ok(event) = receiver.recv() {
            let event = match event {
                ServiceEvent::ServiceResolved(info) => {
                    let addresses: Vec<IpAddr> = info.get_addresses().iter().map(|a| a.to_ip_addr()).collect();
                    let txt = |key: &str| info.get_property_val_str(key).map(str::to_string);
                    match Candidate::new(txt, info.get_fullname(), &addresses, info.get_port()) {
                        Some(c) => LanEvent::Found(c),
                        None => {
                            tracing::debug!("mdns: ignoring {} (no usable device_uuid or SDK too old)", info.get_fullname());
                            continue;
                        }
                    }
                }
                ServiceEvent::ServiceRemoved(_, fullname) => LanEvent::Lost(fullname),
                _ => continue,
            };
            if events.send(event).is_err() {
                break;
            }
        }
    })?;
    Ok(Browser { daemon })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn uuids_match_across_lan_and_session_forms() {
        let session = uuid::Uuid::from_bytes([0x6f; 16]).to_string();
        assert_eq!(normalize_uuid("6F6F6F6F6F6F6F6F6F6F6F6F6F6F6F6F").as_deref(), Some(session.as_str()));
        assert_eq!(normalize_uuid(&format!(" {} ", session.to_uppercase())).as_deref(), Some(session.as_str()));
        assert_eq!(normalize_uuid("Heos-123").as_deref(), Some("heos-123"));
        assert_eq!(normalize_uuid("a b"), None);
        assert_eq!(normalize_uuid(""), None);
    }

    #[test]
    fn candidates_need_uuid_and_recent_sdk() {
        let addrs: Vec<IpAddr> =
            ["fe80::1", "192.168.1.20", "127.0.0.1", "192.168.1.20"].iter().map(|a| a.parse().unwrap()).collect();
        let txt = |pairs: &[(&str, &str)]| {
            let map: HashMap<String, String> = pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
            move |k: &str| map.get(k).cloned()
        };
        let uuid = "6f1c2b9e-1111-4222-8333-944455556666";
        let c = Candidate::new(
            txt(&[("device_uuid", "6f1c2b9e-1111-4222-8333-944455556666"), ("sdk_version", "0.9.6"), ("path", "/qobuz/")]),
            "Salon._qobuz-connect._tcp.local.",
            &addrs,
            8080,
        )
        .unwrap();
        assert_eq!(c.device_uuid, uuid);
        assert_eq!(c.path, "qobuz");
        assert_eq!(c.addresses, vec!["192.168.1.20".parse::<IpAddr>().unwrap(), "fe80::1".parse().unwrap()]);
        assert!(Candidate::new(txt(&[("device_uuid", uuid), ("sdk_version", "bluos")]), "x", &addrs, 1).is_some());
        assert!(Candidate::new(txt(&[("device_uuid", uuid), ("sdk_version", "0.9.4")]), "x", &addrs, 1).is_none());
        assert!(Candidate::new(txt(&[("sdk_version", "1.0.0")]), "x", &addrs, 1).is_none());
        assert!(Candidate::new(txt(&[("device_uuid", uuid)]), "x", &addrs, 1).is_none());
    }

    #[test]
    fn endpoint_urls() {
        let v4: IpAddr = "192.168.0.7".parse().unwrap();
        assert_eq!(endpoint_url(&v4, 8765, "", "get-display-info"), "http://192.168.0.7:8765/get-display-info");
        assert_eq!(endpoint_url(&v4, 8765, "/qobuz/", "connect-to-qconnect"), "http://192.168.0.7:8765/qobuz/connect-to-qconnect");
        let v6: IpAddr = "fe80::1".parse().unwrap();
        assert_eq!(endpoint_url(&v6, 80, "", "x"), "http://[fe80::1]:80/x");
    }

    #[test]
    fn handoff_body_has_the_official_keys() {
        let token = |jwt: &str| LanToken { endpoint: "e".into(), exp: 1, jwt: jwt.into() };
        let body = HandoffBody { session_id: "s".into(), jwt_api: token("a"), jwt_qconnect: token("q"), become_active: true };
        assert_eq!(
            serde_json::to_value(&body).unwrap(),
            serde_json::json!({"session_id": "s", "jwt_api": {"endpoint": "e", "exp": 1, "jwt": "a"},
                               "jwt_qconnect": {"endpoint": "e", "exp": 1, "jwt": "q"}, "become_active": true})
        );
        let info: DisplayInfo = serde_json::from_str(
            r#"{"friendly_name":"Node","serial_number":"1","brand_display_name":"Bluesound","model_display_name":"NODE","max_audio_quality":"UP_TO_DSD","type":"Fridge"}"#,
        )
        .unwrap();
        assert_eq!((info.friendly_name.as_str(), info.brand_display_name.as_str()), ("Node", "Bluesound"));
    }
}
