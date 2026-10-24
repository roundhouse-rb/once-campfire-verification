//! Load generator for the Campfire benchmark (bench/run). Talks plain HTTP/1.1 and Action Cable
//! (WebSocket) to one server and prints one JSON object per command on stdout.
//!
//!   loadgen login  --base URL --email E --password P            -> {"cookie": "..."}
//!   loadgen scrape --base URL --cookie C --room ID              -> csrf token, stream names, assets
//!   loadgen http   --base URL --cookie C --path P --conc N --duration S
//!                  [--post-room ID --csrf T]                     -> latency/throughput
//!                  [--mixed-write-rate N --mixed-write-room ID --mixed-write-validate FILE --mixed-write-audit FILE]
//!   loadgen cable  --base URL --cookie C --room ID --csrf T --clients N [--streams a,b,c]
//!                  [--sources 127.0.0.2,127.0.0.3 --hold-secs 60 --deflate 1]
//!                  [--latency-msgs 30 --interval-ms 200 --tput-secs 15 --posters 4]
//!                  [--sessions FILE --refresh-secs 50]
//!   loadgen upload --base URL --cookie C --room ID --csrf T --file PATH [--reps 5]
//!   loadgen fetch  --base URL --cookie C --path P --out FILE     -> saves an uncompressed body
//!   loadgen gzip   --file F [--iters 200]                         -> CPU per compression, by backend/level
//!   loadgen users  --base URL --plan FILE --viewers N --rates R1,R2 -> many people browsing and posting (users.rs)
//!
//! `http` also takes `--gzip 0` (`Accept-Encoding: identity`), `--requests N` (stop after N requests,
//! for allocation counting) and `--trace FILE` (each request's start, latency and status). `cable` prints `PHASE <name> <unix ms>` lines on stderr so memory
//! samples can be attributed to its phases.
//!
//! By default every `cable` client is a tab of the `--cookie` user. `--sessions FILE` supplies separate sessions:
//! one line per session, `cookie<TAB>identifier<TAB>identifier...` (the subscriptions that session's page makes), client n
//! taking line n % lines, so per-member work (each person's unread notice, presence row and user streams) is exercised.
//! `--refresh-secs S` sends `PresenceChannel#refresh` every S seconds from each client, as a visible room page does
//! every 50; it needs a PresenceChannel identifier among the subscriptions and is not supported with `--deflate`.
//! The result counts unread notices and refreshes sent, supplied session rows and distinct cookie strings.
//! Cookie strings do not prove distinct people; create sessions for independently verified fixture users.
//! Subscription counts report a range and are null when clients use different counts.
//!
//! Every command takes `--user-agent UA`, sent as the `User-Agent` of each request it makes,
//! WebSocket handshakes included; without it there is no `User-Agent` header. For example, a
//! desktop Chrome: `--user-agent 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36
//! (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'` (one line).

mod users;
mod validation;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use hdrhistogram::Histogram;
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1::SendRequest;
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::{Message as WsMessage, client::IntoClientRequest};

type Res<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct Args(HashMap<String, String>);

impl Args {
    fn parse(raw: &[String]) -> Self {
        let mut map = HashMap::new();
        let mut it = raw.iter();
        while let Some(k) = it.next() {
            if let Some(k) = k.strip_prefix("--") {
                map.insert(k.to_string(), it.next().cloned().unwrap_or_default());
            }
        }
        Args(map)
    }
    fn get(&self, k: &str) -> String {
        self.0.get(k).cloned().unwrap_or_else(|| panic!("missing --{k}"))
    }
    fn opt(&self, k: &str) -> Option<String> {
        self.0.get(k).cloned()
    }
    fn num<T: std::str::FromStr>(&self, k: &str, default: T) -> T {
        self.0.get(k).and_then(|v| v.parse().ok()).unwrap_or(default)
    }
}

/// `--user-agent`, set once in `main`.
static USER_AGENT: OnceLock<Option<String>> = OnceLock::new();

fn user_agent() -> Option<&'static str> {
    USER_AGENT.get().and_then(Option::as_deref)
}

fn host_port(base: &str) -> String {
    base.trim_start_matches("http://").trim_end_matches('/').to_string()
}

async fn connect(addr: &str) -> Res<SendRequest<Full<Bytes>>> {
    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    let (sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Ok(sender)
}

struct Resp {
    status: u16,
    headers: hyper::HeaderMap,
    body: Bytes,
}

async fn send(
    sender: &mut SendRequest<Full<Bytes>>,
    addr: &str,
    method: &str,
    path: &str,
    headers: &[(&str, String)],
    body: Bytes,
) -> Res<Resp> {
    let mut req = hyper::Request::builder().method(method).uri(path).header("host", addr);
    if let Some(user_agent) = user_agent() {
        req = req.header("user-agent", user_agent);
    }
    for (k, v) in headers {
        req = req.header(*k, v.as_str());
    }
    let req = req.body(Full::new(body))?;
    // A request that takes longer than this counts as an error (and drops the connection).
    tokio::time::timeout(Duration::from_secs(30), async {
        sender.ready().await?;
        let resp = sender.send_request(req).await?;
        let status = resp.status().as_u16();
        let headers = resp.headers().clone();
        let body = resp.into_body().collect().await?.to_bytes();
        Ok(Resp { status, headers, body })
    })
    .await
    .map_err(|_| "request timed out after 30s")?
}

async fn one_shot(addr: &str, method: &str, path: &str, headers: &[(&str, String)], body: Bytes) -> Res<Resp> {
    let mut s = connect(addr).await?;
    send(&mut s, addr, method, path, headers, body).await
}

fn urlencode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn form(pairs: &[(&str, &str)]) -> Bytes {
    Bytes::from(pairs.iter().map(|(k, v)| format!("{}={}", urlencode(k), urlencode(v))).collect::<Vec<_>>().join("&"))
}

fn merge_cookies(jar: &mut Vec<(String, String)>, headers: &hyper::HeaderMap) {
    for v in headers.get_all("set-cookie") {
        let s = v.to_str().unwrap_or("");
        let pair = s.split(';').next().unwrap_or("");
        if let Some((k, v)) = pair.split_once('=') {
            jar.retain(|(n, _)| n != k);
            jar.push((k.to_string(), v.to_string()));
        }
    }
}

fn cookie_header(jar: &[(String, String)]) -> String {
    jar.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("; ")
}

/// Optional token support for historical references; current apps use `Sec-Fetch-Site`.
fn csrf_from(html: &str) -> Option<String> {
    regex::Regex::new(r#"<meta name="csrf-token" content="([^"]*)""#).unwrap().captures(html).map(|c| c[1].to_string())
}

/// What a browser sends with requests its pages make.
fn same_origin() -> (&'static str, String) {
    ("sec-fetch-site", "same-origin".into())
}

fn unescape(s: &str) -> String {
    s.replace("&amp;", "&").replace("&quot;", "\"").replace("&#39;", "'")
}

async fn login(a: &Args) -> Res<Value> {
    let addr = host_port(&a.get("base"));
    let mut jar = Vec::new();
    let r = one_shot(&addr, "GET", "/session/new", &[], Bytes::new()).await?;
    merge_cookies(&mut jar, &r.headers);
    let token = csrf_from(&String::from_utf8_lossy(&r.body)).unwrap_or_default();
    let (email, password) = (a.get("email"), a.get("password"));
    let mut fields = vec![("email_address", email.as_str()), ("password", password.as_str())];
    if !token.is_empty() {
        fields.push(("authenticity_token", &token));
    }
    let body = form(&fields);
    let r = one_shot(
        &addr,
        "POST",
        "/session",
        &[("cookie", cookie_header(&jar)), ("content-type", "application/x-www-form-urlencoded".into()), same_origin()],
        body,
    )
    .await?;
    merge_cookies(&mut jar, &r.headers);
    if r.status != 302 || !jar.iter().any(|(k, _)| k == "session_token") {
        return Err(format!("login failed: {}", r.status).into());
    }
    Ok(json!({ "cookie": cookie_header(&jar) }))
}

async fn scrape(a: &Args) -> Res<Value> {
    let addr = host_port(&a.get("base"));
    let path = format!("/rooms/{}", a.get("room"));
    let r = one_shot(&addr, "GET", &path, &[("cookie", a.get("cookie"))], Bytes::new()).await?;
    let mut html = String::from_utf8_lossy(&r.body).to_string();
    // The sidebar frame (loaded lazily by the page) carries the rooms/user-rooms stream sources.
    let sidebar = one_shot(&addr, "GET", "/users/me/sidebar", &[("cookie", a.get("cookie"))], Bytes::new()).await?;
    html.push_str(&String::from_utf8_lossy(&sidebar.body));
    let streams: Vec<String> = {
        // "Channel|signed name" for each <turbo-cable-stream-source>.
        let re = regex::Regex::new(r#"<turbo-cable-stream-source channel="([^"]+)" signed-stream-name="([^"]+)""#).unwrap();
        let mut seen = HashSet::new();
        re.captures_iter(&html).map(|c| format!("{}|{}", &c[1], unescape(&c[2]))).filter(|s| seen.insert(s.clone())).collect()
    };
    let css = regex::Regex::new(r#"href="(/assets/[^"]+\.css)""#).unwrap().captures(&html).map(|c| c[1].to_string());
    let js = regex::Regex::new(r#"(/assets/[^"]+\.js)""#).unwrap().captures(&html).map(|c| c[1].to_string());
    Ok(json!({
        "status": r.status,
        "bytes": r.body.len(),
        "csrf": csrf_from(&html),
        "streams": streams,
        "css": css,
        "js": js,
    }))
}

fn hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 120_000_000, 3).unwrap()
}

fn ms(us: u64) -> f64 {
    (us as f64 / 1000.0 * 1000.0).round() / 1000.0
}

fn summary(h: &Histogram<u64>) -> Value {
    if h.is_empty() {
        return json!({"n": 0});
    }
    json!({
        "n": h.len(),
        "p50_ms": ms(h.value_at_quantile(0.5)),
        "p90_ms": ms(h.value_at_quantile(0.9)),
        "p99_ms": ms(h.value_at_quantile(0.99)),
        "max_ms": ms(h.max()),
        "mean_ms": (h.mean() / 10.0).round() / 100.0,
    })
}

static SEQ: AtomicU64 = AtomicU64::new(0);

fn nonce() -> String {
    let t = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("{:x}{:x}", t, SEQ.fetch_add(1, Ordering::Relaxed))
}

fn message_request(cookie: &str, csrf: &str, body_text: &str) -> (Vec<(&'static str, String)>, Bytes) {
    let client_id = nonce();
    let mut fields = vec![("message[body]", body_text), ("message[client_message_id]", client_id.as_str())];
    let mut headers = vec![
        ("cookie", cookie.to_string()),
        ("content-type", "application/x-www-form-urlencoded".into()),
        ("accept", "text/vnd.turbo-stream.html, text/html, application/xhtml+xml".into()),
        same_origin(),
    ];
    if !csrf.is_empty() {
        fields.push(("authenticity_token", csrf));
        headers.push(("x-csrf-token", csrf.to_string()));
    }
    (headers, form(&fields))
}

async fn http_load(a: &Args) -> Res<Value> {
    let start = Instant::now();
    let Some(rate) = a.opt("mixed-write-rate") else { return http_window(a, start).await };
    let rate: u64 = rate.parse().map_err(|_| "invalid mixed write rate")?;
    if !(1..=100).contains(&rate) || a.opt("post-room").is_some() || a.opt("validate").is_none() {
        return Err("mixed reads require validation and a write rate from 1 to 100 per second".into());
    }
    let mut writer = a.0.clone();
    writer.remove("mixed-write-rate");
    writer.remove("requests");
    writer.remove("trace");
    writer.insert("post-room".into(), a.opt("mixed-write-room").ok_or("missing mixed write room")?);
    writer.insert("validate".into(), a.opt("mixed-write-validate").ok_or("missing mixed write contract")?);
    writer.insert("audit-writes".into(), a.opt("mixed-write-audit").ok_or("missing mixed write audit")?);
    writer.insert("conc".into(), "1".into());
    writer.insert("interval-ms".into(), 1000u64.div_ceil(rate).to_string());
    let writer = Args(writer);
    let (reads, writes) = tokio::join!(http_window(a, start), http_window(&writer, start));
    let mut reads = reads?;
    reads["writer"] = writes?;
    reads["profile"] = json!("mixed-read-write-v1");
    reads["writer_rate_limit"] = json!(rate);
    Ok(reads)
}

async fn http_window(a: &Args, start: Instant) -> Res<Value> {
    let addr = host_port(&a.get("base"));
    let cookie = a.opt("cookie").unwrap_or_default();
    let path = a.opt("path").unwrap_or_else(|| "/".into());
    let conc: usize = a.num("conc", 1);
    let duration = Duration::from_secs_f64(a.num("duration", 10.0));
    let post_room = a.opt("post-room");
    let csrf = a.opt("csrf").unwrap_or_default();
    let gzip = a.num("gzip", 1u8) != 0;
    let accept_encoding = if gzip { "gzip" } else { "identity" };
    let limit: u64 = a.num("requests", u64::MAX);
    let trace_path = a.opt("trace");
    let validation_path = a.opt("validate");
    let audit_path = a.opt("audit-writes");
    let interval = a.num("interval-ms", 0u64);
    if interval > 0 && (conc != 1 || post_room.is_none() || audit_path.is_none() || validation_path.is_none()) {
        return Err("paced writes require one POST client, validation and an audit".into());
    }
    if audit_path.is_some() && (validation_path.is_none() || post_room.is_none()) {
        return Err("--audit-writes requires POST and --validate".into());
    }
    let audit = Arc::new(Mutex::new(Vec::<(u64, String)>::new()));
    let trace = Arc::new(Mutex::new(Vec::<(u128, u64, u16)>::new()));
    let issued = Arc::new(AtomicU64::new(0));

    let hist_all = Arc::new(Mutex::new(hist()));
    let statuses = Arc::new(Mutex::new(HashMap::<u16, u64>::new()));
    let errors = Arc::new(AtomicU64::new(0));
    let invalid_responses = Arc::new(AtomicU64::new(0));
    let successful = Arc::new(AtomicU64::new(0));
    let invalid_reasons = Arc::new(Mutex::new(HashMap::<String, u64>::new()));
    let bytes_total = Arc::new(AtomicU64::new(0));
    let deadline = start + duration;
    let mut tasks = Vec::new();
    for _ in 0..conc {
        let (addr, cookie, path, post_room, csrf) = (addr.clone(), cookie.clone(), path.clone(), post_room.clone(), csrf.clone());
        let invalid_responses = invalid_responses.clone();
        let successful = successful.clone();
        let invalid_reasons = invalid_reasons.clone();
        let (hist_all, statuses, errors, bytes_total, issued, trace) =
            (hist_all.clone(), statuses.clone(), errors.clone(), bytes_total.clone(), issued.clone(), trace.clone());
        let mut validator =
            validation_path.as_ref().map(|path| validation::Validator::from_file(std::path::Path::new(path))).transpose()?;
        let audit = audit.clone();
        let auditing = audit_path.is_some();
        let tracing = trace_path.is_some();
        tasks.push(tokio::spawn(async move {
            let mut h = hist();
            let mut local = HashMap::<u16, u64>::new();
            let mut conn: Option<SendRequest<Full<Bytes>>> = None;
            let mut successful_local = 0;
            let mut invalid_reason = None;
            let mut local_trace = Vec::new();
            let mut local_audit = Vec::new();
            let mut next_issue = start;
            while Instant::now() < deadline && issued.fetch_add(1, Ordering::Relaxed) < limit {
                if interval > 0 {
                    tokio::time::sleep_until(next_issue.into()).await;
                    if Instant::now() >= deadline {
                        break;
                    }
                    // Slow responses never produce a catch-up burst.
                    next_issue = Instant::now() + Duration::from_millis(interval);
                }
                if conn.is_none() {
                    match connect(&addr).await {
                        Ok(c) => conn = Some(c),
                        Err(_) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            continue;
                        }
                    }
                }
                let posted = post_room.as_ref().map(|_| format!("bench write {}", nonce()));
                let (method, p, headers, body) = match &post_room {
                    Some(room) => {
                        let (mut h, b) = message_request(&cookie, &csrf, posted.as_deref().unwrap());
                        h.push(("accept-encoding", accept_encoding.into()));
                        ("POST", format!("/rooms/{room}/messages"), h, b)
                    }
                    None => {
                        let mut h = vec![("cookie", cookie.clone())];
                        h.push(("accept-encoding", accept_encoding.into()));
                        ("GET", path.clone(), h, Bytes::new())
                    }
                };
                let t0 = Instant::now();
                let wall0 =
                    if tracing { std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros() } else { 0 };
                match send(conn.as_mut().unwrap(), &addr, method, &p, &headers, body).await {
                    Ok(r) => {
                        h.record(t0.elapsed().as_micros() as u64).ok();
                        let valid = if let Some(v) = &mut validator {
                            v.check(r.status, &r.headers, &r.body, posted.as_deref())
                        } else if r.status == 200 && !r.body.is_empty() {
                            Ok(())
                        } else {
                            Err("unsuccessful or empty response")
                        };
                        if valid.is_ok() {
                            successful_local += 1;
                        }
                        if auditing
                            && valid.is_ok()
                            && let Some(id) = validator.as_ref().and_then(validation::Validator::message_id)
                        {
                            local_audit.push((id, posted.as_ref().unwrap().clone()));
                        }
                        if let Err(reason) = valid {
                            invalid_responses.fetch_add(1, Ordering::Relaxed);
                            invalid_reason.get_or_insert(reason);
                            *invalid_reasons.lock().unwrap().entry(reason.to_owned()).or_default() += 1;
                        }
                        if tracing {
                            local_trace.push((wall0, t0.elapsed().as_micros() as u64, r.status));
                        }
                        *local.entry(r.status).or_default() += 1;
                        bytes_total.fetch_add(r.body.len() as u64, Ordering::Relaxed);
                        if r.headers.get("connection").map(|v| v == "close").unwrap_or(false) {
                            conn = None;
                        }
                    }
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                        conn = None;
                    }
                }
            }
            if let Some(reason) = invalid_reason {
                eprintln!("invalid response: {reason}");
            }
            successful.fetch_add(successful_local, Ordering::Relaxed);
            audit.lock().unwrap().extend(local_audit);
            hist_all.lock().unwrap().add(&h).unwrap();
            trace.lock().unwrap().extend(local_trace);
            let mut s = statuses.lock().unwrap();
            for (k, v) in local {
                *s.entry(k).or_default() += v;
            }
        }));
    }
    for t in tasks {
        t.await?;
    }
    let elapsed = start.elapsed().as_secs_f64();
    if let Some(path) = &trace_path {
        // One line per request: start (unix µs), latency (µs), status.
        let mut t = trace.lock().unwrap();
        t.sort();
        let lines: String = t.iter().map(|(s, l, st)| format!("{s} {l} {st}\n")).collect();
        std::fs::write(path, lines)?;
    }
    if let Some(path) = audit_path {
        use std::io::Write;
        let mut file = std::io::BufWriter::new(std::fs::File::create(path)?);
        for (id, token) in audit.lock().unwrap().iter() {
            serde_json::to_writer(&mut file, &json!({"id":id,"token":token}))?;
            file.write_all(b"\n")?;
        }
        file.flush()?;
    }
    let h = hist_all.lock().unwrap();
    let st = statuses.lock().unwrap();
    let ok = successful.load(Ordering::Relaxed);
    Ok(json!({
        "path": if let Some(r) = &post_room { format!("POST /rooms/{r}/messages") } else { path },
        "conc": conc,
        "gzip": gzip,
        "secs": (elapsed * 100.0).round() / 100.0,
        "rps": ((ok as f64 / elapsed) * 10.0).round() / 10.0,
        "ok": ok,
        "statuses": st.iter().map(|(k, v)| (k.to_string(), json!(v))).collect::<serde_json::Map<_, _>>(),
        "errors": errors.load(Ordering::Relaxed),
        "invalid_responses": invalid_responses.load(Ordering::Relaxed),
        "invalid_reasons": *invalid_reasons.lock().unwrap(),
        "validation": if validation_path.is_some() { "route-contract-v1" } else { "status-only" },
        "avg_bytes": if !h.is_empty() { bytes_total.load(Ordering::Relaxed) / h.len() } else { 0 },
        "latency": summary(&h),
    }))
}

// ---------------------------------------------------------------------------------------------
// Action Cable fan-out

struct Delivery {
    sent: Mutex<HashMap<u64, Instant>>,
    got: Mutex<HashMap<u64, (usize, Instant)>>, // seq -> (clients received, last receipt)
    per_client: Mutex<Histogram<u64>>,
    receipts: AtomicU64,
    /// Bytes read off the sockets (`--deflate` clients only): what the network carries.
    wire_bytes: AtomicU64,
    /// UnreadRoomsChannel notices received (`{"roomId":..}`), across all clients.
    unread_frames: AtomicU64,
    /// PresenceChannel#refresh commands sent (`--refresh-secs`).
    refreshes: AtomicU64,
}

fn markers(text: &str) -> Vec<u64> {
    let mut out = Vec::new();
    let mut rest = text;
    while let Some(i) = rest.find("bmk") {
        rest = &rest[i + 3..];
        let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        if !digits.is_empty()
            && rest[digits.len()..].starts_with('z')
            && let Ok(n) = digits.parse()
        {
            out.push(n);
        }
    }
    out
}

#[allow(clippy::too_many_arguments)]
async fn cable_client(
    addr: String,
    source: Option<std::net::IpAddr>,
    cookie: String,
    subs: Vec<String>,
    confirmed: Arc<AtomicUsize>,
    connected: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    delivery: Arc<Delivery>,
    refresh: Option<(Duration, f64)>,
) -> Res<()> {
    let mut req = format!("ws://{addr}/cable").into_client_request()?;
    let h = req.headers_mut();
    h.insert("cookie", cookie.parse()?);
    h.insert("origin", origin_for(&addr).parse()?);
    h.insert("sec-websocket-protocol", "actioncable-v1-json, actioncable-unsupported".parse()?);
    if let Some(user_agent) = user_agent() {
        h.insert("user-agent", user_agent.parse()?);
    }
    let debug = std::env::var_os("LOADGEN_DEBUG").is_some();
    if debug {
        eprintln!("connecting {:?}", req.headers());
    }
    // One source address has ~28k ephemeral ports towards one server port; many clients connect
    // from several loopback addresses (`--sources`).
    let socket = tokio::net::TcpSocket::new_v4()?;
    if let Some(source) = source {
        socket.bind(std::net::SocketAddr::new(source, 0))?;
    }
    let stream = socket.connect(tokio::net::lookup_host(&addr).await?.next().ok_or("no address")?).await?;
    stream.set_nodelay(true)?;
    let (ws, _) = tokio_tungstenite::client_async(req, stream).await.inspect_err(|e| {
        if debug {
            eprintln!("connect error: {e}")
        }
    })?;
    if debug {
        eprintln!("connected");
    }
    connected.fetch_add(1, Ordering::Relaxed);
    let (mut tx, mut rx) = ws.split();
    for ident in &subs {
        tx.send(WsMessage::text(json!({"command": "subscribe", "identifier": ident}).to_string())).await?;
    }
    let mut seen = HashSet::new();
    let mut confirms = 0;
    let presence = subs
        .iter()
        .find(|identifier| serde_json::from_str::<Value>(identifier).ok().is_some_and(|value| value["channel"] == "PresenceChannel"))
        .cloned();
    let mut refresh_timer: Option<tokio::time::Interval> = None;
    loop {
        let msg = match refresh_timer.as_mut() {
            Some(timer) => tokio::select! {
                m = rx.next() => m,
                _ = timer.tick() => {
                    if stop.load(Ordering::Relaxed) { break; }
                    let data = json!({"action": "refresh"}).to_string();
                    let command = json!({"command": "message", "identifier": presence.as_deref().unwrap_or_default(), "data": data});
                    tx.send(WsMessage::text(command.to_string())).await?;
                    delivery.refreshes.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            },
            None => rx.next().await,
        };
        let Some(msg) = msg else { break };
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let text = match msg? {
            WsMessage::Text(t) => t,
            WsMessage::Close(_) => break,
            _ => continue,
        };
        on_text(&text, subs.len(), &mut confirms, &mut seen, &confirmed, &delivery);
        if refresh_timer.is_none()
            && let (Some((period, phase)), Some(identifier)) = (refresh, &presence)
            && serde_json::from_str::<Value>(&text)
                .ok()
                .is_some_and(|value| value["type"] == "confirm_subscription" && value["identifier"].as_str() == Some(identifier.as_str()))
        {
            refresh_timer = Some(presence_timer(period, phase));
        }
    }
    let _ = tx.send(WsMessage::Close(None)).await;
    Ok(())
}

fn refresh_period(raw: Option<&str>) -> Res<Option<Duration>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let seconds: f64 = raw.parse().map_err(|_| "--refresh-secs must be a finite nonnegative number")?;
    if !seconds.is_finite() || seconds < 0.0 {
        return Err("--refresh-secs must be a finite nonnegative number".into());
    }
    if seconds == 0.0 {
        return Ok(None);
    }
    let period = Duration::try_from_secs_f64(seconds).map_err(|_| "--refresh-secs is outside the supported duration range")?;
    if period.is_zero() {
        return Err("--refresh-secs must be at least one nanosecond".into());
    }
    if Instant::now().checked_add(period).is_none() {
        return Err("--refresh-secs is outside the supported timer range".into());
    }
    Ok(Some(period))
}

fn presence_timer(period: Duration, phase: f64) -> tokio::time::Interval {
    let mut timer = tokio::time::interval_at(tokio::time::Instant::now() + period.mul_f64(1.0 - phase), period);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    timer
}

/// A frame's text: counts subscription confirmations and records each marked message's arrival.
fn on_text(text: &str, subscriptions: usize, confirms: &mut usize, seen: &mut HashSet<u64>, confirmed: &AtomicUsize, delivery: &Delivery) {
    let now = Instant::now();
    if std::env::var_os("LOADGEN_DEBUG").is_some() {
        eprintln!("<< {}", &text[..text.len().min(300)]);
    }
    if text.contains("confirm_subscription") {
        *confirms += 1;
        if *confirms == subscriptions {
            confirmed.fetch_add(1, Ordering::Relaxed);
        }
        return;
    }
    if is_unread_notice(text) {
        delivery.unread_frames.fetch_add(1, Ordering::Relaxed);
        return;
    }
    for seq in markers(text) {
        if !seen.insert(seq) {
            continue;
        }
        delivery.receipts.fetch_add(1, Ordering::Relaxed);
        let sent = delivery.sent.lock().unwrap().get(&seq).copied();
        if let Some(t0) = sent {
            delivery.per_client.lock().unwrap().record(now.duration_since(t0).as_micros() as u64).ok();
        }
        let mut got = delivery.got.lock().unwrap();
        let e = got.entry(seq).or_insert((0, now));
        e.0 += 1;
        e.1 = now;
    }
}

/// The Origin a browser sends to `addr`: without the default port, which Rails also leaves out of the base URL it
/// compares Origin with (`Origin: http://host:80` is refused).
fn origin_for(addr: &str) -> String {
    format!("http://{}", addr.strip_suffix(":80").unwrap_or(addr))
}

/// An UnreadRoomsChannel notice (`{"roomId":..,"at":..}`, in any key order), which carries no marker. A streamed
/// page fragment is a JSON string, where neither an object opening nor an unescaped `"roomId":` can appear.
fn is_unread_notice(text: &str) -> bool {
    text.contains(r#""message":{"#) && text.contains(r#""roomId":"#)
}

/// `--sessions` lines: `cookie<TAB>identifier<TAB>identifier...`; blank lines are skipped.
fn parse_sessions(text: &str) -> Vec<(String, Vec<String>)> {
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            let mut parts = l.split('\t');
            (parts.next().unwrap_or_default().to_string(), parts.map(str::to_string).collect())
        })
        .collect()
}

/// Client n's cookie and subscriptions: line n % lines of `--sessions`, or the `--cookie` user's when there are none.
fn session_for(sessions: &[(String, Vec<String>)], n: usize, cookie: &str, subs: &[String]) -> (String, Vec<String>) {
    match sessions.get(n % sessions.len().max(1)) {
        Some((c, s)) => (c.clone(), s.clone()),
        None => (cookie.to_string(), subs.to_vec()),
    }
}

// Report only the rows and cookie strings actually assigned to clients; neither establishes user identity.
fn session_profile(sessions: &[(String, Vec<String>)], clients: usize, cookie: &str, subs: &[String]) -> Value {
    let rows = if clients == 0 { 0 } else { sessions.len().max(1).min(clients) };
    let mut cookies = HashSet::new();
    let mut min = usize::MAX;
    let mut max = 0;
    for n in 0..rows {
        let (cookie, subs) = session_for(sessions, n, cookie, subs);
        cookies.insert(cookie);
        min = min.min(subs.len());
        max = max.max(subs.len());
    }
    if rows == 0 {
        min = 0;
    }
    json!({
        "session_rows": rows,
        "distinct_session_cookies": cookies.len(),
        "subscriptions_per_client": (min == max).then_some(min),
        "subscriptions_per_client_range": {"min": min, "max": max},
    })
}

/// `--deflate`: a minimal WebSocket client (RFC 6455) that offers `permessage-deflate` as browsers
/// do, since tungstenite has no compression, and counts the bytes read off the socket.
#[allow(clippy::too_many_arguments)]
async fn deflate_cable_client(
    addr: String,
    source: Option<std::net::IpAddr>,
    cookie: String,
    subs: Vec<String>,
    confirmed: Arc<AtomicUsize>,
    connected: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    delivery: Arc<Delivery>,
) -> Res<()> {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
    let socket = tokio::net::TcpSocket::new_v4()?;
    if let Some(source) = source {
        socket.bind(std::net::SocketAddr::new(source, 0))?;
    }
    let stream = socket.connect(tokio::net::lookup_host(&addr).await?.next().ok_or("no address")?).await?;
    stream.set_nodelay(true)?;
    let (read, mut write) = stream.into_split();
    let request = format!(
        "GET /cable HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
         Sec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: actioncable-v1-json, actioncable-unsupported\r\n\
         Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits\r\nOrigin: {origin}\r\nCookie: {cookie}\r\n\r\n",
        origin = origin_for(&addr)
    );
    write.write_all(request.as_bytes()).await?;
    let mut reader = tokio::io::BufReader::with_capacity(16 * 1024, read);
    let mut status = String::new();
    reader.read_line(&mut status).await?;
    if !status.starts_with("HTTP/1.1 101") {
        return Err(format!("upgrade failed: {}", status.trim()).into());
    }
    let mut compressed = false;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).await?;
        if line.trim().is_empty() {
            break;
        }
        compressed |= line.to_ascii_lowercase().starts_with("sec-websocket-extensions:") && line.contains("permessage-deflate");
    }
    connected.fetch_add(1, Ordering::Relaxed);
    for ident in &subs {
        let payload = json!({"command": "subscribe", "identifier": ident}).to_string();
        write.write_all(&masked_text_frame(payload.as_bytes())).await?;
    }
    let mut seen = HashSet::new();
    let mut confirms = 0;
    let mut inflater = flate2::Decompress::new(false);
    let mut payload = Vec::new();
    let mut text = Vec::new();
    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let mut head = [0u8; 2];
        if reader.read_exact(&mut head).await.is_err() {
            break;
        }
        let len = match head[1] & 0x7f {
            126 => reader.read_u16().await? as usize,
            127 => reader.read_u64().await? as usize,
            len => len as usize,
        };
        payload.resize(len, 0);
        reader.read_exact(&mut payload).await?;
        let header_len = 2 + if len >= 65536 {
            8
        } else if len >= 126 {
            2
        } else {
            0
        };
        delivery.wire_bytes.fetch_add((header_len + len) as u64, Ordering::Relaxed);
        match head[0] & 0x0f {
            0x8 => break,
            0x1 => {}
            _ => continue,
        }
        let message = if head[0] & 0x40 != 0 && compressed {
            inflater.reset(false);
            payload.extend_from_slice(&[0, 0, 0xff, 0xff]);
            text.clear();
            text.reserve(len * 8 + 1024);
            loop {
                let consumed = inflater.total_in() as usize;
                inflater.decompress_vec(&payload[consumed..], &mut text, flate2::FlushDecompress::Sync)?;
                if inflater.total_in() as usize == payload.len() && text.len() < text.capacity() {
                    break;
                }
                text.reserve(text.capacity());
            }
            std::str::from_utf8(&text)?
        } else {
            std::str::from_utf8(&payload)?
        };
        on_text(message, subs.len(), &mut confirms, &mut seen, &confirmed, &delivery);
    }
    Ok(())
}

/// A client's text frame: masked, as RFC 6455 requires of clients.
fn masked_text_frame(payload: &[u8]) -> Vec<u8> {
    let mask = [0x37, 0xfa, 0x21, 0x3d];
    let mut frame = vec![0x81];
    match payload.len() {
        len if len < 126 => frame.push(0x80 | len as u8),
        len => {
            frame.push(0x80 | 126);
            frame.extend_from_slice(&(len as u16).to_be_bytes());
        }
    }
    frame.extend_from_slice(&mask);
    frame.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i % 4]));
    frame
}

async fn post_marked(
    sender: &mut Option<SendRequest<Full<Bytes>>>,
    addr: &str,
    room: &str,
    cookie: &str,
    csrf: &str,
    seq: u64,
    delivery: &Delivery,
) -> Option<u64> {
    if sender.is_none() {
        *sender = connect(addr).await.ok();
    }
    let (h, b) = message_request(cookie, csrf, &format!("fanout bmk{seq}z"));
    let t0 = Instant::now();
    delivery.sent.lock().unwrap().insert(seq, t0);
    match send(sender.as_mut()?, addr, "POST", &format!("/rooms/{room}/messages"), &h, b).await {
        Ok(r) if valid_message_post(r.status, &r.body, &format!("fanout bmk{seq}z")) => Some(t0.elapsed().as_micros() as u64),
        _ => {
            *sender = None;
            None
        }
    }
}

fn valid_message_post(status: u16, body: &[u8], expected: &str) -> bool {
    let Ok(text) = std::str::from_utf8(body) else { return false };
    static IDS: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let ids = IDS.get_or_init(|| regex::Regex::new(r#"data-message-id="([0-9]+)""#).unwrap());
    let mut messages = ids.captures_iter(text);
    status == 200
        && text.starts_with("<turbo-stream")
        && text.trim_end().ends_with("</turbo-stream>")
        && text.contains("<template>")
        && text.contains("</template>")
        && text.contains(expected)
        && messages.next().is_some_and(|id| id[1].parse::<u64>().is_ok_and(|id| id > 0))
        && messages.next().is_none()
}

async fn wait_drain(delivery: &Delivery, seqs: &[u64], clients: usize, timeout: Duration) {
    let until = Instant::now() + timeout;
    while Instant::now() < until {
        let drained = {
            let got = delivery.got.lock().unwrap();
            seqs.iter().all(|s| got.get(s).map(|g| g.0 >= clients).unwrap_or(false))
        };
        if drained {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn phase(name: &str) {
    let ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis();
    eprintln!("PHASE {name} {ms}");
}

fn fanout_stats(delivery: &Delivery, seqs: &[u64], clients: usize) -> (Histogram<u64>, usize, Option<Instant>) {
    let sent = delivery.sent.lock().unwrap();
    let got = delivery.got.lock().unwrap();
    let mut all = hist();
    let mut complete = 0;
    let mut last = None::<Instant>;
    for s in seqs {
        if let Some((n, t)) = got.get(s) {
            last = Some(last.map_or(*t, |l| l.max(*t)));
            if *n >= clients {
                complete += 1;
                all.record(t.duration_since(sent[s]).as_micros() as u64).ok();
            }
        }
    }
    (all, complete, last)
}

async fn cable(a: &Args) -> Res<Value> {
    let addr = host_port(&a.get("base"));
    let cookie = a.get("cookie");
    let room = a.get("room");
    let csrf = a.opt("csrf").unwrap_or_default();
    let clients: usize = a.num("clients", 100);
    let latency_msgs: u64 = a.num("latency-msgs", 30);
    let interval = Duration::from_millis(a.num("interval-ms", 200));
    let tput_secs: f64 = a.num("tput-secs", 15.0);
    let posters: usize = a.num("posters", 4);
    let hold_secs: u64 = a.num("hold-secs", 0);
    let deflate = a.num("deflate", 0u8) != 0;
    let sources: Vec<std::net::IpAddr> =
        a.opt("sources").unwrap_or_default().split(',').filter(|s| !s.is_empty()).map(|s| s.parse()).collect::<Result<_, _>>()?;

    // The chatter.js load shape: presence for the room, unread rooms, heartbeat, and the page's
    // turbo stream sources (rooms list, the room's messages, the user's rooms).
    let mut subs = vec![
        json!({"channel": "PresenceChannel", "room_id": room.parse::<u64>()?}).to_string(),
        json!({"channel": "UnreadRoomsChannel"}).to_string(),
        json!({"channel": "HeartbeatChannel"}).to_string(),
    ];
    for s in a.opt("streams").unwrap_or_default().split(',').filter(|s| !s.is_empty()) {
        let (channel, name) = s.split_once('|').unwrap_or(("Turbo::StreamsChannel", s));
        subs.push(json!({"channel": channel, "signed_stream_name": name}).to_string());
    }
    // Client n takes session row n % rows; use a fixture with verified distinct users for a people profile.
    let sessions = match a.opt("sessions") {
        Some(path) => parse_sessions(&std::fs::read_to_string(path)?),
        None => Vec::new(),
    };
    if a.opt("sessions").is_some() && sessions.is_empty() {
        return Err("--sessions has no sessions".into());
    }
    if let Some((_, first)) = sessions.first() {
        subs = first.clone();
    }
    // `--refresh-secs S`: send PresenceChannel#refresh every S seconds, as presence_controller.js does
    // every 50 while the page is visible. 0 (the default) sends none.
    let refresh_period = refresh_period(a.0.get("refresh-secs").map(String::as_str))?;
    if deflate && refresh_period.is_some() {
        return Err("--refresh-secs is not supported with --deflate".into());
    }

    let delivery = Arc::new(Delivery {
        sent: Mutex::new(HashMap::new()),
        got: Mutex::new(HashMap::new()),
        unread_frames: AtomicU64::new(0),
        refreshes: AtomicU64::new(0),
        per_client: Mutex::new(hist()),
        receipts: AtomicU64::new(0),
        wire_bytes: AtomicU64::new(0),
    });
    let confirmed = Arc::new(AtomicUsize::new(0));
    let connected = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let failed = Arc::new(AtomicUsize::new(0));

    // Connect with bounded parallelism (50 handshakes in flight).
    phase("connect");
    let connect_start = Instant::now();
    let gate = Arc::new(tokio::sync::Semaphore::new(50));
    let mut handles = Vec::new();
    for n in 0..clients {
        let permit = gate.clone().acquire_owned().await?;
        let source = (!sources.is_empty()).then(|| sources[n % sources.len()]);
        let (client_cookie, client_subs) = session_for(&sessions, n, &cookie, &subs);
        let (addr, cookie, subs, confirmed, connected, stop, delivery, failed) = (
            addr.clone(),
            client_cookie,
            client_subs,
            confirmed.clone(),
            connected.clone(),
            stop.clone(),
            delivery.clone(),
            failed.clone(),
        );
        let before = connected.load(Ordering::Relaxed);
        handles.push(tokio::spawn(async move {
            let c2 = connected.clone();
            let task = if deflate {
                tokio::spawn(deflate_cable_client(addr, source, cookie, subs, confirmed, connected, stop, delivery))
            } else {
                // Browsers aren't in step: each client's refresh timer starts at its own point of the period.
                let refresh = refresh_period.map(|period| (period, ((n as f64) * 0.618_034).fract()));
                tokio::spawn(cable_client(addr, source, cookie, subs, confirmed, connected, stop, delivery, refresh))
            };
            // Release the permit once this client has connected (or failed).
            let until = Instant::now() + Duration::from_secs(30);
            while c2.load(Ordering::Relaxed) <= before && !task.is_finished() && Instant::now() < until {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            drop(permit);
            if let Ok(Err(_)) = task.await {
                failed.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    let until = Instant::now() + Duration::from_secs(120.max(clients as u64 / 200));
    while confirmed.load(Ordering::Relaxed) + failed.load(Ordering::Relaxed) < clients && Instant::now() < until {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let connect_secs = connect_start.elapsed().as_secs_f64();
    let ready = confirmed.load(Ordering::Relaxed);
    phase("connected");
    tokio::time::sleep(Duration::from_secs(1)).await;
    if hold_secs > 0 {
        // Everyone connected and idle: what the server spends on heartbeats and holding sockets.
        phase("idle");
        tokio::time::sleep(Duration::from_secs(hold_secs)).await;
        phase("idle_done");
    }
    phase("paced");

    // Phase 1: paced messages, one at a time (open loop at `interval`), for delivery latency.
    let mut poster = None;
    let mut post_h = hist();
    let mut post_errors = 0u64;
    let mut seqs = Vec::new();
    let mut seq = 0u64;
    for _ in 0..latency_msgs {
        seq += 1;
        let tick = Instant::now();
        if let Some(us) = post_marked(&mut poster, &addr, &room, &cookie, &csrf, seq, &delivery).await {
            post_h.record(us).ok();
        } else {
            post_errors += 1;
        }
        seqs.push(seq);
        let spent = tick.elapsed();
        if spent < interval {
            tokio::time::sleep(interval - spent).await;
        }
    }
    wait_drain(&delivery, &seqs, ready, Duration::from_secs(20)).await;
    phase("paced_drained");
    let (all_h, complete, _) = fanout_stats(&delivery, &seqs, ready);
    let client_h = std::mem::replace(&mut *delivery.per_client.lock().unwrap(), hist());
    let latency = json!({
        "messages": latency_msgs,
        "post_attempts": latency_msgs,
        "post_successes": post_h.len(),
        "post_errors": post_errors,
        "complete": complete,
        "post": summary(&post_h),
        "per_client": summary(&client_h),
        "all_clients": summary(&all_h),
    });

    // Phase 2: closed-loop posters for tput_secs, then drain; delivered messages/sec.
    let receipts_before = delivery.receipts.load(Ordering::Relaxed);
    let wire_before = delivery.wire_bytes.load(Ordering::Relaxed);
    let next = Arc::new(AtomicU64::new(seq + 1));
    phase("saturated");
    let tput_start = Instant::now();
    let tput_deadline = tput_start + Duration::from_secs_f64(tput_secs);
    let mut ptasks = Vec::new();
    for _ in 0..posters {
        let (addr, room, cookie, csrf, delivery, next) =
            (addr.clone(), room.clone(), cookie.clone(), csrf.clone(), delivery.clone(), next.clone());
        ptasks.push(tokio::spawn(async move {
            let mut conn = None;
            let mut mine = Vec::new();
            let mut h = hist();
            let mut attempts = 0u64;
            let mut errors = 0u64;
            while Instant::now() < tput_deadline {
                let s = next.fetch_add(1, Ordering::Relaxed);
                attempts += 1;
                if let Some(us) = post_marked(&mut conn, &addr, &room, &cookie, &csrf, s, &delivery).await {
                    h.record(us).ok();
                    mine.push(s);
                } else {
                    errors += 1;
                }
            }
            (mine, h, attempts, errors)
        }));
    }
    let mut tseqs = Vec::new();
    let mut tpost_h = hist();
    let mut post_attempts = 0u64;
    let mut post_errors = 0u64;
    for t in ptasks {
        let (m, h, attempts, errors) = t.await?;
        tseqs.extend(m);
        tpost_h.add(&h).ok();
        post_attempts += attempts;
        post_errors += errors;
    }
    let posting_secs = tput_start.elapsed().as_secs_f64();
    phase("saturated_posted");
    wait_drain(&delivery, &tseqs, ready, Duration::from_secs(60)).await;
    phase("saturated_drained");
    let (tall_h, tcomplete, last) = fanout_stats(&delivery, &tseqs, ready);
    let span = last.map(|l| l.duration_since(tput_start).as_secs_f64()).unwrap_or(posting_secs).max(posting_secs);
    let tclient_h = delivery.per_client.lock().unwrap().clone();
    let receipts = delivery.receipts.load(Ordering::Relaxed) - receipts_before;
    let wire_bytes = delivery.wire_bytes.load(Ordering::Relaxed) - wire_before;
    let throughput = json!({
        "posters": posters,
        "post_attempts": post_attempts,
        "post_errors": post_errors,
        "posted": tseqs.len(),
        "posts_per_sec": ((tseqs.len() as f64 / posting_secs) * 10.0).round() / 10.0,
        "complete": tcomplete,
        "delivered_msgs_per_sec": ((tcomplete as f64 / span) * 10.0).round() / 10.0,
        "frames_per_sec": (receipts as f64 / span).round(),
        "wire_mb_per_sec": deflate.then(|| (wire_bytes as f64 / span / 1e6 * 10.0).round() / 10.0),
        "drain_secs": ((span - posting_secs) * 100.0).round() / 100.0,
        "post": summary(&tpost_h),
        "per_client": summary(&tclient_h),
        "all_clients": summary(&tall_h),
    });

    let hold: f64 = a.num("hold-secs", 0.0);
    if hold > 0.0 {
        tokio::time::sleep(Duration::from_secs_f64(hold)).await;
    }
    phase("done");
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.abort();
    }
    let profile = session_profile(&sessions, clients, &cookie, &subs);
    Ok(json!({
        "clients": clients,
        "ready": ready,
        "failed": failed.load(Ordering::Relaxed),
        "connect_secs": (connect_secs * 100.0).round() / 100.0,
        "subscriptions_per_client": profile["subscriptions_per_client"],
        "subscriptions_per_client_range": profile["subscriptions_per_client_range"],
        "session_rows": profile["session_rows"],
        "distinct_session_cookies": profile["distinct_session_cookies"],
        "unread_frames": delivery.unread_frames.load(Ordering::Relaxed),
        "refreshes": delivery.refreshes.load(Ordering::Relaxed),
        "latency": latency,
        "throughput": throughput,
    }))
}

// ---------------------------------------------------------------------------------------------
// Upload + thumbnail

async fn upload(a: &Args) -> Res<Value> {
    let addr = host_port(&a.get("base"));
    let cookie = a.get("cookie");
    let room = a.get("room");
    let csrf = a.opt("csrf").unwrap_or_default();
    let file = a.get("file");
    let reps: usize = a.num("reps", 5);
    let data = std::fs::read(&file)?;
    let name = std::path::Path::new(&file).file_name().unwrap().to_string_lossy().to_string();
    let ctype = if name.ends_with(".png") { "image/png" } else { "image/jpeg" };
    let img_re = regex::Regex::new(r#"<img[^>]+src="([^"]+)""#).unwrap();

    let mut runs = Vec::new();
    for _ in 0..reps {
        let boundary = format!("----bench{}", nonce());
        let mut body = Vec::new();
        for (k, v) in [("authenticity_token", csrf.as_str()), ("message[client_message_id]", &nonce())]
            .into_iter()
            .filter(|(k, v)| *k != "authenticity_token" || !v.is_empty())
        {
            body.extend(format!("--{boundary}\r\nContent-Disposition: form-data; name=\"{k}\"\r\n\r\n{v}\r\n").as_bytes());
        }
        body.extend(
            format!("--{boundary}\r\nContent-Disposition: form-data; name=\"message[attachment]\"; filename=\"{name}\"\r\nContent-Type: {ctype}\r\n\r\n")
                .as_bytes(),
        );
        body.extend(&data);
        body.extend(format!("\r\n--{boundary}--\r\n").as_bytes());
        let t0 = Instant::now();
        let mut headers = vec![
            ("cookie", cookie.clone()),
            ("content-type", format!("multipart/form-data; boundary={boundary}")),
            ("accept", "text/vnd.turbo-stream.html, text/html".into()),
            same_origin(),
        ];
        if !csrf.is_empty() {
            headers.push(("x-csrf-token", csrf.clone()));
        }
        let r = one_shot(&addr, "POST", &format!("/rooms/{room}/messages"), &headers, Bytes::from(body)).await?;
        let post_ms = t0.elapsed().as_secs_f64() * 1000.0;
        let html = String::from_utf8_lossy(&r.body).to_string();
        let Some(src) = img_re.captures(&html).map(|c| unescape(&c[1])) else {
            runs.push(json!({"post_status": r.status, "post_ms": post_ms, "error": "no <img> in response"}));
            continue;
        };
        // Follow redirects to the bytes (representations/redirect -> disk service).
        let mut url = src.clone();
        let mut status = 0;
        let mut size = 0;
        for _ in 0..5 {
            let path = url.trim_start_matches(&format!("http://{addr}")).to_string();
            let g = one_shot(&addr, "GET", &path, &[("cookie", cookie.clone())], Bytes::new()).await?;
            status = g.status;
            size = g.body.len();
            match g.headers.get("location") {
                Some(loc) if (300..400).contains(&g.status) => url = loc.to_str()?.to_string(),
                _ => break,
            }
        }
        let total_ms = t0.elapsed().as_secs_f64() * 1000.0;
        runs.push(json!({
            "post_status": r.status, "post_ms": (post_ms * 10.0).round() / 10.0,
            "thumb_status": status, "thumb_bytes": size, "thumb_ms": ((total_ms - post_ms) * 10.0).round() / 10.0,
            "total_ms": (total_ms * 10.0).round() / 10.0,
        }));
    }
    let mut totals: Vec<f64> = runs.iter().filter_map(|r| r["total_ms"].as_f64()).collect();
    totals.sort_by(|a, b| a.partial_cmp(b).unwrap());
    Ok(json!({
        "file": name, "bytes": data.len(),
        "median_total_ms": totals.get(totals.len() / 2),
        "runs": runs,
    }))
}

// ---------------------------------------------------------------------------------------------
// Compression cost

async fn fetch(a: &Args) -> Res<Value> {
    let addr = host_port(&a.get("base"));
    let path = a.get("path");
    let r = one_shot(&addr, "GET", &path, &[("cookie", a.opt("cookie").unwrap_or_default())], Bytes::new()).await?;
    std::fs::write(a.get("out"), &r.body)?;
    Ok(
        json!({"status": r.status, "bytes": r.body.len(), "content_encoding": r.headers.get("content-encoding").map(|v| v.to_str().unwrap_or("").to_string())}),
    )
}

/// CPU per compression of one body. zlib-rs is the app's backend; the app's `Rack::Deflater` port
/// writes the body as one chunk with a sync flush and then finishes, at `Compression::default()`
/// (6). `miniz_oxide` is flate2's default `rust_backend`, which the app used before.
fn gzip_cost(a: &Args) -> Res<Value> {
    use std::io::Write;
    let data = std::fs::read(a.get("file"))?;
    let iters: u32 = a.num("iters", 200);
    let time = |f: &dyn Fn() -> usize| {
        let mut out = 0;
        for _ in 0..3 {
            out = f();
        }
        let mut samples: Vec<f64> = (0..iters)
            .map(|_| {
                let t0 = Instant::now();
                out = f();
                t0.elapsed().as_secs_f64() * 1e6
            })
            .collect();
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        json!({"median_us": samples[samples.len() / 2].round(), "min_us": samples[0].round(), "out_bytes": out})
    };
    let mut results = serde_json::Map::new();
    for level in [1u8, 4, 6, 9] {
        results.insert(format!("miniz_oxide_l{level}"), time(&|| miniz_oxide::deflate::compress_to_vec(&data, level).len()));
        results.insert(
            format!("zlib_rs_l{level}"),
            time(&|| {
                // The app's exact call sequence: one write, sync flush, finish.
                let mut e = flate2::GzBuilder::new().mtime(0).operating_system(3).write(Vec::new(), flate2::Compression::new(level as u32));
                e.write_all(&data).unwrap();
                e.flush().unwrap();
                e.finish().unwrap().len()
            }),
        );
    }
    Ok(json!({"file": a.get("file"), "bytes": data.len(), "iters": iters, "results": results}))
}

#[tokio::main]
async fn main() {
    let raw: Vec<String> = std::env::args().collect();
    let cmd = raw.get(1).cloned().unwrap_or_default();
    let a = Args::parse(&raw[2.min(raw.len())..]);
    USER_AGENT.set(a.opt("user-agent")).expect("set once");
    let out = match cmd.as_str() {
        "login" => login(&a).await,
        "scrape" => scrape(&a).await,
        "http" => http_load(&a).await,
        "cable" => cable(&a).await,
        "upload" => upload(&a).await,
        "fetch" => fetch(&a).await,
        "gzip" => gzip_cost(&a),
        "users" => users::users(&a).await,
        _ => Err("usage: loadgen login|scrape|http|cable|upload|fetch|gzip|users --base URL ...".into()),
    };
    match out {
        Ok(v) => println!("{v}"),
        Err(e) => {
            eprintln!("loadgen {cmd}: {e}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod validation_integration_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn browser_posts_need_no_csrf_fields_and_legacy_tokens_remain_optional() {
        let (headers, body) = message_request("session_token=signed", "", "hello");
        assert!(headers.iter().any(|(name, value)| *name == "sec-fetch-site" && value == "same-origin"));
        assert!(!headers.iter().any(|(name, _)| *name == "x-csrf-token"));
        assert!(!String::from_utf8_lossy(&body).contains("authenticity_token"));
        let (headers, body) = message_request("session_token=signed", "legacy", "hello");
        assert!(headers.iter().any(|(name, value)| *name == "x-csrf-token" && value == "legacy"));
        assert!(String::from_utf8_lossy(&body).contains("authenticity_token=legacy"));
    }

    #[tokio::test]
    async fn mixed_writer_is_paced_validated_and_audited_separately() {
        let directory = std::env::temp_dir().join(format!("campfire-mixed-{}", nonce()));
        std::fs::create_dir(&directory).unwrap();
        let read = directory.join("read.json");
        let write = directory.join("write.json");
        let audit = directory.join("audit.jsonl");
        std::fs::write(&read, json!({"kind":"up","content_type":"text/plain","required":["OK"]}).to_string()).unwrap();
        std::fs::write(&write, json!({"kind":"post_message","content_type":"text/vnd.turbo-stream.html","required":[]}).to_string())
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let arrivals = Arc::new(Mutex::new(Vec::new()));
        let seen = arrivals.clone();
        let server = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    socket.read_exact(&mut byte).await.unwrap();
                    request.push(byte[0]);
                }
                let head = String::from_utf8(request).unwrap();
                let (body, content_type) = if head.starts_with("POST ") {
                    let length = head
                        .lines()
                        .find_map(|line| line.to_ascii_lowercase().strip_prefix("content-length: ").and_then(|v| v.parse::<usize>().ok()))
                        .unwrap();
                    let mut payload = vec![0; length];
                    socket.read_exact(&mut payload).await.unwrap();
                    let token =
                        String::from_utf8(payload).unwrap().split('&').next().unwrap().split_once('=').unwrap().1.replace("%20", " ");
                    let id = {
                        let mut times = seen.lock().unwrap();
                        times.push(Instant::now());
                        times.len()
                    };
                    let rendered = if id == 2 { "wrong body" } else { &token };
                    (
                        format!(
                            "<turbo-stream action=\"append\" target=\"messages_room_2\"><template><div data-message-id=\"{id}\">{rendered}</div></template></turbo-stream>"
                        ),
                        "text/vnd.turbo-stream.html",
                    )
                } else {
                    ("OK".into(), "text/plain")
                };
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
            }
        });
        let args = Args(HashMap::from([
            ("base".into(), format!("http://{addr}")),
            ("path".into(), "/up".into()),
            ("validate".into(), read.to_string_lossy().into_owned()),
            ("conc".into(), "1".into()),
            ("duration".into(), "0.25".into()),
            ("requests".into(), "4".into()),
            ("gzip".into(), "0".into()),
            ("mixed-write-rate".into(), "10".into()),
            ("mixed-write-room".into(), "2".into()),
            ("mixed-write-validate".into(), write.to_string_lossy().into_owned()),
            ("mixed-write-audit".into(), audit.to_string_lossy().into_owned()),
        ]));
        let result = http_load(&args).await.unwrap();
        server.abort();
        assert_eq!(result["ok"], 4);
        assert_eq!(result["invalid_responses"], 0);
        assert_eq!(result["writer"]["ok"], 2);
        assert_eq!(result["writer"]["invalid_responses"], 1);
        let lines = std::fs::read_to_string(audit).unwrap();
        assert_eq!(lines.lines().count(), 2);
        for line in lines.lines() {
            let row: Value = serde_json::from_str(line).unwrap();
            assert!(row["token"].as_str().unwrap().starts_with("bench write "));
            assert_ne!(row["id"], 2);
        }
        let times = arrivals.lock().unwrap();
        assert_eq!(times.len(), 3);
        assert!(times.windows(2).all(|pair| pair[1].duration_since(pair[0]) >= Duration::from_millis(95)));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn mixed_writer_rejects_missing_contracts_and_unbounded_rates() {
        for rate in ["0", "101", "bad", "10"] {
            let args = Args(HashMap::from([("mixed-write-rate".into(), rate.into()), ("validate".into(), "unused.json".into())]));
            assert!(http_load(&args).await.is_err());
        }
    }

    #[tokio::test]
    async fn intermittent_200_error_page_invalidates_a_loaded_run() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for i in 0..8 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    socket.read_exact(&mut byte).await.unwrap();
                    request.push(byte[0]);
                }
                let body = if i == 3 {
                    "<!DOCTYPE html><html>Internal server error</html>"
                } else {
                    "<!DOCTYPE html><html><div id=\"message_42\" data-message-id=\"42\"><div id=\"presentation_message_42\">coffee</div></div></html>"
                };
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
            }
        });
        let path = std::env::temp_dir().join(format!("campfire-validation-{}.json", nonce()));
        std::fs::write(
            &path,
            json!({"kind":"room_show","content_type":"text/html","required":[],"message_ids":[42],"message_content":[["coffee"]]})
                .to_string(),
        )
        .unwrap();
        let args = Args(HashMap::from([
            ("base".into(), format!("http://{addr}")),
            ("path".into(), "/rooms/1".into()),
            ("validate".into(), path.to_str().unwrap().into()),
            ("requests".into(), "8".into()),
            ("conc".into(), "1".into()),
            ("duration".into(), "5".into()),
        ]));
        let result = http_load(&args).await.unwrap();
        server.await.unwrap();
        std::fs::remove_file(path).unwrap();
        assert_eq!(result["errors"], 0);
        assert_eq!(result["statuses"]["200"], 8);
        assert_eq!(result["ok"], 7);
        assert_eq!(result["invalid_responses"], 1);
        assert_eq!(result["invalid_reasons"]["incorrect message window"], 1);
        assert_eq!(result["validation"], "route-contract-v1");
    }
}
#[cfg(test)]
mod cable_client_tests {
    use super::*;
    use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

    #[test]
    fn origin_leaves_out_only_the_default_port() {
        assert_eq!(origin_for("campfire:80"), "http://campfire");
        assert_eq!(origin_for("127.0.0.1:3000"), "http://127.0.0.1:3000");
        assert_eq!(origin_for("campfire:8080"), "http://campfire:8080");
        assert_eq!(origin_for("[::1]:80"), "http://[::1]");
    }

    #[test]
    fn sessions_preserve_each_rows_cookie_and_subscriptions() {
        let text = "session_token=a%3D--1\t{\"channel\":\"PresenceChannel\",\"room_id\":1}\t{\"channel\":\"HeartbeatChannel\"}\n\n\
                    session_token=b%3D--2\t{\"channel\":\"UnreadRoomsChannel\"}\n";
        let sessions = parse_sessions(text);
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0].0, "session_token=a%3D--1");
        assert_eq!(sessions[0].1, vec![r#"{"channel":"PresenceChannel","room_id":1}"#, r#"{"channel":"HeartbeatChannel"}"#]);
        assert_eq!(sessions[1].0, "session_token=b%3D--2");
        assert_eq!(sessions[1].1, vec![r#"{"channel":"UnreadRoomsChannel"}"#]);
        assert!(parse_sessions("\n \n").is_empty());
    }

    #[test]
    fn each_client_takes_its_own_session_in_turn() {
        let sessions = parse_sessions("a\tsa\nb\tsb1\tsb2\n");
        let tab = vec!["shared".to_string()];
        assert_eq!(session_for(&sessions, 0, "user", &tab), ("a".into(), vec!["sa".into()]));
        assert_eq!(session_for(&sessions, 1, "user", &tab), ("b".into(), vec!["sb1".into(), "sb2".into()]));
        assert_eq!(session_for(&sessions, 2, "user", &tab), ("a".into(), vec!["sa".into()]));
        assert_eq!(session_for(&[], 5, "user", &tab), ("user".into(), tab.clone()));
    }

    #[test]
    fn refresh_period_rejects_invalid_and_unrepresentable_values() {
        assert_eq!(refresh_period(None).unwrap(), None);
        assert_eq!(refresh_period(Some("0")).unwrap(), None);
        assert_eq!(refresh_period(Some("0.05")).unwrap(), Some(Duration::from_millis(50)));
        for value in ["NaN", "inf", "-1", "nope", "1e300", "1e-30"] {
            assert!(refresh_period(Some(value)).is_err(), "accepted {value}");
        }
    }

    #[test]
    fn session_profile_reports_used_rows_without_claiming_distinct_people() {
        let sessions = parse_sessions("same\ta\nsame\ta\tb\nunused\ta\n");
        let profile = session_profile(&sessions, 2, "fallback", &[]);
        assert_eq!(profile["session_rows"], 2);
        assert_eq!(profile["distinct_session_cookies"], 1);
        assert!(profile["subscriptions_per_client"].is_null());
        assert_eq!(profile["subscriptions_per_client_range"], json!({"min": 1, "max": 2}));
        let profile = session_profile(&[], 5, "fallback", &["a".into()]);
        assert_eq!(profile["session_rows"], 1);
        assert_eq!(profile["distinct_session_cookies"], 1);
        assert_eq!(profile["subscriptions_per_client"], 1);
        assert_eq!(session_profile(&sessions, 0, "fallback", &[])["session_rows"], 0);
    }

    #[tokio::test]
    async fn presence_timer_delays_first_refresh_and_skips_missed_ticks() {
        let mut timer = presence_timer(Duration::from_millis(50), 0.0);
        assert_eq!(timer.missed_tick_behavior(), tokio::time::MissedTickBehavior::Skip);
        assert!(tokio::time::timeout(Duration::from_millis(10), timer.tick()).await.is_err());
    }

    #[test]
    fn unread_notices_are_told_from_streamed_html() {
        assert!(is_unread_notice(r#"{"identifier":"{\"channel\":\"UnreadRoomsChannel\"}","message":{"roomId":1,"at":1.5}}"#));
        assert!(is_unread_notice(r#"{"identifier":"{\"channel\":\"UnreadRoomsChannel\"}","message":{"at":1.5,"roomId":1}}"#));
        assert!(!is_unread_notice(r#"{"identifier":"x","message":"<turbo-stream>{\"roomId\":1}</turbo-stream>"}"#));
        assert!(!is_unread_notice(r#"{"type":"ping","message":1}"#));
    }

    // One client against a local Action Cable stand-in: it must send the session's cookie and an Origin without
    // the default port, count one person's confirmations, an unread notice and a marked message, and send
    // PresenceChannel#refresh on its timer with the presence identifier.
    // The handshake callback's error type is tungstenite's, not ours to shrink.
    #[allow(clippy::result_large_err)]
    #[tokio::test]
    async fn a_client_is_its_session_and_refreshes_its_presence() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let presence = r#"{"channel":"PresenceChannel","room_id":1}"#.to_string();
        let unread = r#"{"channel":"UnreadRoomsChannel"}"#.to_string();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let headers = Arc::new(Mutex::new(Vec::new()));
            let seen = headers.clone();
            let ws = tokio_tungstenite::accept_hdr_async(stream, move |req: &Request, mut resp: Response| {
                for name in ["cookie", "origin"] {
                    seen.lock().unwrap().push(req.headers()[name].to_str().unwrap().to_string());
                }
                resp.headers_mut().insert("sec-websocket-protocol", "actioncable-v1-json".parse().unwrap());
                Ok(resp)
            })
            .await
            .unwrap();
            let (mut tx, mut rx) = ws.split();
            let mut subscribed = Vec::new();
            let mut refreshes = Vec::new();
            while let Some(Ok(WsMessage::Text(text))) = rx.next().await {
                let command: Value = serde_json::from_str(&text).unwrap();
                let identifier = command["identifier"].as_str().unwrap().to_string();
                match command["command"].as_str().unwrap() {
                    "subscribe" => {
                        subscribed.push(identifier.clone());
                        if subscribed.len() == 2 {
                            assert!(
                                tokio::time::timeout(Duration::from_millis(75), rx.next()).await.is_err(),
                                "refresh arrived before presence confirmation"
                            );
                            for identifier in &subscribed {
                                let confirm = json!({"identifier": identifier, "type": "confirm_subscription"});
                                tx.send(WsMessage::text(confirm.to_string())).await.unwrap();
                            }
                            let notice = json!({"identifier": subscribed[1], "message": {"roomId": 1, "at": 1.5}});
                            tx.send(WsMessage::text(notice.to_string())).await.unwrap();
                            let marked = json!({"identifier": subscribed[0], "message": "<turbo-stream>fanout bmk7z</turbo-stream>"});
                            tx.send(WsMessage::text(marked.to_string())).await.unwrap();
                        }
                    }
                    "message" => {
                        refreshes.push((identifier, command["data"].as_str().unwrap().to_string()));
                        if refreshes.len() == 2 {
                            break;
                        }
                    }
                    other => panic!("unexpected command {other}"),
                }
            }
            tx.send(WsMessage::Close(None)).await.ok();
            (headers.lock().unwrap().clone(), subscribed, refreshes)
        });

        let delivery = Arc::new(Delivery {
            sent: Mutex::new(HashMap::new()),
            got: Mutex::new(HashMap::new()),
            per_client: Mutex::new(hist()),
            receipts: AtomicU64::new(0),
            wire_bytes: AtomicU64::new(0),
            unread_frames: AtomicU64::new(0),
            refreshes: AtomicU64::new(0),
        });
        let (confirmed, connected) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let client = cable_client(
            addr.clone(),
            None,
            "session_token=person-7".into(),
            vec![presence.clone(), unread.clone()],
            confirmed.clone(),
            connected.clone(),
            Arc::new(AtomicBool::new(false)),
            delivery.clone(),
            Some((Duration::from_millis(50), 0.0)),
        );
        tokio::time::timeout(Duration::from_secs(10), client).await.unwrap().unwrap();
        let (headers, subscribed, refreshes) = server.await.unwrap();

        assert_eq!(headers, vec!["session_token=person-7".to_string(), format!("http://{addr}")]);
        assert_eq!(subscribed, vec![presence.clone(), unread]);
        assert_eq!(confirmed.load(Ordering::Relaxed), 1);
        assert_eq!(delivery.unread_frames.load(Ordering::Relaxed), 1);
        assert_eq!(delivery.receipts.load(Ordering::Relaxed), 1);
        assert_eq!(refreshes, vec![(presence.clone(), r#"{"action":"refresh"}"#.to_string()); 2]);
        assert!(delivery.refreshes.load(Ordering::Relaxed) >= 2);
    }
}

#[cfg(test)]
mod tests {
    use super::valid_message_post;

    #[test]
    fn message_post_requires_a_complete_response_with_its_actual_message() {
        let valid = br#"<turbo-stream action="append" target="messages_room_1"><template><div data-message-id="42">fanout bmk1z</div></template></turbo-stream>"#;
        assert!(valid_message_post(200, valid, "fanout bmk1z"));
        for status in [201, 302, 500] {
            assert!(!valid_message_post(status, valid, "fanout bmk1z"));
        }
        assert!(!valid_message_post(200, valid, "fanout bmk2z"));
        assert!(!valid_message_post(200, &valid[..valid.len() - 1], "fanout bmk1z"));
        assert!(!valid_message_post(200, b"<turbo-stream></turbo-stream>", "fanout bmk1z"));
        assert!(!valid_message_post(200, b"an error page", "fanout bmk1z"));
        assert!(!valid_message_post(200, &[0xff], "fanout bmk1z"));
    }
}
