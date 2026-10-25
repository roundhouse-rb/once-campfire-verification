//! `loadgen users`: many distinct people, each with a browser's socket and a browser's requests.
//!
//!   loadgen users --base URL --plan FILE --people 500,1000,2000 [--think-secs 10 --post-secs 300]
//!   loadgen users --base URL --plan FILE --viewers 500 --rates 100,200,400 [--write-rate 5]
//!                 [--step-secs 20 --warmup-secs 10 --refresh-secs 50 --slo-ms 100 --delivery-slo-ms 500]
//!                 [--abort-ms 2000 --db FILE --audit FILE --gzip 1 --seed N]
//!
//! The plan (built from the fixture database, independently of the server) names each user's signed session
//! cookie and rooms, and every room's message ids. Load comes in steps. `--people` grows the population: each
//! step's new people arrive (an untimed, checked room page, sidebar and socket each), then every person navigates
//! on average every `--think-secs` and posts every `--post-secs`, so the result is how many people a server
//! serves within the latency targets. `--viewers --rates` instead offers a fixed population rising navigation
//! rates, a stress ramp at per-person rates nobody keeps up, which favours caches keyed per person.
//!
//! Navigations are one Poisson process across the population; posts are a second, into rooms chosen by
//! membership, by a member who is in the room. Latency is measured from when the generator dispatched the
//! action, so a server that falls behind (and a viewer's own queue behind it) shows in the tail; the
//! generator's own timer lag is reported separately as `generator_lag`.
//!
//! A navigation is one of:
//!   visit   GET /rooms/R; the page's subscriptions move to R (its PresenceChannel `present` writes the
//!           membership); once HeartbeatChannel confirms, GET /rooms/R/refresh?since=.. as refresh_room does
//!   scroll  GET /rooms/R/messages?before=<oldest message shown>
//!   search  GET /searches?q=<term>
//!   return  a tab hidden for over a minute becoming visible: the refresh, then the sidebar frame
//! Each socket also sends PresenceChannel#refresh every `--refresh-secs`, as a visible page does.
//!
//! Every response is checked. Pages must name the requesting user (`current-user-id`), list only messages of
//! the room (room page, scroll) or of the user's rooms (search), list only the user's rooms (sidebar), and a
//! room page must include the newest message acknowledged in that room before the request was sent. A posted
//! message must reach every viewer whose room stream was confirmed when it was sent and who stayed. Message ids
//! the plan does not know yet (posted concurrently) are checked at the end against the acknowledged writes and,
//! with `--db`, the database, which must also hold every acknowledged post in its room with its body.
//! Responses carrying `Server-Timing: page;desc="hit"|"miss"` or `fragments;desc="hits=N misses=M"` are tallied
//! per route. A step meets its targets when the p99 navigation is within `--slo-ms`, no navigation failed, and
//! the p99 delivery is within `--delivery-slo-ms`. The command prints one JSON object (`valid` false when any
//! check failed).

use super::*;
use flate2::read::GzDecoder;
use std::collections::{BTreeMap, VecDeque};
use std::io::Read;
use tokio::sync::{Notify, mpsc};

const PAGE: usize = 40;
const ROUTES: [&str; 7] = ["room", "refresh", "scroll", "search", "sidebar", "post", "subscribe"];

// ---------------------------------------------------------------------------------------------
// Plan

struct PlanUser {
    id: u64,
    cookie: String,
    rooms: Vec<u64>,
}

struct Plan {
    users: Vec<PlanUser>,
    /// Message ids per room, oldest first.
    rooms: HashMap<u64, Vec<u64>>,
    terms: Vec<String>,
}

fn load_plan(path: &str) -> Res<Plan> {
    let v: Value = serde_json::from_str(&std::fs::read_to_string(path)?)?;
    let users = v["users"]
        .as_array()
        .ok_or("plan: users")?
        .iter()
        .map(|u| PlanUser {
            id: u["id"].as_u64().unwrap_or_default(),
            cookie: u["cookie"].as_str().unwrap_or_default().to_string(),
            rooms: u["rooms"].as_array().map(|r| r.iter().filter_map(Value::as_u64).collect()).unwrap_or_default(),
        })
        .collect::<Vec<_>>();
    let rooms = v["rooms"]
        .as_object()
        .ok_or("plan: rooms")?
        .iter()
        .map(|(k, ids)| {
            (k.parse().unwrap_or_default(), ids.as_array().map(|a| a.iter().filter_map(Value::as_u64).collect()).unwrap_or_default())
        })
        .collect();
    let terms = v["terms"].as_array().map(|t| t.iter().filter_map(|s| s.as_str().map(str::to_string)).collect()).unwrap_or_default();
    if users.iter().any(|u| u.id == 0 || u.cookie.is_empty() || u.rooms.is_empty()) {
        return Err("plan: every user needs an id, a cookie and at least one room".into());
    }
    Ok(Plan { users, rooms, terms })
}

// ---------------------------------------------------------------------------------------------
// A small deterministic generator: the run's choices are reproducible from `--seed`.

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn unit(&mut self) -> f64 {
        ((self.next() >> 11) as f64 + 0.5) / (1u64 << 53) as f64
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n.max(1) as u64) as usize
    }
    fn exp(&mut self, rate: f64) -> Duration {
        Duration::from_secs_f64(-self.unit().ln() / rate)
    }
    fn weighted(&mut self, weights: &[f64]) -> usize {
        let total: f64 = weights.iter().sum();
        let mut x = self.unit() * total;
        for (i, w) in weights.iter().enumerate() {
            if x < *w {
                return i;
            }
            x -= w;
        }
        weights.len() - 1
    }
}

// ---------------------------------------------------------------------------------------------
// Measurements

#[derive(Default)]
struct Route {
    lat: Option<Histogram<u64>>,
    svc: Option<Histogram<u64>>,
    ok: u64,
    errors: u64,
    invalid: u64,
    bytes: u64,
    page_hit: u64,
    page_miss: u64,
    frag_hits: u64,
    frag_misses: u64,
    timed: u64,
}

impl Route {
    fn record(&mut self, scheduled: Instant, sent: Instant, done: Instant) {
        self.lat.get_or_insert_with(hist).record(done.duration_since(scheduled).as_micros() as u64).ok();
        self.svc.get_or_insert_with(hist).record(done.duration_since(sent).as_micros() as u64).ok();
    }
    fn json(&self) -> Value {
        let mut v = json!({
            "ok": self.ok, "errors": self.errors, "invalid": self.invalid, "bytes": self.bytes,
            "latency": self.lat.as_ref().map(summary).unwrap_or(json!({"n": 0})),
            "service": self.svc.as_ref().map(summary).unwrap_or(json!({"n": 0})),
        });
        if self.page_hit + self.page_miss > 0 {
            v["page_cache"] = json!({"hit": self.page_hit, "miss": self.page_miss});
        }
        if self.frag_hits + self.frag_misses > 0 {
            v["fragments"] = json!({"hits": self.frag_hits, "misses": self.frag_misses});
        }
        v
    }
}

#[derive(Default)]
struct Step {
    offered: f64,
    /// The load generator's own CPU over the step, in seconds (user + system, every thread).
    generator_cpu_secs: f64,
    people: usize,
    writes: f64,
    seconds: f64,
    routes: HashMap<&'static str, Route>,
    /// Whole navigations, from dispatch to the last response the page needed.
    navs: HashMap<&'static str, Histogram<u64>>,
    nav_failed: u64,
    navigations: u64,
    delivery: Option<Histogram<u64>>,
    expected_deliveries: u64,
    delivered: u64,
    /// Requests that got no response, by route and reason.
    errors: BTreeMap<String, u64>,
    /// Subscribe commands sent again because no confirmation came within 500 ms, by channel.
    resubscribes: BTreeMap<String, u64>,
}

struct Stats {
    steps: Vec<Mutex<Step>>,
    invalid_samples: Mutex<Vec<(String, String)>>,
    error_samples: Mutex<Vec<(String, String)>>,
    invalid: AtomicU64,
    errors: AtomicU64,
    unread_notices: AtomicU64,
    refreshes: AtomicU64,
    rejected: AtomicU64,
    /// Subscribe commands sent again because no confirmation came within 500 ms.
    resubscribes: AtomicU64,
    /// The step being measured (None while warming up or between steps), for counters with no Action to carry it.
    current_step: Mutex<Option<usize>>,
    /// Keep-alive connections found closed by the server before a request was sent, and replaced.
    reconnects: AtomicU64,
    presence_writes: AtomicU64,
}

impl Stats {
    fn invalid(&self, step: Option<usize>, route: &'static str, user: u64, why: String) {
        self.invalid.fetch_add(1, Ordering::Relaxed);
        if let Some(s) = step {
            self.steps[s].lock().unwrap().routes.entry(route).or_default().invalid += 1;
        }
        let mut samples = self.invalid_samples.lock().unwrap();
        // At most five per route and kind of failure (the text before any digit), so one flood cannot hide another.
        let kind: String = format!("{route} {why}").chars().filter(|c| !c.is_ascii_digit()).collect();
        if samples.iter().filter(|(k, _)| *k == kind).count() < 5 && samples.len() < 200 {
            samples.push((kind, format!("{route} user {user}: {why}")));
        }
    }
    /// `kind` is the reason without details that vary (it names the error's tally); `detail` goes in samples.
    fn error(&self, step: Option<usize>, route: &'static str, user: u64, kind: String, detail: String) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        let kind = format!("{route}: {kind}");
        if let Some(s) = step {
            let mut step = self.steps[s].lock().unwrap();
            step.routes.entry(route).or_default().errors += 1;
            *step.errors.entry(kind.clone()).or_default() += 1;
        }
        let mut samples = self.error_samples.lock().unwrap();
        if samples.iter().filter(|(k, _)| *k == kind).count() < 5 && samples.len() < 100 {
            let at = step.map_or("arrival".to_string(), |s| format!("step {s}"));
            samples.push((kind.clone(), format!("{kind} (user {user}, {at}, {detail})")));
        }
    }
}

// ---------------------------------------------------------------------------------------------
// What the harness knows about messages: the plan's, plus every acknowledged post.

struct Known {
    room_of: HashMap<u64, u64>,
    /// Newest acknowledged post per room: (message id, acknowledged at).
    newest: HashMap<u64, u64>,
    /// Ids seen in pages before their post was acknowledged: checked at the end.
    deferred: Vec<(u64, Vec<u64>, u64, &'static str)>, // (message id, allowed rooms, user, route)
    audit: Vec<Value>,
    /// Posts sent without a valid acknowledgement: the server may still have committed them.
    unacknowledged: Vec<(u64, String)>, // (room, body)
}

// ---------------------------------------------------------------------------------------------
// Fan-out: who must receive each post.

struct PostRec {
    room: u64,
    sent: Instant,
    step: Option<usize>,
    expected: HashSet<usize>,
    got: HashSet<usize>,
    exempt: HashSet<usize>,
}

struct Fanout {
    posts: Mutex<HashMap<u64, PostRec>>,
}

impl Fanout {
    fn receipt(&self, seq: u64, viewer: usize, stats: &Stats) {
        let now = Instant::now();
        let mut posts = self.posts.lock().unwrap();
        let Some(p) = posts.get_mut(&seq) else { return };
        if p.expected.contains(&viewer)
            && p.got.insert(viewer)
            && let Some(s) = p.step
        {
            let mut step = stats.steps[s].lock().unwrap();
            step.delivered += 1;
            step.delivery.get_or_insert_with(hist).record(now.duration_since(p.sent).as_micros() as u64).ok();
        }
    }
    /// A viewer leaving a room owes nothing for posts to it still in flight.
    fn left(&self, viewer: usize, room: u64) {
        for p in self.posts.lock().unwrap().values_mut() {
            if p.room == room && p.expected.contains(&viewer) && !p.got.contains(&viewer) {
                p.exempt.insert(viewer);
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// A viewer: one person with one tab.

#[derive(Default)]
struct ViewerState {
    room: Option<u64>,
    /// The room whose message stream the server has confirmed.
    confirmed: Option<u64>,
    stream_names: HashMap<u64, String>,
    oldest: Option<u64>,
    since_ms: u64,
}

struct Viewer {
    index: usize,
    user: u64,
    cookie: String,
    rooms: Vec<u64>,
    state: Mutex<ViewerState>,
    heartbeat: Notify,
    /// Failed checks and errors this viewer's requests have had: a navigation failed if its own count moved.
    failures: AtomicU64,
    /// Subscribe commands awaiting confirmation, with when each was last sent.
    pending: Mutex<HashMap<String, Instant>>,
    ws: tokio::sync::Mutex<Option<futures_util::stream::SplitSink<WsStream, WsMessage>>>,
}

type WsStream = tokio_tungstenite::WebSocketStream<TcpStream>;

#[derive(Clone, Copy)]
enum Kind {
    Visit(u64),
    Scroll,
    Search(usize),
    Return,
    Post(u64),
}

struct Action {
    kind: Kind,
    /// When the generator dispatched it; a busy viewer's queue counts against the server.
    scheduled: Instant,
    step: Option<usize>,
}

impl Action {
    /// A request the page makes after an earlier one finished starts its own clock.
    fn then(&self) -> Action {
        Action { kind: self.kind, scheduled: Instant::now(), step: self.step }
    }
}

struct Ctx {
    addr: String,
    gzip: bool,
    plan: Plan,
    stats: Stats,
    known: Mutex<Known>,
    fanout: Fanout,
    viewers: Vec<Arc<Viewer>>,
    seq: AtomicU64,
    stopping: AtomicBool,
    member_rooms: HashMap<u64, HashSet<u64>>, // user -> rooms
}

fn identifier(channel: &str, room: Option<u64>) -> String {
    match room {
        Some(r) => json!({"channel": channel, "room_id": r}).to_string(),
        None => json!({"channel": channel}).to_string(),
    }
}

fn stream_identifier(channel: &str, name: &str) -> String {
    json!({"channel": channel, "signed_stream_name": name}).to_string()
}

fn decode(r: &Resp) -> Result<String, String> {
    let gz = r.headers.get("content-encoding").is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"gzip"));
    let bytes = if gz {
        let mut out = Vec::new();
        GzDecoder::new(r.body.as_ref()).read_to_end(&mut out).map_err(|e| format!("gzip: {e}"))?;
        out
    } else {
        r.body.to_vec()
    };
    String::from_utf8(bytes).map_err(|_| "body is not UTF-8".to_string())
}

fn content_type(r: &Resp) -> &str {
    r.headers.get("content-type").and_then(|v| v.to_str().ok()).unwrap_or("")
}

fn re(pattern: &'static str) -> &'static regex::Regex {
    use std::sync::LazyLock;
    static CACHE: LazyLock<Mutex<HashMap<&'static str, &'static regex::Regex>>> = LazyLock::new(Default::default);
    let mut c = CACHE.lock().unwrap();
    c.entry(pattern).or_insert_with(|| Box::leak(Box::new(regex::Regex::new(pattern).unwrap())))
}

fn message_ids(html: &str) -> Vec<u64> {
    re(r#"data-message-id="(\d+)""#).captures_iter(html).filter_map(|c| c[1].parse().ok()).collect()
}

fn page_user(html: &str) -> Option<u64> {
    re(r#"<meta name="current-user-id" content="(\d+)""#).captures(html).and_then(|c| c[1].parse().ok())
}

fn tally_server_timing(r: &Resp, route: &mut Route) {
    for v in r.headers.get_all("server-timing") {
        let Ok(text) = v.to_str() else { continue };
        for metric in text.split(',') {
            let metric = metric.trim();
            let name = metric.split(';').next().unwrap_or("").trim();
            let desc = metric.split(';').find_map(|p| p.trim().strip_prefix("desc=")).map(|d| d.trim_matches('"')).unwrap_or("");
            match name {
                "page" if desc == "hit" => route.page_hit += 1,
                "page" if desc == "miss" => route.page_miss += 1,
                "fragments" => {
                    for kv in desc.split_whitespace() {
                        match kv.split_once('=') {
                            Some(("hits", n)) => route.frag_hits += n.parse().unwrap_or(0),
                            Some(("misses", n)) => route.frag_misses += n.parse().unwrap_or(0),
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

impl Ctx {
    fn invalid(&self, v: &Viewer, step: Option<usize>, route: &'static str, why: String) {
        v.failures.fetch_add(1, Ordering::Relaxed);
        self.stats.invalid(step, route, v.user, why);
    }

    fn error(&self, v: &Viewer, step: Option<usize>, route: &'static str, kind: String, detail: String) {
        v.failures.fetch_add(1, Ordering::Relaxed);
        self.stats.error(step, route, v.user, kind, detail);
    }

    fn headers(&self, v: &Viewer, accept: &str, frame: Option<&str>) -> Vec<(&'static str, String)> {
        let mut h = vec![
            ("cookie", v.cookie.clone()),
            ("accept", accept.to_string()),
            ("accept-encoding", if self.gzip { "gzip" } else { "identity" }.to_string()),
            ("sec-fetch-site", "same-origin".to_string()),
        ];
        if let Some(f) = frame {
            h.push(("turbo-frame", f.to_string()));
        }
        h
    }

    /// One request on the viewer's connection, recorded under `route` when timed. Returns the decoded body of a
    /// 200 with the expected content type, or records why not.
    #[allow(clippy::too_many_arguments)]
    async fn get(
        &self,
        conn: &mut Option<SendRequest<Full<Bytes>>>,
        v: &Viewer,
        route: &'static str,
        path: &str,
        headers: Vec<(&'static str, String)>,
        body: Option<Bytes>,
        want_type: &str,
        a: &Action,
    ) -> Option<(String, Resp)> {
        self.get_status(conn, v, route, path, headers, body, want_type, 200, a).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn get_status(
        &self,
        conn: &mut Option<SendRequest<Full<Bytes>>>,
        v: &Viewer,
        route: &'static str,
        path: &str,
        headers: Vec<(&'static str, String)>,
        body: Option<Bytes>,
        want_type: &str,
        want_status: u16,
        a: &Action,
    ) -> Option<(String, Resp)> {
        // A keep-alive connection the server has since closed (an idle timeout, say) is replaced before
        // use, as a browser does, rather than counted as an error. It can still close just as a request
        // is sent on it; that is an error, noted as being on a reused connection.
        if conn.as_ref().is_some_and(|c| c.is_closed()) {
            *conn = None;
            self.stats.reconnects.fetch_add(1, Ordering::Relaxed);
        }
        let reused = conn.is_some();
        if conn.is_none() {
            let started = Instant::now();
            match connect(&self.addr).await {
                Ok(c) => *conn = Some(c),
                Err(e) => {
                    let detail = format!("{path} after {} ms", started.elapsed().as_millis());
                    self.error(v, a.step, route, format!("connect failed: {e}"), detail);
                    return None;
                }
            }
        }
        let sent = Instant::now();
        let method = if body.is_some() { "POST" } else { "GET" };
        let result = send(conn.as_mut().unwrap(), &self.addr, method, path, &headers, body.unwrap_or_default()).await;
        let done = Instant::now();
        let r = match result {
            Ok(r) => r,
            Err(e) => {
                *conn = None;
                let connection = if reused { "a reused" } else { "a new" };
                let detail = format!("{method} {path} after {} ms", (done - sent).as_millis());
                self.error(v, a.step, route, format!("{e} on {connection} connection"), detail);
                return None;
            }
        };
        if r.headers.get("connection").is_some_and(|c| c == "close") {
            *conn = None;
        }
        if let Some(s) = a.step {
            let mut step = self.stats.steps[s].lock().unwrap();
            let rt = step.routes.entry(route).or_default();
            rt.record(a.scheduled, sent, done);
            rt.bytes += r.body.len() as u64;
            rt.timed += 1;
            tally_server_timing(&r, rt);
        }
        if r.status != want_status {
            self.invalid(v, a.step, route, format!("{path}: HTTP {}, expected {want_status}", r.status));
            return None;
        }
        if want_status == 204 {
            return Some((String::new(), r));
        }
        if !content_type(&r).starts_with(want_type) {
            self.invalid(v, a.step, route, format!("{path}: content-type {:?}", content_type(&r)));
            return None;
        }
        match decode(&r) {
            Ok(text) => Some((text, r)),
            Err(why) => {
                self.invalid(v, a.step, route, format!("{path}: {why}"));
                None
            }
        }
    }

    fn ok(&self, a: &Action, route: &'static str) {
        if let Some(s) = a.step {
            self.stats.steps[s].lock().unwrap().routes.entry(route).or_default().ok += 1;
        }
    }

    /// Ids must belong to `allowed` rooms; ids not known yet are deferred to the end-of-run audit.
    fn check_ids(&self, ids: &[u64], allowed: &[u64], user: u64, route: &'static str) -> Result<(), String> {
        let mut seen = HashSet::new();
        let mut known = self.known.lock().unwrap();
        for id in ids {
            if !seen.insert(*id) {
                return Err(format!("message {id} listed twice"));
            }
            match known.room_of.get(id) {
                Some(room) if allowed.contains(room) => {}
                Some(room) => return Err(format!("message {id} belongs to room {room}, outside {allowed:?}")),
                None => known.deferred.push((*id, allowed.to_vec(), user, route)),
            }
        }
        Ok(())
    }

    async fn room_page(&self, conn: &mut Option<SendRequest<Full<Bytes>>>, v: &Viewer, room: u64, a: &Action) -> Option<()> {
        let newest_before = self.known.lock().unwrap().newest.get(&room).copied();
        let headers = self.headers(v, "text/html, application/xhtml+xml", None);
        let (html, _) = self.get(conn, v, "room", &format!("/rooms/{room}"), headers, None, "text/html", a).await?;
        let check = (|| {
            if page_user(&html) != Some(v.user) {
                return Err(format!("page names user {:?}", page_user(&html)));
            }
            let ids = message_ids(&html);
            let total = self.plan.rooms.get(&room).map_or(0, Vec::len);
            if ids.len() != PAGE.min(total) && total >= PAGE {
                return Err(format!("{} messages, expected {PAGE}", ids.len()));
            }
            self.check_ids(&ids, &[room], v.user, "room")?;
            if let Some(newest) = newest_before
                && !ids.iter().any(|id| *id >= newest)
            {
                return Err(format!("stale: message {newest} was acknowledged before the request and is missing"));
            }
            Ok(ids)
        })();
        match check {
            Ok(ids) => {
                let name = re(r#"<turbo-cable-stream-source channel="RoomMessagesChannel" signed-stream-name="([^"]+)""#)
                    .captures(&html)
                    .map(|c| unescape(&c[1]));
                let since = re(r#"data-message-updated-at="(\d+)""#).captures_iter(&html).filter_map(|c| c[1].parse::<u64>().ok()).max();
                let mut s = v.state.lock().unwrap();
                if let Some(name) = name {
                    s.stream_names.insert(room, name);
                }
                s.oldest = ids.iter().copied().min();
                s.since_ms = since.unwrap_or(s.since_ms);
                drop(s);
                self.ok(a, "room");
                Some(())
            }
            Err(why) => {
                self.invalid(v, a.step, "room", format!("/rooms/{room}: {why}"));
                None
            }
        }
    }

    async fn refresh(&self, conn: &mut Option<SendRequest<Full<Bytes>>>, v: &Viewer, room: u64, reason: &str, a: &Action) {
        let since = v.state.lock().unwrap().since_ms;
        let headers = self.headers(v, "text/vnd.turbo-stream.html, text/html, application/xhtml+xml", None);
        let path = format!("/rooms/{room}/refresh?since={since}&reason={reason}");
        let Some((body, _)) = self.get(conn, v, "refresh", &path, headers, None, "text/vnd.turbo-stream.html", a).await else { return };
        match self.check_ids(&message_ids(&body), &[room], v.user, "refresh") {
            Ok(()) => self.ok(a, "refresh"),
            Err(why) => self.invalid(v, a.step, "refresh", format!("{path}: {why}")),
        }
    }

    async fn sidebar(&self, conn: &mut Option<SendRequest<Full<Bytes>>>, v: &Viewer, a: &Action) -> Option<String> {
        let headers = self.headers(v, "text/html, application/xhtml+xml", Some("user_sidebar"));
        let (html, _) = self.get(conn, v, "sidebar", "/users/me/sidebar", headers, None, "text/html", a).await?;
        let rooms: HashSet<u64> = re(r#"data-room-id="(\d+)""#).captures_iter(&html).filter_map(|c| c[1].parse().ok()).collect();
        let current = v.state.lock().unwrap().room;
        let why = match rooms.iter().find(|r| !v.rooms.contains(r)) {
            Some(r) => Some(format!("lists room {r}, which user {} is not in", v.user)),
            None => current.filter(|c| !rooms.contains(c)).map(|c| format!("omits the current room {c}")),
        };
        match why {
            Some(why) => {
                self.invalid(v, a.step, "sidebar", why);
                None
            }
            None => {
                self.ok(a, "sidebar");
                Some(html)
            }
        }
    }

    async fn scroll(&self, conn: &mut Option<SendRequest<Full<Bytes>>>, v: &Viewer, a: &Action) {
        let (room, oldest) = {
            let s = v.state.lock().unwrap();
            (s.room, s.oldest)
        };
        let Some(room) = room else { return };
        let before = match oldest {
            Some(b) => b,
            None => match self.plan.rooms.get(&room).and_then(|ids| ids.len().checked_sub(PAGE).map(|i| ids[i])) {
                Some(b) => b,
                None => return,
            },
        };
        let headers = self.headers(v, "text/html, application/xhtml+xml", None);
        let path = format!("/rooms/{room}/messages?before={before}");
        let older = self.plan.rooms.get(&room).map_or(0, |all| all.iter().filter(|id| **id < before).count());
        let want = if older == 0 { 204 } else { 200 };
        let Some((html, _)) = self.get_status(conn, v, "scroll", &path, headers, None, "text/html", want, a).await else { return };
        if older == 0 {
            // The start of the room: the next scroll begins again from the newest page.
            v.state.lock().unwrap().oldest = None;
            self.ok(a, "scroll");
            return;
        }
        let ids = message_ids(&html);
        // Not "every id below the anchor": the page is ordered by created_at, and concurrent posts can commit out of
        // id order. The anchor itself must not reappear.
        let check = if ids.contains(&before) {
            Err(format!("lists its own anchor {before}"))
        } else if ids.len() != PAGE.min(older) {
            Err(format!("{} messages, expected {}", ids.len(), PAGE.min(older)))
        } else {
            self.check_ids(&ids, &[room], v.user, "scroll")
        };
        match check {
            Ok(()) => {
                // Keep scrolling back; past the start of the room, return to the newest page's anchor next time.
                v.state.lock().unwrap().oldest = ids.iter().copied().min();
                self.ok(a, "scroll");
            }
            Err(why) => self.invalid(v, a.step, "scroll", format!("{path}: {why}")),
        }
    }

    async fn search(&self, conn: &mut Option<SendRequest<Full<Bytes>>>, v: &Viewer, term: usize, a: &Action) {
        let Some(term) = self.plan.terms.get(term) else { return };
        let headers = self.headers(v, "text/html, application/xhtml+xml", None);
        let path = format!("/searches?q={}", urlencode(term));
        let Some((html, _)) = self.get(conn, v, "search", &path, headers, None, "text/html", a).await else { return };
        let check = if page_user(&html) != Some(v.user) {
            Err(format!("page names user {:?}", page_user(&html)))
        } else {
            self.check_ids(&message_ids(&html), &v.rooms, v.user, "search")
        };
        match check {
            Ok(()) => self.ok(a, "search"),
            Err(why) => self.invalid(v, a.step, "search", format!("{path}: {why}")),
        }
    }

    async fn ws_send(&self, v: &Viewer, text: String) -> bool {
        let mut ws = v.ws.lock().await;
        match ws.as_mut() {
            Some(tx) => tx.send(WsMessage::text(text)).await.is_ok(),
            None => false,
        }
    }

    /// Move the tab's room subscriptions from the current room to `room`.
    async fn move_subscriptions(&self, v: &Viewer, room: u64) {
        let (old, old_name, new_name) = {
            let mut s = v.state.lock().unwrap();
            let old = s.room.replace(room);
            s.confirmed = None;
            (old, old.and_then(|o| s.stream_names.get(&o).cloned()), s.stream_names.get(&room).cloned())
        };
        if let Some(old) = old {
            self.fanout.left(v.index, old);
            let mut leave = vec![identifier("PresenceChannel", Some(old)), identifier("HeartbeatChannel", None)];
            if let Some(n) = old_name {
                leave.push(stream_identifier("RoomMessagesChannel", &n));
            }
            for ident in leave {
                v.pending.lock().unwrap().remove(&ident);
                self.ws_send(v, json!({"command": "unsubscribe", "identifier": ident}).to_string()).await;
            }
        }
        let mut join = vec![identifier("HeartbeatChannel", None), identifier("PresenceChannel", Some(room))];
        if let Some(n) = new_name {
            join.push(stream_identifier("RoomMessagesChannel", &n));
        }
        for ident in join {
            v.pending.lock().unwrap().insert(ident.clone(), Instant::now());
            self.ws_send(v, json!({"command": "subscribe", "identifier": ident}).to_string()).await;
        }
        self.stats.presence_writes.fetch_add(1, Ordering::Relaxed);
    }

    async fn visit(&self, conn: &mut Option<SendRequest<Full<Bytes>>>, v: &Viewer, room: u64, a: &Action) {
        if self.room_page(conn, v, room, a).await.is_none() {
            return;
        }
        let heartbeat = v.heartbeat.notified();
        tokio::pin!(heartbeat);
        heartbeat.as_mut().enable();
        let sent = Instant::now();
        self.move_subscriptions(v, room).await;
        if tokio::time::timeout(Duration::from_secs(5), heartbeat).await.is_err() {
            self.invalid(v, a.step, "subscribe", format!("HeartbeatChannel unconfirmed 5s after visiting room {room}"));
            return;
        }
        // The socket's part of a visit: unsubscribing the old room and subscribing the new, until confirmed.
        if let Some(s) = a.step {
            let mut step = self.stats.steps[s].lock().unwrap();
            let r = step.routes.entry("subscribe").or_default();
            r.record(sent, sent, Instant::now());
            r.ok += 1;
        }
        self.refresh(conn, v, room, "connection", &a.then()).await;
    }

    async fn post(&self, conn: &mut Option<SendRequest<Full<Bytes>>>, v: &Viewer, room: u64, a: &Action) {
        if v.state.lock().unwrap().room != Some(room) {
            self.visit(conn, v, room, a).await;
        }
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let text = format!("users bmk{seq}z");
        let expected: HashSet<usize> =
            self.viewers.iter().filter(|w| w.state.lock().unwrap().confirmed == Some(room)).map(|w| w.index).collect();
        let (mut headers, body) = message_request(&v.cookie, "", &text);
        headers.push(("accept-encoding", if self.gzip { "gzip" } else { "identity" }.into()));
        self.fanout
            .posts
            .lock()
            .unwrap()
            .insert(seq, PostRec { room, sent: Instant::now(), step: a.step, expected, got: HashSet::new(), exempt: HashSet::new() });
        let path = format!("/rooms/{room}/messages");
        let Some((body, _)) = self.get(conn, v, "post", &path, headers, Some(body), "text/vnd.turbo-stream.html", a).await else {
            self.fanout.posts.lock().unwrap().remove(&seq);
            self.known.lock().unwrap().unacknowledged.push((room, text));
            return;
        };
        if !valid_message_post(200, body.as_bytes(), &text) {
            self.fanout.posts.lock().unwrap().remove(&seq);
            self.known.lock().unwrap().unacknowledged.push((room, text.clone()));
            self.invalid(v, a.step, "post", format!("{path}: not one appended message carrying the body"));
            return;
        }
        let id = message_ids(&body)[0];
        let mut known = self.known.lock().unwrap();
        if let Some(prev) = known.room_of.insert(id, room) {
            drop(known);
            self.invalid(v, a.step, "post", format!("message id {id} acknowledged twice (room {prev}, then {room})"));
            return;
        }
        let newest = known.newest.entry(room).or_insert(id);
        *newest = (*newest).max(id);
        known.audit.push(json!({"id": id, "room": room, "user": v.user, "body": text}));
        drop(known);
        self.ok(a, "post");
    }

    async fn act(&self, conn: &mut Option<SendRequest<Full<Bytes>>>, v: &Viewer, a: Action) {
        let failures = v.failures.load(Ordering::Relaxed);
        let name = match a.kind {
            Kind::Visit(room) => {
                self.visit(conn, v, room, &a).await;
                "visit"
            }
            Kind::Scroll => {
                self.scroll(conn, v, &a).await;
                "scroll"
            }
            Kind::Search(term) => {
                self.search(conn, v, term, &a).await;
                "search"
            }
            Kind::Return => {
                let room = v.state.lock().unwrap().room;
                if let Some(room) = room {
                    self.refresh(conn, v, room, "visibility", &a).await;
                }
                self.sidebar(conn, v, &a.then()).await;
                "return"
            }
            Kind::Post(room) => {
                self.post(conn, v, room, &a).await;
                "post"
            }
        };
        if let Some(s) = a.step {
            let failed = v.failures.load(Ordering::Relaxed) > failures;
            let mut step = self.stats.steps[s].lock().unwrap();
            if failed {
                step.nav_failed += 1;
            } else {
                step.navs.entry(name).or_insert_with(hist).record(a.scheduled.elapsed().as_micros() as u64).ok();
            }
        }
    }
}

/// The socket's reading half: confirmations, broadcasts and unread notices.
async fn read_socket(ctx: Arc<Ctx>, v: Arc<Viewer>, mut rx: futures_util::stream::SplitStream<WsStream>) {
    let heartbeat = identifier("HeartbeatChannel", None);
    while let Some(Ok(msg)) = rx.next().await {
        let WsMessage::Text(text) = msg else { continue };
        let text = text.as_str();
        if text.contains(r#""type":"ping""#) || text.contains(r#""type":"welcome""#) {
            continue;
        }
        if text.contains("reject_subscription") {
            ctx.stats.rejected.fetch_add(1, Ordering::Relaxed);
            if let Ok(value) = serde_json::from_str::<Value>(text) {
                v.pending.lock().unwrap().remove(value["identifier"].as_str().unwrap_or(""));
            }
            continue;
        }
        if text.contains("confirm_subscription") {
            let Ok(value) = serde_json::from_str::<Value>(text) else { continue };
            let ident = value["identifier"].as_str().unwrap_or("");
            v.pending.lock().unwrap().remove(ident);
            if ident == heartbeat {
                v.heartbeat.notify_one();
            } else if ident.contains("RoomMessagesChannel") {
                let mut s = v.state.lock().unwrap();
                let room = s.room;
                if room.and_then(|r| s.stream_names.get(&r)).is_some_and(|n| ident == stream_identifier("RoomMessagesChannel", n)) {
                    s.confirmed = room;
                }
            }
            continue;
        }
        if is_unread_notice(text) {
            ctx.stats.unread_notices.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        for seq in markers(text) {
            ctx.fanout.receipt(seq, v.index, &ctx.stats);
        }
    }
}

async fn open_socket(ctx: &Arc<Ctx>, v: &Arc<Viewer>, subs: Vec<String>) -> Res<()> {
    let mut req = format!("ws://{}/cable", ctx.addr).into_client_request()?;
    let h = req.headers_mut();
    h.insert("cookie", v.cookie.parse()?);
    h.insert("origin", origin_for(&ctx.addr).parse()?);
    h.insert("sec-websocket-protocol", "actioncable-v1-json, actioncable-unsupported".parse()?);
    if let Some(user_agent) = user_agent() {
        h.insert("user-agent", user_agent.parse()?);
    }
    let stream = TcpStream::connect(&ctx.addr).await?;
    stream.set_nodelay(true)?;
    let (ws, _) = tokio_tungstenite::client_async(req, stream).await?;
    let (mut tx, rx) = ws.split();
    for ident in subs {
        v.pending.lock().unwrap().insert(ident.clone(), Instant::now());
        tx.send(WsMessage::text(json!({"command": "subscribe", "identifier": ident}).to_string())).await?;
    }
    *v.ws.lock().await = Some(tx);
    tokio::spawn(read_socket(ctx.clone(), v.clone(), rx));
    tokio::spawn(guarantee_subscriptions(ctx.clone(), v.clone()));
    Ok(())
}

/// As Action Cable's JavaScript SubscriptionGuarantor does: a subscribe that is not confirmed within 500 ms is sent
/// again until it is. A server may run one connection's commands concurrently, so `unsubscribe X` followed by
/// `subscribe X` (a page moving to another room keeps its HeartbeatChannel) can be applied out of order; Rails then
/// answers the subscribe with AlreadySubscribedError and no confirmation, and browsers recover by resending.
async fn guarantee_subscriptions(ctx: Arc<Ctx>, v: Arc<Viewer>) {
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    while !ctx.stopping.load(Ordering::Relaxed) {
        tick.tick().await;
        let due: Vec<String> = {
            let mut pending = v.pending.lock().unwrap();
            let now = Instant::now();
            let due: Vec<String> = pending
                .iter()
                .filter(|(_, sent)| now.duration_since(**sent) >= Duration::from_millis(500))
                .map(|(i, _)| i.clone())
                .collect();
            for ident in &due {
                pending.insert(ident.clone(), now);
            }
            due
        };
        for ident in due {
            if ctx.ws_send(&v, json!({"command": "subscribe", "identifier": ident}).to_string()).await {
                ctx.stats.resubscribes.fetch_add(1, Ordering::Relaxed);
                if let Some(s) = *ctx.stats.current_step.lock().unwrap() {
                    let channel = serde_json::from_str::<Value>(&ident)
                        .ok()
                        .and_then(|v| v["channel"].as_str().map(str::to_string))
                        .unwrap_or_default();
                    *ctx.stats.steps[s].lock().unwrap().resubscribes.entry(channel).or_default() += 1;
                }
            }
        }
    }
}

/// Arrival: the room page, the sidebar frame and the socket with the page's subscriptions. Untimed but checked.
async fn arrive(ctx: &Arc<Ctx>, v: &Arc<Viewer>, conn: &mut Option<SendRequest<Full<Bytes>>>, room: u64) -> Res<()> {
    let a = Action { kind: Kind::Visit(room), scheduled: Instant::now(), step: None };
    ctx.room_page(conn, v, room, &a).await.ok_or("arrival room page failed its checks")?;
    let sidebar = ctx.sidebar(conn, v, &a).await.ok_or("arrival sidebar failed its checks")?;
    // The sidebar's own streams (the user's rooms list, the shared rooms list).
    let mut subs = vec![identifier("UnreadRoomsChannel", None)];
    for c in re(r#"<turbo-cable-stream-source channel="([^"]+)" signed-stream-name="([^"]+)""#).captures_iter(&sidebar) {
        subs.push(stream_identifier(&c[1], &unescape(&c[2])));
    }
    let heartbeat = v.heartbeat.notified();
    tokio::pin!(heartbeat);
    heartbeat.as_mut().enable();
    open_socket(ctx, v, subs).await?;
    ctx.move_subscriptions(v, room).await;
    tokio::time::timeout(Duration::from_secs(10), heartbeat).await.map_err(|_| "HeartbeatChannel unconfirmed at arrival")?;
    ctx.refresh(conn, v, room, "connection", &a).await;
    Ok(())
}

struct StepPlan {
    people: usize,
    navigations_per_sec: f64,
    writes_per_sec: f64,
}

pub async fn users(a: &Args) -> Res<Value> {
    let addr = host_port(&a.get("base"));
    let plan = load_plan(&a.get("plan"))?;
    let step_secs: f64 = a.num("step-secs", 20.0);
    let warmup_secs: f64 = a.num("warmup-secs", 10.0);
    let write_rate: Option<f64> = a.opt("write-rate").and_then(|w| w.parse().ok());
    let refresh = refresh_period(Some(&a.opt("refresh-secs").unwrap_or_else(|| "50".into())))?;
    let slo_ms: f64 = a.num("slo-ms", 100.0);
    let delivery_slo_ms: f64 = a.num("delivery-slo-ms", 500.0);
    let abort_ms: f64 = a.num("abort-ms", 2000.0);
    let gzip = a.num("gzip", 1u8) != 0;
    let mut rng = Rng(a.num("seed", 0x9e37_79b9_7f4a_7c15u64) | 1);
    let list = |k: &str| -> Option<Vec<f64>> { a.opt(k).map(|v| v.split(',').filter_map(|r| r.parse().ok()).collect()) };
    // Two ways to step the load. `--people 500,1000,2000`: the population grows, each person navigating every
    // `--think-secs` and posting every `--post-secs` on average (the capacity question: how many people). Or
    // `--viewers N --rates 100,200`: a fixed population offered rising navigation rates (a stress ramp, at
    // per-person rates no person keeps up, so reuse of per-person caches is higher than any deployment's).
    let steps_plan: Vec<StepPlan> = if let Some(people) = list("people") {
        let think: f64 = a.num("think-secs", 30.0);
        let post_secs: f64 = a.num("post-secs", 300.0);
        people
            .iter()
            .map(|p| {
                let p = (*p as usize).min(plan.users.len());
                StepPlan { people: p, navigations_per_sec: p as f64 / think, writes_per_sec: write_rate.unwrap_or(p as f64 / post_secs) }
            })
            .collect()
    } else {
        let viewers: usize = a.num("viewers", 100).min(plan.users.len());
        list("rates")
            .unwrap_or_else(|| vec![50.0])
            .iter()
            .map(|r| StepPlan { people: viewers, navigations_per_sec: *r, writes_per_sec: write_rate.unwrap_or(0.0) })
            .collect()
    };
    if steps_plan.is_empty() || steps_plan.iter().any(|s| s.people == 0 || s.navigations_per_sec <= 0.0) {
        return Err("--people or --rates must list positive values".into());
    }
    let viewers_n = steps_plan.iter().map(|s| s.people).max().unwrap_or(0);
    // Navigation mix: visit, scroll, search, return.
    let mix = [0.55, 0.2, 0.1, 0.15];

    let mut room_of = HashMap::new();
    for (room, ids) in &plan.rooms {
        for id in ids {
            room_of.insert(*id, *room);
        }
    }
    let member_rooms = plan.users.iter().map(|u| (u.id, u.rooms.iter().copied().collect())).collect();
    let viewers: Vec<Arc<Viewer>> = plan.users[..viewers_n]
        .iter()
        .enumerate()
        .map(|(index, u)| {
            Arc::new(Viewer {
                index,
                user: u.id,
                cookie: u.cookie.clone(),
                rooms: u.rooms.clone(),
                state: Mutex::new(ViewerState::default()),
                heartbeat: Notify::new(),
                failures: AtomicU64::new(0),
                pending: Mutex::new(HashMap::new()),
                ws: tokio::sync::Mutex::new(None),
            })
        })
        .collect();
    // Rooms weighted by membership: busy rooms get the visits and the posts.
    let mut members_of: HashMap<u64, Vec<usize>> = HashMap::new();
    for v in &viewers {
        for r in &v.rooms {
            members_of.entry(*r).or_default().push(v.index);
        }
    }
    let ctx = Arc::new(Ctx {
        addr,
        gzip,
        stats: Stats {
            steps: steps_plan
                .iter()
                .map(|p| {
                    Mutex::new(Step { offered: p.navigations_per_sec, people: p.people, writes: p.writes_per_sec, ..Default::default() })
                })
                .collect(),
            invalid_samples: Mutex::new(Vec::new()),
            error_samples: Mutex::new(Vec::new()),
            invalid: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            unread_notices: AtomicU64::new(0),
            refreshes: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            presence_writes: AtomicU64::new(0),
            resubscribes: AtomicU64::new(0),
            current_step: Mutex::new(None),
            reconnects: AtomicU64::new(0),
        },
        known: Mutex::new(Known { room_of, newest: HashMap::new(), deferred: Vec::new(), audit: Vec::new(), unacknowledged: Vec::new() }),
        fanout: Fanout { posts: Mutex::new(HashMap::new()) },
        viewers: viewers.clone(),
        seq: AtomicU64::new(1),
        stopping: AtomicBool::new(false),
        member_rooms,
        plan,
    });

    let room_weight = |r: &u64| members_of.get(r).map_or(1, Vec::len) as f64;
    let mut senders = Vec::new();
    let mut receivers = Vec::new();
    for _ in &viewers {
        let (tx, rx) = mpsc::unbounded_channel::<Action>();
        senders.push(tx);
        receivers.push(Some(rx));
    }
    let mut handles = Vec::new();
    let gate = Arc::new(tokio::sync::Semaphore::new(32));
    let stop = Arc::new(AtomicBool::new(false));
    let arrival_failures = Arc::new(AtomicUsize::new(0));
    let writer_stop = Arc::new(AtomicBool::new(false));
    let active = Arc::new(AtomicUsize::new(0));

    // The writer: a Poisson process of posts, each into a room by membership weight, by an active member who is in
    // it (any active member when nobody is). Its rate follows the step.
    let rooms_by_weight: Vec<(u64, f64)> = members_of.iter().map(|(r, m)| (*r, m.len() as f64)).collect();
    let writer_rate = Arc::new(AtomicU64::new(0f64.to_bits()));
    let current_step = Arc::new(Mutex::new(None::<usize>));
    let writer = {
        let (senders, viewers, stop, current_step, writer_rate, active) =
            (senders.clone(), viewers.clone(), writer_stop.clone(), current_step.clone(), writer_rate.clone(), active.clone());
        let members_of = members_of.clone();
        let mut wrng = Rng(rng.next() | 1);
        tokio::spawn(async move {
            let mut next = Instant::now();
            while !stop.load(Ordering::Relaxed) {
                let rate = f64::from_bits(writer_rate.load(Ordering::Relaxed));
                if rate <= 0.0 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    next = Instant::now();
                    continue;
                }
                next += wrng.exp(rate);
                tokio::time::sleep_until(next.into()).await;
                if f64::from_bits(writer_rate.load(Ordering::Relaxed)) <= 0.0 {
                    continue;
                }
                let n = active.load(Ordering::Relaxed);
                let weights: Vec<f64> =
                    rooms_by_weight.iter().map(|(r, _)| members_of[r].iter().filter(|i| **i < n).count() as f64).collect();
                if weights.iter().sum::<f64>() == 0.0 {
                    continue;
                }
                let room = rooms_by_weight[wrng.weighted(&weights)].0;
                let members: Vec<usize> = members_of[&room].iter().copied().filter(|i| *i < n).collect();
                let present: Vec<usize> =
                    members.iter().copied().filter(|i| viewers[*i].state.lock().unwrap().room == Some(room)).collect();
                let poster = if present.is_empty() { members[wrng.below(members.len())] } else { present[wrng.below(present.len())] };
                let step = *current_step.lock().unwrap();
                let _ = senders[poster].send(Action { kind: Kind::Post(room), scheduled: Instant::now(), step });
            }
        })
    };

    let mut schedule: VecDeque<(Option<usize>, f64)> = VecDeque::new();
    if warmup_secs > 0.0 {
        schedule.push_back((None, warmup_secs));
    }
    for i in 0..steps_plan.len() {
        schedule.push_back((Some(i), step_secs));
    }
    let mut completed_steps = 0;
    let mut lag = hist();
    let mut stopped_early = None;
    let mut arrivals = Vec::new();
    while let Some((step, secs)) = schedule.pop_front() {
        let sp = &steps_plan[step.unwrap_or(0)];
        // Arrivals: the step's new people come in (room page, sidebar, socket), 32 at a time, untimed, with the
        // previous population still browsing at the previous step's rates.
        let before = active.load(Ordering::Relaxed);
        if sp.people > before {
            phase(&format!("arrive {before}..{}", sp.people));
            // Arrivals are untimed; what they cost is not charged to the step before them.
            *ctx.stats.current_step.lock().unwrap() = None;
            let arrive_start = Instant::now();
            for v in &viewers[before..sp.people] {
                let weights: Vec<f64> = v.rooms.iter().map(room_weight).collect();
                let start_room = v.rooms[rng.weighted(&weights)];
                let mut rx = receivers[v.index].take().expect("arrives once");
                let (actx, av, gate, failures) = (ctx.clone(), v.clone(), gate.clone(), arrival_failures.clone());
                handles.push(tokio::spawn(async move {
                    let (ctx, v) = (actx, av);
                    let mut conn = None;
                    {
                        let _permit = gate.acquire().await;
                        if let Err(e) = arrive(&ctx, &v, &mut conn, start_room).await {
                            ctx.invalid(&v, None, "room", format!("arrival: {e}"));
                            failures.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    while let Some(action) = rx.recv().await {
                        ctx.act(&mut conn, &v, action).await;
                    }
                }));
                // Presence refreshes, each socket on its own phase.
                if let Some(period) = refresh {
                    let (ctx, v, stop) = (ctx.clone(), v.clone(), stop.clone());
                    let phase = rng.unit();
                    tokio::spawn(async move {
                        let mut timer = presence_timer(period, phase);
                        loop {
                            timer.tick().await;
                            if stop.load(Ordering::Relaxed) {
                                break;
                            }
                            let room = v.state.lock().unwrap().room;
                            if let Some(room) = room {
                                let data = json!({"action": "refresh"}).to_string();
                                let cmd =
                                    json!({"command": "message", "identifier": identifier("PresenceChannel", Some(room)), "data": data});
                                if ctx.ws_send(&v, cmd.to_string()).await {
                                    ctx.stats.refreshes.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    });
                }
            }
            let deadline = Instant::now() + Duration::from_secs(120);
            let failed_before = arrival_failures.load(Ordering::Relaxed);
            loop {
                let confirmed = viewers[before..sp.people].iter().filter(|v| v.state.lock().unwrap().confirmed.is_some()).count();
                let failed = arrival_failures.load(Ordering::Relaxed) - failed_before;
                if confirmed + failed >= sp.people - before || Instant::now() > deadline {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let arrived = viewers[before..sp.people].iter().filter(|v| v.state.lock().unwrap().confirmed.is_some()).count();
            arrivals.push(json!({"from": before, "to": sp.people, "arrived": arrived, "seconds": arrive_start.elapsed().as_secs_f64()}));
            active.store(sp.people, Ordering::Relaxed);
            if arrived == 0 {
                stopped_early = Some(format!("none of people {before}..{} arrived", sp.people));
                break;
            }
        }
        let people = active.load(Ordering::Relaxed);
        let rate = sp.navigations_per_sec;
        phase(&format!("step {} {people} people {rate}/s", step.map_or("warmup".to_string(), |s| s.to_string())));
        *current_step.lock().unwrap() = step;
        *ctx.stats.current_step.lock().unwrap() = step;
        writer_rate.store(sp.writes_per_sec.to_bits(), Ordering::Relaxed);
        let started = Instant::now();
        let cpu_at_start = own_cpu_secs();
        let end = started + Duration::from_secs_f64(secs);
        let mut next = started;
        let mut navigations = 0u64;
        loop {
            next += rng.exp(rate);
            if next >= end {
                break;
            }
            tokio::time::sleep_until(next.into()).await;
            let dispatched = Instant::now();
            lag.record(dispatched.duration_since(next).as_micros() as u64).ok();
            let i = rng.below(people);
            let v = &viewers[i];
            let kind = match rng.weighted(&mix) {
                0 => {
                    let current = v.state.lock().unwrap().room;
                    let others: Vec<u64> = v.rooms.iter().copied().filter(|r| Some(*r) != current).collect();
                    if others.is_empty() {
                        Kind::Return
                    } else {
                        let weights: Vec<f64> = others.iter().map(room_weight).collect();
                        Kind::Visit(others[rng.weighted(&weights)])
                    }
                }
                1 => Kind::Scroll,
                2 => Kind::Search(rng.below(ctx.plan.terms.len().max(1))),
                _ => Kind::Return,
            };
            navigations += 1;
            let _ = senders[i].send(Action { kind, scheduled: dispatched, step });
        }
        tokio::time::sleep_until(end.into()).await;
        if let Some(s) = step {
            let mut st = ctx.stats.steps[s].lock().unwrap();
            st.seconds = secs;
            st.generator_cpu_secs = own_cpu_secs() - cpu_at_start;
            st.navigations = navigations;
            drop(st);
            completed_steps = s + 1;
            // Stop stepping once the server is clearly past saturation: latency is measured from dispatch, so a
            // backlog only grows.
            let p99 = {
                let st = ctx.stats.steps[s].lock().unwrap();
                let mut all = hist();
                for (name, h) in &st.navs {
                    if *name != "post" {
                        all.add(h).ok();
                    }
                }
                if all.is_empty() { 0.0 } else { all.value_at_quantile(0.99) as f64 / 1000.0 }
            };
            if p99 > abort_ms {
                stopped_early = Some(format!("p99 {p99:.0} ms with {people} people at {rate}/s exceeds --abort-ms {abort_ms}"));
                break;
            }
        }
    }
    writer_rate.store(0f64.to_bits(), Ordering::Relaxed);
    writer_stop.store(true, Ordering::Relaxed);
    let _ = writer.await;
    let arrived: usize = viewers[..active.load(Ordering::Relaxed)]
        .iter()
        .filter(|v| v.state.lock().unwrap().confirmed.is_some() || v.state.lock().unwrap().room.is_some())
        .count();
    let expected_arrivals = active.load(Ordering::Relaxed);
    // Drain: every queued action completes (or the run gives up after 60s), then deliveries settle.
    drop(senders);
    let drain_deadline = Instant::now() + Duration::from_secs(60);
    let mut unfinished = 0;
    for h in handles {
        let left = drain_deadline.saturating_duration_since(Instant::now());
        if tokio::time::timeout(left, h).await.is_err() {
            unfinished += 1;
        }
    }
    tokio::time::sleep(Duration::from_secs(10)).await;
    stop.store(true, Ordering::Relaxed);
    ctx.stopping.store(true, Ordering::Relaxed);

    // Deliveries: every expected viewer received each post, unless it left the room first.
    let mut missing = 0u64;
    let mut missing_samples = Vec::new();
    for (seq, p) in ctx.fanout.posts.lock().unwrap().iter() {
        let owed = p.expected.iter().filter(|i| !p.got.contains(i) && !p.exempt.contains(i)).count() as u64;
        if let Some(s) = p.step {
            let excused = p.exempt.iter().filter(|i| !p.got.contains(i)).count();
            ctx.stats.steps[s].lock().unwrap().expected_deliveries += (p.expected.len() - excused) as u64;
        }
        if owed > 0 {
            missing += owed;
            if missing_samples.len() < 10 {
                missing_samples.push(format!("post {seq} to room {}: {owed} of {} viewers never received it", p.room, p.expected.len()));
            }
        }
    }
    // Deferred ids: messages a page listed before the harness had their acknowledgement. With `--db`, the database
    // settles the rest: a post the server committed without a valid acknowledgement is reported, not mistaken
    // for a fabricated message, and every acknowledged post must be persisted with its room and body.
    let mut deferred_failures = Vec::new();
    let mut db_audit = Value::Null;
    if let Some(db) = a.opt("db") {
        let (audit_value, committed, failures) = audit_db(&db, &ctx)?;
        db_audit = audit_value;
        deferred_failures.extend(failures);
        let mut known = ctx.known.lock().unwrap();
        for (id, room) in committed {
            known.room_of.entry(id).or_insert(room);
        }
    }
    {
        let known = ctx.known.lock().unwrap();
        for (id, allowed, user, route) in &known.deferred {
            match known.room_of.get(id) {
                Some(room) if allowed.contains(room) => {}
                Some(room) => {
                    deferred_failures.push(format!("{route} user {user}: message {id} belongs to room {room}, outside {allowed:?}"))
                }
                None => deferred_failures.push(format!("{route} user {user}: message {id} is neither in the plan nor acknowledged")),
            }
        }
    }
    if let Some(path) = a.opt("audit") {
        use std::io::Write;
        let mut f = std::io::BufWriter::new(std::fs::File::create(path)?);
        for line in &ctx.known.lock().unwrap().audit {
            writeln!(f, "{line}")?;
        }
    }

    let steps: Vec<Value> = ctx.stats.steps[..completed_steps]
        .iter()
        .map(|s| {
            let s = s.lock().unwrap();
            let mut all = hist();
            let mut requests_ok = 0;
            let mut routes = serde_json::Map::new();
            for name in ROUTES {
                if let Some(r) = s.routes.get(name) {
                    requests_ok += r.ok;
                    routes.insert(name.to_string(), r.json());
                }
            }
            let mut navs = serde_json::Map::new();
            for name in ["visit", "scroll", "search", "return", "post"] {
                if let Some(h) = s.navs.get(name) {
                    if name != "post" {
                        all.add(h).ok();
                    }
                    navs.insert(name.to_string(), summary(h));
                }
            }
            let p99 = if all.is_empty() { None } else { Some(ms(all.value_at_quantile(0.99))) };
            let navigation_ok = p99.is_some_and(|p| p <= slo_ms) && s.nav_failed == 0;
            let delivery_ok = s.delivery.as_ref().is_none_or(|h| ms(h.value_at_quantile(0.99)) <= delivery_slo_ms);
            json!({
                "people": s.people,
                "offered_writes_per_sec": s.writes,
                "offered_navigations_per_sec": s.offered,
                // Percent of one core; compare with generator_cpus to see how close the generator came to its limit.
                "generator_cpu_percent": 100.0 * s.generator_cpu_secs / s.seconds.max(1e-9),
                "seconds": s.seconds,
                "navigations_scheduled": s.navigations,
                "navigations_completed_per_sec": all.len() as f64 / s.seconds.max(1e-9),
                "navigations_failed": s.nav_failed,
                "requests_ok_per_sec": requests_ok as f64 / s.seconds.max(1e-9),
                "navigation_latency": if all.is_empty() { json!({"n": 0}) } else { summary(&all) },
                "navigations": navs,
                "meets_navigation_slo": navigation_ok,
                "meets_delivery_slo": delivery_ok,
                "meets_slo": navigation_ok && delivery_ok,
                "routes": routes,
                "errors": s.errors,
                "resubscribes": s.resubscribes,
                "delivery": {
                    "expected": s.expected_deliveries,
                    "received": s.delivered,
                    "latency": s.delivery.as_ref().map(summary).unwrap_or(json!({"n": 0})),
                },
            })
        })
        .collect();
    let capacity = steps
        .iter()
        .filter(|s| s["meets_slo"] == true)
        .filter_map(|s| s["offered_navigations_per_sec"].as_f64())
        .fold(None, |m: Option<f64>, r| Some(m.map_or(r, |m| m.max(r))));
    let capacity_people = steps.iter().filter(|s| s["meets_slo"] == true).filter_map(|s| s["people"].as_u64()).max();
    let invalid = ctx.stats.invalid.load(Ordering::Relaxed) + deferred_failures.len() as u64;
    let errors = ctx.stats.errors.load(Ordering::Relaxed);
    let rejected = ctx.stats.rejected.load(Ordering::Relaxed);
    let valid = invalid == 0 && errors == 0 && missing == 0 && rejected == 0 && unfinished == 0 && arrived == expected_arrivals;
    let mut samples: Vec<String> = ctx.stats.invalid_samples.lock().unwrap().iter().map(|(_, s)| s.clone()).collect();
    samples.extend(deferred_failures.into_iter().take(10));
    samples.extend(missing_samples);
    let _ = &ctx.member_rooms;
    Ok(json!({
        "profile": "users-v1",
        "valid": valid,
        "people": expected_arrivals,
        "arrived": arrived,
        "arrivals": arrivals,
        "refresh_secs": refresh.map(|d| d.as_secs_f64()),
        "mix": {"visit": mix[0], "scroll": mix[1], "search": mix[2], "return": mix[3]},
        "slo_p99_ms": slo_ms,
        "delivery_slo_p99_ms": delivery_slo_ms,
        "capacity_navigations_per_sec": capacity,
        "capacity_people": capacity_people,
        "stopped_early": stopped_early,
        "generator_lag": summary(&lag),
        "generator_cpus": std::thread::available_parallelism().map_or(0, |n| n.get()),
        "steps": steps,
        "errors": errors,
        "invalid": invalid,
        "missing_deliveries": missing,
        "rejected_subscriptions": rejected,
        "resubscribes": ctx.stats.resubscribes.load(Ordering::Relaxed),
        "reconnects": ctx.stats.reconnects.load(Ordering::Relaxed),
        "unfinished_viewers": unfinished,
        "unread_notices": ctx.stats.unread_notices.load(Ordering::Relaxed),
        "presence_refreshes": ctx.stats.refreshes.load(Ordering::Relaxed),
        "presence_subscribes": ctx.stats.presence_writes.load(Ordering::Relaxed),
        "acknowledged_posts": ctx.known.lock().unwrap().audit.len(),
        "unacknowledged_posts": ctx.known.lock().unwrap().unacknowledged.len(),
        "db_audit": db_audit,
        "invalid_samples": samples,
        "error_samples": ctx.stats.error_samples.lock().unwrap().iter().map(|(_, s)| s.clone()).collect::<Vec<_>>(),
    }))
}

fn sqlite_json(db: &str, query: &str) -> Res<Vec<Value>> {
    let out = std::process::Command::new("sqlite3").args(["-readonly", "-json", "-cmd", ".timeout 10000", db, query]).output()?;
    if !out.status.success() {
        return Err(format!("sqlite3: {}", String::from_utf8_lossy(&out.stderr)).into());
    }
    let text = String::from_utf8_lossy(&out.stdout);
    Ok(if text.trim().is_empty() { Vec::new() } else { serde_json::from_str(&text)? })
}

/// Every message the run created, read back from the database: acknowledged posts must be there once, in their
/// room, with their body; anything else must be one of the posts sent without an acknowledgement.
/// The audit summary, every (message id, room) the run created, and the failures.
type DbAudit = (Value, Vec<(u64, u64)>, Vec<String>);

fn audit_db(db: &str, ctx: &Ctx) -> Res<DbAudit> {
    let plan_max = ctx.plan.rooms.values().flatten().copied().max().unwrap_or(0);
    let rows = sqlite_json(
        db,
        &format!(
            "SELECT m.id, m.room_id, coalesce(rt.body, '') AS body FROM messages m LEFT JOIN action_text_rich_texts rt \
             ON rt.record_type = 'Message' AND rt.record_id = m.id AND rt.name = 'body' WHERE m.id > {plan_max} ORDER BY m.id"
        ),
    )?;
    let known = ctx.known.lock().unwrap();
    let mut failures = Vec::new();
    let mut committed = Vec::new();
    let mut by_id = HashMap::new();
    for r in &rows {
        let (id, room) = (r["id"].as_u64().unwrap_or(0), r["room_id"].as_u64().unwrap_or(0));
        by_id.insert(id, (room, r["body"].as_str().unwrap_or("").to_string()));
        committed.push((id, room));
    }
    for a in &known.audit {
        let (id, room, body) = (a["id"].as_u64().unwrap_or(0), a["room"].as_u64().unwrap_or(0), a["body"].as_str().unwrap_or(""));
        match by_id.get(&id) {
            Some((r, b)) if *r == room && b.contains(body) => {}
            Some((r, _)) => {
                failures.push(format!("audit: acknowledged message {id} is in room {r} or lost its body (expected room {room})"))
            }
            None => failures.push(format!("audit: acknowledged message {id} is not in the database")),
        }
    }
    let acknowledged: HashSet<u64> = known.audit.iter().filter_map(|a| a["id"].as_u64()).collect();
    let mut unacknowledged_committed = 0;
    for (id, (room, body)) in &by_id {
        if acknowledged.contains(id) {
            continue;
        }
        if known.unacknowledged.iter().any(|(r, b)| r == room && body.contains(b.as_str())) {
            unacknowledged_committed += 1;
        } else {
            failures.push(format!("audit: message {id} in room {room} was created by nothing the run sent"));
        }
    }
    let value = json!({
        "messages_created": rows.len(),
        "acknowledged": known.audit.len(),
        "committed_without_acknowledgement": unacknowledged_committed,
        "sent_without_acknowledgement": known.unacknowledged.len(),
    });
    Ok((value, committed, failures))
}

/// This process's CPU time so far, user plus system across all threads, from /proc/self/stat (Linux; 0 elsewhere).
/// Reported per step so a reader can see how much of its cores the generator itself used.
fn own_cpu_secs() -> f64 {
    let Ok(stat) = std::fs::read_to_string("/proc/self/stat") else { return 0.0 };
    // Fields after the parenthesized command name: utime and stime are the 12th and 13th (fields 14 and 15).
    let Some(rest) = stat.rfind(')').map(|i| &stat[i + 2..]) else { return 0.0 };
    let fields: Vec<&str> = rest.split_whitespace().collect();
    let ticks: f64 = [11, 12].iter().filter_map(|&i| fields.get(i)?.parse::<f64>().ok()).sum();
    ticks / 100.0 // USER_HZ, 100 on Linux
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resp(headers: &[(&str, &str)], body: &str) -> Resp {
        let mut map = hyper::HeaderMap::new();
        for (k, v) in headers {
            map.append(hyper::header::HeaderName::from_bytes(k.as_bytes()).unwrap(), v.parse().unwrap());
        }
        Resp { status: 200, headers: map, body: Bytes::from(body.to_string()) }
    }

    #[test]
    fn server_timing_tallies_page_and_fragment_reuse() {
        let mut route = Route::default();
        tally_server_timing(
            &resp(&[("server-timing", r#"page;desc="miss", records;desc="hits=2 misses=0", fragments;desc="hits=38 misses=2""#)], ""),
            &mut route,
        );
        tally_server_timing(&resp(&[("server-timing", r#"page;desc="hit""#)], ""), &mut route);
        tally_server_timing(&resp(&[("server-timing", "db;dur=3.2")], ""), &mut route);
        assert_eq!((route.page_hit, route.page_miss, route.frag_hits, route.frag_misses), (1, 1, 38, 2));
        assert_eq!(route.json()["page_cache"], json!({"hit": 1, "miss": 1}));
    }

    #[test]
    fn pages_name_their_viewer_and_messages() {
        let html = r#"<meta name="current-user-id" content="42" /><div data-message-id="7"></div><div data-message-id="9"></div>"#;
        assert_eq!(page_user(html), Some(42));
        assert_eq!(message_ids(html), vec![7, 9]);
        assert_eq!(page_user("<html></html>"), None);
    }

    #[test]
    fn gzip_bodies_are_decoded_before_checks() {
        use std::io::Write;
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        gz.write_all(b"<p>hello</p>").unwrap();
        let mut r = resp(&[("content-encoding", "gzip")], "");
        r.body = Bytes::from(gz.finish().unwrap());
        assert_eq!(decode(&r).unwrap(), "<p>hello</p>");
        r.body = Bytes::from_static(b"not gzip");
        assert!(decode(&r).is_err());
    }

    #[test]
    fn generator_choices_are_reproducible_and_weighted() {
        let (mut a, mut b) = (Rng(7), Rng(7));
        assert!((0..100).all(|_| a.next() == b.next()));
        let mut r = Rng(11);
        let mut counts = [0; 2];
        for _ in 0..10_000 {
            counts[r.weighted(&[1.0, 3.0])] += 1;
        }
        assert!((2_200..2_800).contains(&counts[0]), "{counts:?}");
        let mean = (0..10_000).map(|_| r.exp(10.0).as_secs_f64()).sum::<f64>() / 10_000.0;
        assert!((0.09..0.11).contains(&mean), "{mean}");
    }

    #[test]
    fn plans_need_cookies_and_rooms() {
        let dir = std::env::temp_dir().join(format!("campfire-users-plan-{}", nonce()));
        std::fs::create_dir(&dir).unwrap();
        let good = dir.join("good.json");
        std::fs::write(
            &good,
            r#"{"users":[{"id":1,"cookie":"session_token=x","rooms":[1,2]}],"rooms":{"1":[10,11],"2":[12]},"terms":["coffee"]}"#,
        )
        .unwrap();
        let plan = load_plan(good.to_str().unwrap()).unwrap();
        assert_eq!((plan.users.len(), plan.rooms[&1].clone(), plan.terms.clone()), (1, vec![10, 11], vec!["coffee".to_string()]));
        let bad = dir.join("bad.json");
        std::fs::write(&bad, r#"{"users":[{"id":1,"cookie":"","rooms":[]}],"rooms":{}}"#).unwrap();
        assert!(load_plan(bad.to_str().unwrap()).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
