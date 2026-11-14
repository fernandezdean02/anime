"""Serve the anime API + local UI + stream proxy + VLC launcher.
Uses curl_cffi (browser TLS impersonation) for CDN bypass when available,
falls back to httpx when cffi fails (e.g. non-Cloudflare CDNs).

Run:  python serve.py

Env overrides:
  EXTRACT_TIMEOUT    default 50
  EPISODES_TIMEOUT   default 90
  INFO_TIMEOUT       default 20
  FORCE_HTTPX_MP4    set to 1 to stream mp4 via httpx instead of buffering via cffi
"""
import os
import re
import json
import time
import hashlib
import asyncio
import logging
import shutil
import subprocess
from urllib.parse import urljoin, quote

import httpx
from fastapi import Request
from fastapi.responses import Response, StreamingResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from api import app

# ═══════════════ curl_cffi (browser TLS impersonation) ═══════════════
try:
    from curl_cffi.requests import AsyncSession as CffiAsyncSession
    HAS_CFFI = True
except ImportError:
    HAS_CFFI = False
    CffiAsyncSession = None

log = logging.getLogger("uvicorn.error")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ═══════════════ shared httpx client (fallback) ═══════════════
_shared_client: httpx.AsyncClient | None = None

def get_http() -> httpx.AsyncClient:
    global _shared_client
    if _shared_client is None or _shared_client.is_closed:
        _shared_client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=8.0, read=30.0, write=10.0, pool=5.0),
            follow_redirects=True,
            http2=True,
            limits=httpx.Limits(
                max_connections=100,
                max_keepalive_connections=20,
                keepalive_expiry=15.0,
            ),
        )
    return _shared_client


# ═══════════════ curl_cffi session ═══════════════
_cffi_session: "CffiAsyncSession | None" = None
_cffi_lock = asyncio.Lock()

async def get_cffi():
    global _cffi_session
    if not HAS_CFFI:
        return None
    if _cffi_session is None:
        async with _cffi_lock:
            if _cffi_session is None:
                _cffi_session = CffiAsyncSession(max_clients=20)
    return _cffi_session


# ═══════════════ per-endpoint hard timeouts ═══════════════
TIMEOUTS = {
    "/anime/extract":  float(os.environ.get("EXTRACT_TIMEOUT",  "50")),
    "/anime/episodes": float(os.environ.get("EPISODES_TIMEOUT", "90")),
    "/anime/info":     float(os.environ.get("INFO_TIMEOUT",     "20")),
}

def _timeout_for(path: str) -> "float | None":
    for prefix, t in TIMEOUTS.items():
        if path.startswith(prefix):
            return t
    return None


# ═══════════════ persistent disk cache ═══════════════
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cache")
os.makedirs(CACHE_DIR, exist_ok=True)

CACHE_TTL = [
    ("/anime/extract",  300),
    ("/anime/episodes", 7200),
    ("/anime/info",     86400),
    ("/anime/",         600),
]
MAX_CACHE_BODY = 8_000_000
NEGATIVE_TTL = 60
MIN_CACHE_BODY = 4

def _ttl_for(path: str) -> int:
    for prefix, ttl in CACHE_TTL:
        if path.startswith(prefix):
            return ttl
    return 600

def _ttl_for_body(path: str, body: bytes) -> int:
    base = _ttl_for(path)
    if "/extract" in path or "/race" in path:
        try:
            data = json.loads(body)
            if isinstance(data, dict) and not data.get("streams"):
                return NEGATIVE_TTL
        except Exception:
            pass
    return base

def _cache_path(key: str) -> str:
    return os.path.join(CACHE_DIR, hashlib.md5(key.encode()).hexdigest() + ".json")

def _cache_get(key: str):
    p = _cache_path(key)
    if not os.path.exists(p):
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            rec = json.load(f)
        if rec.get("exp", 0) < time.time():
            try: os.remove(p)
            except: pass
            return None
        return rec
    except Exception:
        return None

def _cache_set(key, body, ct, status, ttl):
    if len(body) > MAX_CACHE_BODY or len(body) < MIN_CACHE_BODY:
        return
    try:
        rec = {"exp": time.time() + ttl, "ct": ct, "st": status,
               "body": body.decode("utf-8", errors="replace")}
        tmp = _cache_path(key) + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(rec, f)
        os.replace(tmp, _cache_path(key))
    except Exception as e:
        log.warning(f"cache write failed: {e}")


# ═══════════════ periodic client reset ═══════════════
_last_reset = [time.time()]

@app.middleware("http")
async def reset_http_periodically(request: Request, call_next):
    global _shared_client, _cffi_session, _last_reset
    if time.time() - _last_reset[0] > 1800:
        old_http = _shared_client
        old_cffi = _cffi_session
        _shared_client = None
        _cffi_session = None
        _last_reset[0] = time.time()
        log.info("[http-client] reset after 30 min")
        if old_http:
            try: asyncio.create_task(old_http.aclose())
            except: pass
        if old_cffi:
            try: asyncio.create_task(old_cffi.close())
            except: pass
    return await call_next(request)


# ═══════════════ middleware: timeout + cache + tagged log ═══════════════
@app.middleware("http")
async def cache_and_log(request: Request, call_next):
    path = request.url.path
    is_api = path.startswith("/anime/")
    t0 = time.time()

    qp = dict(request.query_params)
    tag = ""
    if "provider" in qp:
        tag += f" [provider={qp['provider']}]"
    if "deep" in qp:
        tag += " [deep]"
    if "direct" in qp:
        tag += " [direct]"

    if not is_api:
        resp = await call_next(request)
        if path.startswith("/p/"):
            log.info(f"{(time.time()-t0)*1000:7.0f}ms  {resp.status_code}  {path}{tag}")
        return resp

    bypass = "nocache=1" in request.url.query
    timeout = None if "deep" in qp else _timeout_for(path)

    if not bypass:
        key = f"{path}?{request.url.query}"
        hit = _cache_get(key)
        if hit:
            cached_prov = ""
            try:
                data = json.loads(hit["body"])
                if isinstance(data, dict):
                    if data.get("provider"):
                        cached_prov = f" [cached from {data['provider']}]"
                    elif isinstance(data.get("streams"), list):
                        ps = [s.get("provider") for s in data["streams"]
                              if isinstance(s, dict) and s.get("provider")]
                        if ps:
                            cached_prov = f" [cached from {','.join(ps[:3])}]"
            except Exception:
                pass
            log.info(f"{0:7.0f}ms  {hit['st']}  {path}{tag}{cached_prov}  [CACHE HIT]")
            return Response(content=hit["body"].encode("utf-8"),
                            status_code=hit["st"], media_type=hit["ct"],
                            headers={"X-Cache": "HIT"})

    try:
        if timeout:
            resp = await asyncio.wait_for(call_next(request), timeout=timeout)
        else:
            resp = await call_next(request)
    except asyncio.TimeoutError:
        dt = (time.time() - t0) * 1000
        log.warning(f"{dt:7.0f}ms  504  {path}{tag}  [TIMEOUT {timeout}s]")
        return Response(
            content=json.dumps({
                "error": f"Upstream took longer than {timeout:.0f}s",
                "timeout": True, "streams": [], "providers": {}, "results": [],
            }).encode(),
            status_code=504, media_type="application/json",
        )

    dt = (time.time() - t0) * 1000

    if resp.status_code == 200 and not bypass:
        try:
            body = getattr(resp, "body", b"") or b""
            if not body and hasattr(resp, "body_iterator"):
                chunks = []
                async for chunk in resp.body_iterator:
                    chunks.append(chunk)
                body = b"".join(chunks)
            if body:
                if "/extract" in path or "/race" in path:
                    try:
                        data = json.loads(body)
                        if isinstance(data, dict) and not isinstance(data.get("streams"), list):
                            data["streams"] = data.get("streams") or []
                            body = json.dumps(data).encode()
                            log.info(f"normalized null streams for {path}")
                    except json.JSONDecodeError:
                        pass
                ttl = _ttl_for_body(path, body)
                key = f"{path}?{request.url.query}"
                _cache_set(key, body,
                           resp.headers.get("content-type", "application/json"),
                           resp.status_code, ttl)
                tag_ttl = f"cached {ttl}s" if ttl != NEGATIVE_TTL else f"negative {ttl}s"
                log.info(f"{dt:7.0f}ms  {resp.status_code}  {path}{tag}  [MISS → {tag_ttl}]")
                return Response(content=body, status_code=resp.status_code,
                                media_type=resp.headers.get("content-type"),
                                headers={"X-Cache": "MISS"})
        except Exception as e:
            log.warning(f"cache write failed: {e}")

    log.info(f"{dt:7.0f}ms  {resp.status_code}  {path}{tag}")
    return resp


# ═══════════════ stream proxy helpers ═══════════════
UA = ("Mozilla/5.0 (Linux; Android 13; SM-G991B) "
      "AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/131.0.0.0 Mobile Safari/537.36")

FORCE_HTTPX_MP4 = os.environ.get("FORCE_HTTPX_MP4", "0") == "1"


def _origin(referer: str) -> str:
    p = referer.split("/", 3)
    return f"{p[0]}//{p[2]}" if len(p) >= 3 else referer


def _is_m3u8_url(u: str) -> bool:
    path = u.split("?", 1)[0].lower()
    return path.endswith(".m3u8") or "m3u8" in path


def _is_mp4_url(u: str) -> bool:
    path = u.split("?", 1)[0].lower()
    return path.endswith(".mp4") or path.endswith(".m4v")


def _looks_like_mp4(data: bytes) -> bool:
    if len(data) < 12: return False
    return data[4:8] in (b'ftyp', b'moov', b'mdat', b'free',
                         b'skip', b'wide', b'styp')


def _is_ass_url(u: str) -> bool:
    path = u.split("?", 1)[0].lower()
    return path.endswith(".ass") or path.endswith(".ssa")


def _ass_to_vtt(ass_text: str) -> str:
    raw = ass_text.replace("\r\n", "\n").replace("\r", "\n")
    lines = raw.split("\n")
    fields = ["Layer","Start","End","Style","Name","MarginL","MarginR","MarginV","Effect","Text"]
    in_events = False
    events: "list[str]" = []
    for line in lines:
        s = line.strip()
        if s == "[Events]": in_events = True; continue
        if in_events and s.startswith("Format:"):
            fields = [f.strip() for f in s[len("Format:"):].split(",")]; continue
        if in_events and s.startswith("Dialogue:"):
            events.append(s[len("Dialogue:"):].strip())
    try:
        start_idx = fields.index("Start"); end_idx = fields.index("End"); text_idx = fields.index("Text")
    except ValueError:
        start_idx, end_idx, text_idx = 1, 2, len(fields) - 1
    _time_re = re.compile(r"^(\d+):(\d{2}):(\d{2})[.:](\d{1,3})$")
    def parse_time(t):
        m = _time_re.match(t.strip())
        if not m: return None
        h, mm, ss, frac = m.groups()
        frac = frac.ljust(3, "0")[:3]
        return f"{int(h):02d}:{mm}:{ss}.{frac}"
    _tag_re = re.compile(r"\{[^}]*\}")
    def clean_text(t):
        t = _tag_re.sub("", t)
        t = t.replace("\\N", "\n").replace("\\n", "\n").replace("\\h", " ")
        t = t.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
        return "\n".join(l for l in t.split("\n") if l.strip()).strip()
    out = ["WEBVTT", ""]
    for ev in events:
        if re.search(r"\{\\p[1-9]", ev): continue
        parts = ev.split(",", len(fields) - 1)
        if len(parts) <= max(start_idx, end_idx, text_idx): continue
        start = parse_time(parts[start_idx]); end = parse_time(parts[end_idx])
        if not start or not end: continue
        text = clean_text(parts[text_idx])
        if not text: continue
        out.append(f"{start} --> {end}"); out.append(text); out.append("")
    return "\n".join(out)


# ═══════════════ browser-fingerprint headers ═══════════════
def _build_cdn_headers(referer, range_hdr=None):
    h = {
        "User-Agent": UA,
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Sec-Fetch-Dest": "video",
        "Sec-Fetch-Mode": "no-cors",
        "Sec-Fetch-Site": "cross-site",
        "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "Sec-Ch-Ua-Mobile": "?1",
        "Sec-Ch-Ua-Platform": '"Android"',
        "Priority": "u=1, i",
    }
    if referer:
        h["Referer"] = referer
        h["Origin"] = _origin(referer)
    if range_hdr:
        h["Range"] = range_hdr
    return h


def _is_good_media_resp(resp) -> bool:
    if resp is None or resp.status_code >= 400:
        return False
    ct = (resp.headers.get("content-type") or "").lower()
    return (
        ct.startswith("video/")
        or ct.startswith("audio/")
        or ct.startswith("application/octet-stream")
        or ct.startswith("application/vnd.apple.mpegurl")
        or ct.startswith("application/x-mpegurl")
        or "mpegurl" in ct
    )


CFFI_STRATEGIES = ["chrome131", "chrome120", "safari17_0", "chrome124"]


async def _fetch_media_cffi(url, referer, range_hdr=None, overall_timeout=20.0):
    """Concurrent cffi fetch. First successful strategy wins."""
    session = await get_cffi()
    if session is None:
        return None, None, 0, "curl_cffi not installed", {}

    async def try_strategy(imp):
        try:
            r = await session.get(
                url,
                headers=_build_cdn_headers(referer, range_hdr),
                impersonate=imp,
                timeout=15,
                allow_redirects=True,
            )
            if _is_good_media_resp(r):
                hdrs = {}
                for h in ("content-length", "content-range",
                          "accept-ranges", "cache-control"):
                    v = r.headers.get(h)
                    if v: hdrs[h] = v
                return (r.content,
                        r.headers.get("content-type", "application/vnd.apple.mpegurl"),
                        r.status_code, imp, hdrs)
            ct = (r.headers.get("content-type") or "")[:40]
            snip = (r.content[:120].decode("utf-8", "replace").replace("\n", " ")
                    if r.content else "")
            log.info(f"[cffi] {imp} → {r.status_code} ct={ct!r} body={snip!r}")
        except Exception as e:
            log.info(f"[cffi] {imp} err: {e}")
        return None

    tasks = [asyncio.create_task(try_strategy(imp)) for imp in CFFI_STRATEGIES]
    try:
        loop = asyncio.get_event_loop()
        deadline = loop.time() + overall_timeout
        pending = set(tasks)
        while pending:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            done, pending = await asyncio.wait(
                pending, timeout=remaining, return_when=asyncio.FIRST_COMPLETED)
            if not done:
                break
            for t in done:
                try:
                    r = t.result()
                except Exception:
                    r = None
                if r is not None:
                    return r
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()
        for t in tasks:
            try: await t
            except BaseException: pass

    return None, None, 0, "all strategies failed", {}


async def _fetch_small(url: str, referer: "str | None"):
    headers = {"User-Agent": UA, "Accept": "*/*"}
    if referer:
        headers["Referer"] = referer
        headers["Origin"] = _origin(referer)
    return await get_http().get(url, headers=headers)


def _rewrite_m3u8(text: str, base: str, referer: "str | None",
                  request_base: "str | None" = None) -> str:
    ref_q = f"&referer={quote(referer, safe='')}" if referer else ""
    abs_q = "&abs=1" if request_base else ""

    def wrap(u: str) -> str:
        absu = urljoin(base, u)
        path = "/p/m3u8" if _is_m3u8_url(absu) else "/p/seg"
        rel = f"{path}?url={quote(absu, safe='')}{ref_q}{abs_q}"
        return f"{request_base}{rel}" if request_base else rel

    out = []
    for line in text.splitlines():
        s = line.strip()
        if not s:
            out.append(line); continue
        if s.startswith("#"):
            if 'URI="' in s:
                s = re.sub(r'URI="([^"]+)"', lambda m: f'URI="{wrap(m.group(1))}"', s)
            out.append(s); continue
        out.append(wrap(s))
    return "\n".join(out)


# ═══════════════ /p/m3u8 ═══════════════
@app.get("/p/m3u8")
async def proxy_m3u8(request: Request, url: str,
                     referer: "str | None" = None,
                     direct: int = 0, abs: int = 0):
    if direct:
        log.info("/p/m3u8 DIRECT redirect")
        return RedirectResponse(url=url, status_code=302)

    request_base = str(request.base_url).rstrip("/") if abs else None

    if HAS_CFFI:
        body, ct, status, label, _hdrs = await _fetch_media_cffi(url, referer)
        if body is not None:
            return Response(
                content=_rewrite_m3u8(body.decode("utf-8", "replace"),
                                      url, referer, request_base),
                media_type="application/vnd.apple.mpegurl",
            )
        log.warning(f"/p/m3u8 cffi failed ({label}), trying httpx")

    try:
        r = await _fetch_small(url, referer)
    except Exception as e:
        return Response(content=f"upstream error: {e}".encode(),
                        status_code=502, media_type="text/plain")
    if r.status_code != 200:
        return Response(content=r.content, status_code=r.status_code,
                        media_type=r.headers.get("content-type", "text/plain"))
    return Response(content=_rewrite_m3u8(r.text, url, referer, request_base),
                    media_type="application/vnd.apple.mpegurl")


# ═══════════════ /p/seg ═══════════════
@app.get("/p/seg")
async def proxy_seg(request: Request, url: str,
                    referer: "str | None" = None,
                    direct: int = 0, abs: int = 0):
    if direct:
        log.info("/p/seg DIRECT redirect")
        return RedirectResponse(url=url, status_code=302)

    if _is_m3u8_url(url):
        return await proxy_m3u8(request, url, referer, direct=direct, abs=abs)

    if _is_ass_url(url):
        try:
            r = await _fetch_small(url, referer)
        except Exception as e:
            return Response(content=f"upstream error: {e}".encode(),
                            status_code=502, media_type="text/plain")
        if r.status_code != 200:
            return Response(content=r.content, status_code=r.status_code,
                            media_type=r.headers.get("content-type", "text/plain"))
        try:
            text = r.content.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = r.content.decode("cp1252", errors="replace")
        if not text.strip() or text.lstrip().startswith("<"):
            return Response(content=r.content, status_code=200,
                            media_type="text/plain; charset=utf-8")
        try:
            vtt = _ass_to_vtt(text)
            cues = max(0, vtt.count("-->"))
            log.info(f"ass→vtt {cues} cues")
            return Response(content=vtt.encode("utf-8"),
                            media_type="text/vtt; charset=utf-8",
                            headers={"Cache-Control": "public, max-age=3600"})
        except Exception as e:
            log.warning(f"ass→vtt failed: {e}")
            return Response(content=r.content, status_code=200,
                            media_type="text/plain; charset=utf-8")

    range_hdr = request.headers.get("range")
    is_mp4 = _is_mp4_url(url)
    use_cffi = HAS_CFFI and not (FORCE_HTTPX_MP4 and is_mp4)

    if use_cffi:
        body, ct, status, label, hdrs = await _fetch_media_cffi(url, referer, range_hdr)
        if body is not None:
            if is_mp4 and not range_hdr and not _looks_like_mp4(body[:16384]):
                log.warning(f"non-MP4 at .mp4 URL via cffi: {url[:100]}")
                return Response(content=b"upstream returned non-MP4",
                                status_code=502, media_type="text/plain")
            if is_mp4 and len(body) > 100 * 1024 * 1024:
                log.warning(f"/p/seg buffered {len(body)//(1024*1024)}MB via cffi for {url[:80]}")
            out_hdrs = {"Accept-Ranges": "bytes", "Cache-Control": "public, max-age=300"}
            for h in ("content-length", "content-range"):
                if h in hdrs:
                    out_hdrs[h.title()] = hdrs[h]
            return Response(content=body, status_code=status,
                            media_type=ct or "application/octet-stream",
                            headers=out_hdrs)
        log.warning(f"/p/seg cffi failed ({label}), trying httpx")

    fwd_headers = {"User-Agent": UA, "Accept": "*/*"}
    if referer:
        fwd_headers["Referer"] = referer
        fwd_headers["Origin"] = _origin(referer)
    if range_hdr:
        fwd_headers["Range"] = range_hdr

    client = get_http()
    try:
        req = client.build_request("GET", url, headers=fwd_headers)
        upstream = await client.send(req, stream=True)
    except Exception as e:
        return Response(content=f"upstream error: {e}".encode(),
                        status_code=502, media_type="text/plain")

    ct = (upstream.headers.get("content-type") or "").lower()

    if "mpegurl" in ct:
        body = await upstream.aread(); await upstream.aclose()
        text = body.decode("utf-8", errors="replace")
        request_base = str(request.base_url).rstrip("/") if abs else None
        return Response(content=_rewrite_m3u8(text, url, referer, request_base),
                        media_type="application/vnd.apple.mpegurl")

    if "text/html" in ct:
        await upstream.aclose()
        log.warning(f"HTML returned for media: {url[:100]}")
        return Response(content=b"upstream returned HTML",
                        status_code=502, media_type="text/plain")

    if is_mp4 and not range_hdr:
        head_chunks = []
        head_total = 0
        async for chunk in upstream.aiter_raw(chunk_size=8192):
            head_chunks.append(chunk)
            head_total += len(chunk)
            if head_total >= 16384: break
        head = b"".join(head_chunks)
        if not _looks_like_mp4(head):
            await upstream.aclose()
            log.warning(f"non-MP4 at .mp4 URL: {url[:100]}")
            return Response(content=b"upstream returned non-MP4",
                            status_code=502, media_type="text/plain")

        out_headers = {}
        for h in ("content-type", "content-length", "content-range",
                  "accept-ranges", "cache-control"):
            v = upstream.headers.get(h)
            if v: out_headers[h.title()] = v

        async def mp4_body():
            try:
                yield head
                async for chunk in upstream.aiter_raw(chunk_size=65536):
                    yield chunk
            finally:
                await upstream.aclose()

        return StreamingResponse(mp4_body(), status_code=upstream.status_code,
                                 headers=out_headers)

    out_headers = {}
    for h in ("content-type", "content-length", "content-range",
              "accept-ranges", "cache-control"):
        v = upstream.headers.get(h)
        if v: out_headers[h.title()] = v

    async def body():
        try:
            async for chunk in upstream.aiter_raw(chunk_size=65536):
                yield chunk
        finally:
            await upstream.aclose()

    return StreamingResponse(body(), status_code=upstream.status_code,
                             headers=out_headers)


# ═══════════════ VLC launcher ═══════════════
def _launch_vlc(url: str, mime: str) -> "tuple[bool, str]":
    """Launch an Android app for a URL. Detects dead am-socket and gives
    an actionable error instead of a cryptic one."""
    am = shutil.which("termux-am") or shutil.which("am")
    if not am:
        return False, "termux-am / am not found — pkg install termux-am"
    last_err = ""
    for extra in (["-p", "org.videolan.vlc"], []):
        try:
            r = subprocess.run(
                [am, "start", "-a", "android.intent.action.VIEW",
                 "-d", url, "-t", mime] + extra,
                capture_output=True, timeout=8,
            )
            if r.returncode == 0 and not (r.stderr or b"").strip():
                return True, ""
            last_err = (r.stderr or r.stdout or b"").decode("utf-8", "replace").strip()
        except Exception as e:
            last_err = str(e)
    if "Could not connect to socket" in last_err or "No such file" in last_err:
        return False, (
            "Termux am socket server is disabled in this build. "
            "Fix: install Termux from GitHub (see backup/migrate steps), "
            "OR use 'Copy link' in the player and paste in VLC → New stream."
        )
    return False, last_err or "launch failed"


@app.get("/open")
async def open_vlc(request: Request, url: str,
                   referer: str = "", kind: str = "hls",
                   mode: str = "raw"):
    """Open a stream in VLC.

    mode=raw   → pass the CDN URL straight to VLC (works when CDN doesn't need Referer)
    mode=proxy → route through local proxy (needed when CDN requires Referer)
    """
    client = request.client.host if request.client else ""
    if client not in ("127.0.0.1", "::1", "localhost"):
        return Response(content=b"local only", status_code=403)

    if mode == "raw":
        target = url
        mime = "video/mp4" if (kind == "mp4" or _is_mp4_url(url)) else "application/x-mpegURL"
    else:
        base = str(request.base_url).rstrip("/")
        if kind == "mp4" or _is_mp4_url(url):
            mime = "video/mp4"
            target = f"{base}/p/seg?url={quote(url, safe='')}"
        else:
            mime = "application/x-mpegURL"
            target = f"{base}/p/m3u8?url={quote(url, safe='')}&abs=1"
        if referer:
            target += f"&referer={quote(referer, safe='')}"

    ok, err = _launch_vlc(target, mime)
    log.info(f"VLC open [{mode}] {mime}: {target[:120]} → {'ok' if ok else err}")
    if not ok:
        return Response(content=f"launch failed: {err}".encode(),
                        status_code=500, media_type="text/plain")
    return {"ok": True, "target": target, "mime": mime, "mode": mode}


# ═══════════════ server-side recent-URL history ═══════════════
RECENT_FILE = os.path.join(CACHE_DIR, "_recent.json")
RECENT_MAX = 50
_recent_lock = asyncio.Lock()

def _recent_load() -> list:
    try:
        with open(RECENT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def _recent_save(items: list):
    try:
        tmp = RECENT_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(items[:RECENT_MAX], f)
        os.replace(tmp, RECENT_FILE)
    except Exception as e:
        log.warning(f"recent save failed: {e}")

@app.get("/recent")
async def recent_list():
    return {"items": _recent_load()}

@app.post("/recent")
async def recent_add(request: Request):
    try:
        data = await request.json()
    except Exception:
        return Response(status_code=400, content=b"bad json")
    url = data.get("url", "")
    if not url:
        return Response(status_code=400, content=b"url required")
    entry = {
        "url": url,
        "referer": data.get("referer", ""),
        "provider": data.get("provider", ""),
        "animeId": data.get("animeId"),
        "ep": data.get("ep"),
        "kind": data.get("kind", "hls"),
        "ts": int(time.time()),
    }
    async with _recent_lock:
        items = [e for e in _recent_load() if e.get("url") != url]
        items.insert(0, entry)
        _recent_save(items)
    return {"ok": True, "count": len(items)}

@app.delete("/recent")
async def recent_clear():
    async with _recent_lock:
        _recent_save([])
    return {"ok": True}


# ═══════════════ health ═══════════════
@app.get("/health")
async def health():
    n_cache = 0
    try:
        n_cache = len([f for f in os.listdir(CACHE_DIR) if f.endswith(".json")])
    except Exception:
        pass
    return {
        "ok": True,
        "cffi": HAS_CFFI,
        "strategies": CFFI_STRATEGIES,
        "timeouts": TIMEOUTS,
        "cache_entries": n_cache,
        "recent_entries": len(_recent_load()),
        "force_httpx_mp4": FORCE_HTTPX_MP4,
    }


app.mount("/ui", StaticFiles(directory="static", html=True), name="static")

if not any(getattr(r, "path", None) == "/" for r in app.routes):
    @app.get("/")
    def _root():
        return RedirectResponse(url="/ui/")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
