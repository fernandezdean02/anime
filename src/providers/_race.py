import asyncio
import importlib
import time

from src.providers._media import build_ctx

RANKING = [
    "anineko", "anizone", "anikoto", "reanime", "aniwaves",
    "kaa", "anibd", "animegg", "mkissa", "animeonsen",
]

# ─── Timeout strategy ──────────────────────────────────────────────────
# Since all providers race in parallel, worst-case total = max(timeout).
# Good providers return in <10s; slower ones get more but capped.
WATCH_TIMEOUT        = 22.0   # was 75.0 — huge win, worst case now 22s not 75s
FAST_PROVIDER_TIMEOUT = 8.0   # proven-fast providers fail fast on regression
DRAIN_AFTER_WIN      = 1.0    # was 8.0 — just enough to record latency, not hunt for winner
EPISODES_TIMEOUT     = 30.0   # was 60.0 — merged_episodes waits for slowest
WATCH_CACHE_TTL      = 300.0  # was 45.0 — 5 min makes ep-switching back/forth instant

_latency: dict = {}
_ep_latency: dict = {}
_failures: dict = {}
_last_win: dict = {}    # anilistId -> (provider_name, timestamp)
_watch_cache: dict = {}


def _load(name):
    try:
        return importlib.import_module(f"src.providers.{name}")
    except Exception as e:
        print(f"[RACE] provider {name} unavailable: {e}")
        return None


def providers():
    mods = []
    for name in RANKING:
        mod = _load(name)
        if mod is not None:
            mods.append((name, mod))
    return mods


def record_latency(name: str, seconds: float, ok: bool, kind: str = "watch"):
    table = _latency if kind == "watch" else _ep_latency
    if ok:
        prev = table.get(name)
        table[name] = seconds if prev is None else round(0.7 * prev + 0.3 * seconds, 3)
        _failures[name] = 0
    else:
        _failures[name] = _failures.get(name, 0) + 1


def _provider_timeout(name: str) -> float:
    """Adaptive per-provider timeout. Fast providers get shorter timeouts;
    providers with a streak of failures get even shorter leashes so they
    stop dragging the race out."""
    fails = _failures.get(name, 0)
    if fails >= 3:
        return FAST_PROVIDER_TIMEOUT
    avg = _latency.get(name)
    if avg is None:
        return WATCH_TIMEOUT
    return max(FAST_PROVIDER_TIMEOUT, min(WATCH_TIMEOUT, avg * 4))


def default_provider():
    if not _latency:
        return RANKING[0]
    return sorted(_latency.items(), key=lambda kv: kv[1])[0][0]


def latency_table() -> dict:
    return {name: {"avg_seconds": _latency.get(name),
                   "episodes_seconds": _ep_latency.get(name),
                   "failures": _failures.get(name, 0),
                   "effective_timeout": round(_provider_timeout(name), 1)}
            for name in RANKING}


async def _watch_one(mod, name, anilist_id, ep, audio, ctx, timeout):
    key = (name, anilist_id, ep, audio)
    hit = _watch_cache.get(key)
    if hit and hit[0] > time.time():
        return name, hit[1], 0.0, True
    t0 = time.time()
    try:
        streams = await asyncio.wait_for(mod.watch(anilist_id, audio, ep, ctx), timeout)
        dt = round(time.time() - t0, 3)
        if streams:
            _watch_cache[key] = (time.time() + WATCH_CACHE_TTL, streams)
            return name, streams, dt, True
        return name, None, dt, False
    except asyncio.TimeoutError:
        return name, None, round(time.time() - t0, 3), False
    except Exception:
        return name, None, round(time.time() - t0, 3), False


async def race_watch(anilist_id: int, ep: int, audio: str = "sub",
                     preferred: str | None = None) -> dict | None:
    mods = providers()
    if not mods:
        return None
    ctx = await build_ctx(anilist_id)

    # Priority ordering only affects which task starts first (microseconds
    # matter for cancellation order, not for who wins). The real speedup
    # comes from racing EVERYONE simultaneously, not sequencing.
    last_win = _last_win.get(anilist_id, (None, 0))[0]
    priority = []
    if preferred:
        priority.append(preferred)
    if last_win and last_win not in priority:
        priority.append(last_win)

    ordered = []
    seen = set()
    for pname in priority:
        pick = next((m for m in mods if m[0] == pname), None)
        if pick is not None:
            ordered.append(pick)
            seen.add(pname)
    for m in mods:
        if m[0] not in seen:
            ordered.append(m)

    pending = {
        asyncio.ensure_future(
            _watch_one(mod, name, anilist_id, ep, audio, ctx, _provider_timeout(name))
        )
        for name, mod in ordered
    }
    timings: dict = {}
    winner = None
    try:
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                try:
                    name, streams, dt, ok = task.result()
                except Exception:
                    continue
                timings[name] = {"seconds": dt, "ok": ok}
                record_latency(name, dt, ok)
                if ok and winner is None:
                    winner = {"provider": name, "streams": streams}
                    _last_win[anilist_id] = (name, time.time())
            if winner is not None:
                break
    finally:
        if pending:
            try:
                done2, pending2 = await asyncio.wait(pending, timeout=DRAIN_AFTER_WIN)
                for task in done2:
                    try:
                        name, streams, dt, ok = task.result()
                        timings[name] = {"seconds": dt, "ok": ok}
                        record_latency(name, dt, ok)
                    except Exception:
                        pass
                pending = pending2
            finally:
                for task in pending:
                    task.cancel()
    if winner is None:
        return None
    winner["timings"] = timings
    winner["defaultProvider"] = default_provider()
    if preferred:
        winner["requestedProvider"] = preferred
    return winner


async def _episodes_one(mod, name, anilist_id, ctx):
    t0 = time.time()
    try:
        data = await asyncio.wait_for(mod.get_episodes(anilist_id, ctx), EPISODES_TIMEOUT)
        eps = (data.get("episodes") or {})
        count = sum(len(v) for v in eps.values() if isinstance(v, list))
        record_latency(name, time.time() - t0, count > 0, kind="episodes")
        return name, data if count > 0 else None
    except Exception as e:
        print(f"[RACE] {name} episodes failed: {str(e)[:120]}")
        record_latency(name, time.time() - t0, False, kind="episodes")
        return name, None


async def merged_episodes(anilist_id: int) -> dict | None:
    mods = providers()
    if not mods:
        return None
    ctx = await build_ctx(anilist_id)
    results = await asyncio.gather(
        *[_episodes_one(mod, name, anilist_id, ctx) for name, mod in mods])
    merged = {name: data for name, data in results if data}
    if not merged:
        return None
    return {"anilistId": anilist_id, "source": "native", "providers": merged}


def merge_subtitles(streams: list) -> list:
    seen, out = set(), []
    for s in streams or []:
        for sub in s.get("subtitles") or []:
            url = sub.get("url")
            if url and url not in seen:
                seen.add(url)
                out.append(sub)
    return out


def providers_status() -> dict:
    return {
        "defaultProvider": default_provider(),
        "ranking": RANKING,
        "available": [name for name, _ in providers()],
        "latency": latency_table(),
    }
