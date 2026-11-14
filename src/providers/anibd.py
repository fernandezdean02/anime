import asyncio
import re
import time
from urllib.parse import quote, urlparse

from src.providers._http import fetch_json, fetch_text
from src.providers._match import episode_meta, expected_count
from src.providers._media import build_ctx

NAME = "anibd"
BASE = "https://epeng.animeapps.top"

# ─── Caches ────────────────────────────────────────────────────────────
# The /api2.php server list is stable per anilistId (episode catalog).
# Caching it saves a full round trip per episode request — both
# get_episodes() and find_episode_link() benefit.
_SERVERS_CACHE: dict = {}      # anilistId -> (expires_at, list)
_SERVERS_FUTURES: dict = {}    # anilistId -> in-flight task (dedupe concurrent)
_SERVERS_TTL = 300.0           # 5 min

_PLAYER_CACHE: dict = {}       # provider_link -> (expires_at, list)
_PLAYER_FUTURES: dict = {}
_PLAYER_TTL = 300.0

RESOLVE_TIMEOUT = 6.0          # per-server resolve; fail fast so we don't wait


async def _fetch_servers_uncached(anilist_id: int) -> list:
    data = await fetch_json(f"{BASE}/api2.php?epid={anilist_id}")
    result = data if isinstance(data, list) else []
    _SERVERS_CACHE[anilist_id] = (time.time() + _SERVERS_TTL, result)
    return result


async def fetch_servers(anilist_id: int) -> list:
    now = time.time()
    hit = _SERVERS_CACHE.get(anilist_id)
    if hit and hit[0] > now:
        return hit[1]
    # Dedupe concurrent calls: if get_episodes() and watch() race, only
    # one HTTP request is made and both await the same future.
    fut = _SERVERS_FUTURES.get(anilist_id)
    if fut and not fut.done():
        return await fut
    task = asyncio.create_task(_fetch_servers_uncached(anilist_id))
    _SERVERS_FUTURES[anilist_id] = task
    try:
        return await task
    finally:
        if _SERVERS_FUTURES.get(anilist_id) is task:
            _SERVERS_FUTURES.pop(anilist_id, None)


async def _fetch_player_links_uncached(provider_link: str) -> list:
    data = await fetch_json(f"{BASE}/apilink.php?data={quote(provider_link, safe="-_.!~*'()")}")
    result = data if isinstance(data, list) else []
    _PLAYER_CACHE[provider_link] = (time.time() + _PLAYER_TTL, result)
    return result


async def fetch_player_links(provider_link: str) -> list:
    now = time.time()
    hit = _PLAYER_CACHE.get(provider_link)
    if hit and hit[0] > now:
        return hit[1]
    fut = _PLAYER_FUTURES.get(provider_link)
    if fut and not fut.done():
        return await fut
    task = asyncio.create_task(_fetch_player_links_uncached(provider_link))
    _PLAYER_FUTURES[provider_link] = task
    try:
        return await task
    finally:
        if _PLAYER_FUTURES.get(provider_link) is task:
            _PLAYER_FUTURES.pop(provider_link, None)


def extract_video_url(html: str, origin: str) -> str | None:
    m = re.search(r'videoUrl\s*:\s*"([^"]+)"', html)
    if not m:
        return None
    raw = m.group(1)
    if re.match(r"^https?://", raw, re.IGNORECASE):
        return raw
    return f"{origin}{'' if raw.startswith('/') else '/'}{raw}"


async def _resolve_one(link: str) -> dict | None:
    """Resolve a single player link. Returns {hls, referer, link} or None."""
    try:
        parts = urlparse(link)
        origin = f"{parts.scheme}://{parts.netloc}"
        referer = f"{origin}/"
        html = await asyncio.wait_for(
            fetch_text(link, {"Referer": referer}),
            timeout=RESOLVE_TIMEOUT,
        )
        hls = extract_video_url(html, origin)
        if not hls:
            return None
        return {"hls": hls, "referer": referer, "link": link}
    except Exception:
        return None


def audio_from_server_name(name: str = "") -> str:
    return "dub" if re.search(r"dub", name or "", re.IGNORECASE) else "sub"


def _ep_number(value) -> int | float | None:
    try:
        n = float(value)
    except (TypeError, ValueError):
        return None
    if not n >= 1 or n == float("inf"):
        return None
    return int(n) if n.is_integer() else n


def _build_lists(anilist_id: int, groups: list, ctx: dict, expected) -> dict:
    sub, dub = [], []
    seen_sub, seen_dub = set(), set()
    for group in groups or []:
        group = group if isinstance(group, dict) else {}
        audio = audio_from_server_name(group.get("server_name") or "")
        bucket = dub if audio == "dub" else sub
        seen = seen_dub if audio == "dub" else seen_sub
        for ep in group.get("server_data") or []:
            ep = ep if isinstance(ep, dict) else {}
            raw = ep.get("name") if ep.get("name") is not None else ep.get("slug")
            number = _ep_number(raw)
            if number is None:
                continue
            if expected and number > expected:
                continue
            if number in seen:
                continue
            seen.add(number)
            meta = episode_meta(number, ctx)
            bucket.append({
                "id": f"watch/{NAME}/{anilist_id}/{audio}/{NAME}-{number}",
                "number": number,
                "title": meta["title"] or f"Episode {number}",
                "duration": meta["duration"],
                "filler": meta["filler"],
                "uncensored": meta["uncensored"],
                "description": meta["description"],
                "image": meta["image"],
                "airDate": meta["airDate"],
                "sourceNumber": number,
                "sourceLink": ep.get("link"),
                "audio": audio,
            })
    sub.sort(key=lambda e: e["number"])
    dub.sort(key=lambda e: e["number"])
    return {"sub": sub, "dub": dub}


async def find_episode_link(anilist_id: int, audio: str, ep: int):
    groups = await fetch_servers(anilist_id)
    for group in groups or []:
        group = group if isinstance(group, dict) else {}
        if audio_from_server_name(group.get("server_name") or "") != audio:
            continue
        for item in group.get("server_data") or []:
            item = item if isinstance(item, dict) else {}
            raw = item.get("name") if item.get("name") is not None else item.get("slug")
            try:
                if float(raw) == float(ep):
                    return item.get("link")
            except (TypeError, ValueError):
                continue
    return None


async def get_episodes(anilist_id: int, ctx: dict | None = None) -> dict:
    ctx = ctx or await build_ctx(anilist_id)
    media = ctx["media"]
    groups = await fetch_servers(anilist_id)
    if not groups:
        raise RuntimeError(f"anibd: no episodes found for AniList {anilist_id}")
    expected = expected_count(media, ctx.get("anizip"))
    titles = media.get("title") or {}
    return {
        "meta": {
            "id": str(anilist_id),
            "title": titles.get("english") or titles.get("romaji"),
            "source": NAME,
            "matchScore": 1,
            "numbering": "standard",
            "episodeOffset": 0,
        },
        "episodes": _build_lists(anilist_id, groups, ctx, expected),
    }


async def watch(anilist_id: int, audio: str, ep: int, ctx: dict | None = None) -> list:
    provider_link = await find_episode_link(anilist_id, audio, ep)
    if not provider_link:
        raise RuntimeError(f"anibd episode {ep} not found")

    servers = await fetch_player_links(provider_link)
    entries = [e for e in servers or [] if isinstance(e, dict) and e.get("link")]
    if not entries:
        return []

    # Race ALL player servers in parallel; take the FIRST successful HLS
    # resolution; cancel the rest. The frontend only ever plays the first
    # isActive stream, so resolving the others is wasted work.
    tasks = {
        asyncio.create_task(_resolve_one(e["link"])): e
        for e in entries
    }
    streams = []
    try:
        while tasks:
            done, _pending = await asyncio.wait(
                tasks.keys(), return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                entry = tasks.pop(task)
                try:
                    resolved = task.result()
                except Exception:
                    resolved = None
                if resolved:
                    streams.append({
                        "url": resolved["hls"],
                        "type": "hls",
                        "server": entry.get("server") or "AniBD",
                        "audio": audio,
                        "referer": resolved["referer"],
                        "priority": 5,
                        "isActive": len(streams) == 0,
                    })
            if streams:
                break   # got one → don't wait for the rest
    finally:
        # Cancel any still-running resolves; they'd just be thrown away
        for t in tasks:
            t.cancel()
        for t in tasks:
            try:
                await t
            except BaseException:
                pass
    return streams
