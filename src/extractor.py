import asyncio
import hashlib
import json
import time

import httpx
from fastapi import HTTPException

from src.config import ANILIST_URL, iter_miruro_pipe_targets
from src.parser import encode_pipe_request, decode_pipe_response, deep_translate
from src.providers._http import _get_client

# ─── Caches ────────────────────────────────────────────────────────────
# AniList GraphQL responses — protects against hammering AniList when
# endpoints call anilist_query() directly (browsing/trending/popular).
_AL_CACHE: dict = {}       # md5(query|vars) -> (expires_at, data)
_AL_DEFAULT_TTL = 300.0    # 5 min for hot-path endpoint queries

# Raw episode lists from the Miruro pipe — same data every time for a given
# anilist ID, so a 10-min cache eliminates redundant network round-trips.
_PIPE_CACHE: dict = {}     # anilistId -> (expires_at, data)
_PIPE_TTL = 600.0


UA_ANILIST = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120 Safari/537.36"
)


async def anilist_query(query: str, variables: dict = None, ttl: float = None):
    """Query AniList GraphQL. Cached for `ttl` seconds (default 5 min).
    Uses the shared pooled HTTP/2 client — no fresh TLS handshake per call.
    """
    if ttl is None:
        ttl = _AL_DEFAULT_TTL

    vars_key = json.dumps(variables or {}, sort_keys=True)
    cache_key = hashlib.md5((query + "|" + vars_key).encode()).hexdigest()

    now = time.time()
    hit = _AL_CACHE.get(cache_key)
    if hit and hit[0] > now:
        return hit[1]

    body = {"query": query}
    if variables:
        body["variables"] = variables

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": UA_ANILIST,
        "Origin": "https://www.miruro.ru",
        "Referer": "https://www.miruro.ru/",
    }

    client = await _get_client()
    try:
        res = await client.post(ANILIST_URL, json=body, headers=headers, timeout=15.0)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"anilist query failed: {e}")
    if res.status_code != 200:
        raise HTTPException(status_code=500, detail="anilist query failed")
    data = res.json().get("data", {})
    _AL_CACHE[cache_key] = (now + ttl, data)
    return data


async def fetch_raw_episodes(anilist_id: int) -> dict:
    """Fetch raw episode payload via Miruro pipe. Races all mirrors in
    parallel; first successful decode wins; rest cancelled. Cached 10 min.
    """
    now = time.time()
    hit = _PIPE_CACHE.get(int(anilist_id))
    if hit and hit[0] > now:
        return hit[1]

    payload = {
        "path": "episodes",
        "method": "GET",
        "query": {"anilistId": anilist_id},
        "body": None,
        "version": "0.1.0",
    }
    encoded_req = encode_pipe_request(payload)
    client = await _get_client()

    targets = list(iter_miruro_pipe_targets(encoded_req))
    if not targets:
        raise HTTPException(status_code=502, detail="no pipe targets available")

    async def _try(target_tuple):
        pipe_target, headers = target_tuple
        try:
            res = await client.get(pipe_target, headers=headers, timeout=8.0)
            if res.status_code == 200:
                data = decode_pipe_response(res.text.strip())
                deep_translate(data)
                return data
            return None
        except Exception as e:
            print(f"[KUHI API] Failed {pipe_target}: {e}")
            return None

    # Race mirrors — first successful decode wins, others cancelled
    tasks = {asyncio.create_task(_try(t)): t for t in targets}
    winner = None
    try:
        while tasks:
            done, _pending = await asyncio.wait(tasks.keys(), return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                tasks.pop(task, None)
                try:
                    r = task.result()
                except Exception:
                    r = None
                if r is not None:
                    winner = r
                    break
            if winner is not None:
                break
    finally:
        for t in tasks:
            t.cancel()
        for t in tasks:
            try:
                await t
            except BaseException:
                pass

    if winner is None:
        raise HTTPException(status_code=502, detail="pipe request failed")

    _PIPE_CACHE[int(anilist_id)] = (now + _PIPE_TTL, winner)
    return winner
