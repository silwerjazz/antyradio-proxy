import asyncio
import json
import logging
import os
import re
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import RedirectResponse, StreamingResponse

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("antyradio")

STREAM_URLS = [
    "https://an.cdn.eurozet.pl/ant-web.mp3",
    "https://an01.cdn.eurozet.pl/ant-web.mp3",
    "https://an02.cdn.eurozet.pl/ant-web.mp3",
]
RDS_URL = "https://rds.eurozet.pl/reader/var/antyradio.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "*/*",
}

ANTYRADIO_LOGO = "https://gfx.antyradio.pl/design/antyradio/images/logo.png"
STATION_URL = "https://www.antyradio.pl"
META_INTERVAL = 16000
POLL_SECONDS = 10
# Jeśli serwer stoi za reverse proxy / w innej sieci niż odtwarzacz,
# ustaw np. PUBLIC_URL=http://192.168.1.10:8000
PUBLIC_URL = os.getenv("PUBLIC_URL", "").rstrip("/")

state = {
    "song": "Antyradio Warszawa",
    "artist": "",
    "title": "",
    "track_id": "",
    "cover": ANTYRADIO_LOGO,
    "cover_bytes": None,
    "cover_type": "image/jpeg",
    "cover_version": 0,
    "logo_bytes": None,
    "logo_type": "image/png",
    "last_updated": "Nigdy",
    "stream": STREAM_URLS[0],
}
http: httpx.AsyncClient | None = None


def clean_artist_title(artist: str, title: str) -> tuple[str, str]:
    artist = re.sub(r"\s*\([^)]*\)", "", artist).strip()
    title = re.sub(r"\s*\([^)]*\)", "", title).strip()
    if artist.isupper() and len(artist) > 3:
        artist = artist.title()
    if title.isupper() and len(title) > 3:
        title = title.title()
    return artist, title


def parse_rds(text: str) -> dict | None:
    """Obsługuje zarówno JSONP: rdsData({...}) jak i zwykły JSON."""
    m = re.search(r"\((.*)\)\s*;?\s*$", text.strip(), re.S)
    payload = m.group(1) if m else text
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        return None


async def fetch_rds() -> dict | None:
    r = await http.get(RDS_URL, params={"_t": int(time.time())}, headers=HEADERS)
    if r.status_code != 200:
        log.warning("RDS HTTP %s", r.status_code)
        return None
    data = parse_rds(r.text)
    if not data:
        log.warning("RDS: nie udało się sparsować: %r", r.text[:200])
        return None
    return data.get("now")


async def itunes_cover(artist: str, title: str) -> str:
    try:
        r = await http.get(
            "https://itunes.apple.com/search",
            params={"term": f"{artist} {title}", "entity": "song", "limit": 5},
            headers=HEADERS,
        )
        if r.status_code == 200:
            first_word = artist.lower().split()[0] if artist else ""
            for item in r.json().get("results", []):
                if first_word and first_word in item.get("artistName", "").lower():
                    art = item.get("artworkUrl100")
                    if art:
                        return art.replace("100x100bb", "600x600bb")
    except Exception as e:
        log.warning("iTunes błąd: %r", e)
    return ""


async def download_cover(url: str) -> tuple[bytes, str] | None:
    try:
        r = await http.get(url, headers=HEADERS)
        ct = r.headers.get("content-type", "image/jpeg").split(";")[0]
        if r.status_code == 200 and ct.startswith("image/") and r.content:
            return r.content, ct
        log.warning("Obrazek %s: HTTP %s, %s", url, r.status_code, ct)
    except Exception as e:
        log.warning("Pobieranie obrazka błąd: %r", e)
    return None


async def update_cover(artist: str, title: str, rds_img: str | None):
    candidates = []
    # 1) okładka od Eurozetu, jeśli to nie ogólny placeholder
    if rds_img and "eurozet.jpg" not in rds_img:
        candidates.append(rds_img)
    # 2) iTunes
    it = await itunes_cover(artist, title)
    if it:
        candidates.append(it)
    # 3) logo stacji
    candidates.append(ANTYRADIO_LOGO)

    for url in candidates:
        got = await download_cover(url)
        if got:
            state["cover"] = url
            state["cover_bytes"], state["cover_type"] = got
            return
    state["cover"] = ANTYRADIO_LOGO
    state["cover_bytes"] = None


async def refresh_metadata():
    now = await fetch_rds()
    if not now:
        return
    artist = (now.get("artist") or "").strip()
    title = (now.get("title") or "").strip()
    if not artist or not title:
        return
    artist, title = clean_artist_title(artist, title)
    track_id = str(now.get("id") or f"{artist}-{title}")
    state["last_updated"] = f"RDS Eurozet ({time.strftime('%H:%M:%S')})"
    if track_id == state["track_id"]:
        return
    state.update(song=f"{artist} - {title}", artist=artist, title=title, track_id=track_id)
    await update_cover(artist, title, now.get("img"))
    state["cover_version"] += 1
    log.info("Teraz gra: %s | okładka: %s", state["song"], state["cover"])


async def update_metadata_loop():
    while True:
        try:
            await refresh_metadata()
        except Exception as e:
            log.warning("Błąd pętli metadanych: %r", e)
        await asyncio.sleep(POLL_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http
    http = httpx.AsyncClient(timeout=8.0, follow_redirects=True)
    got = await download_cover(ANTYRADIO_LOGO)
    if got:
        state["logo_bytes"], state["logo_type"] = got
        log.info("Logo stacji pobrane (%s B)", len(got[0]))
    else:
        log.warning("Nie udało się pobrać logo stacji")
    task = asyncio.create_task(update_metadata_loop())
    yield
    task.cancel()
    await http.aclose()


app = FastAPI(lifespan=lifespan)


def base_url(request: Request) -> str:
    return PUBLIC_URL or str(request.base_url).rstrip("/")


def cover_url(request: Request) -> str:
    return f"{base_url(request)}/cover.jpg?v={state['cover_version']}"


def build_icy_metadata(title: str, url: str) -> bytes:
    def esc(s: str) -> str:
        return s.replace("'", "").replace(";", "")

    meta = f"StreamTitle='{esc(title)[:150]}';StreamUrl='{esc(url)}';".encode("utf-8", errors="replace")
    if len(meta) > 255 * 16:  # limit protokołu
        meta = f"StreamTitle='{esc(title)[:150]}';".encode("utf-8", errors="replace")
    blocks = (len(meta) + 15) // 16
    return bytes([blocks]) + meta.ljust(blocks * 16, b"\x00")


async def open_upstream(client: httpx.AsyncClient) -> httpx.Response:
    urls = [state["stream"]] + [u for u in STREAM_URLS if u != state["stream"]]
    for url in urls:
        try:
            req = client.build_request("GET", url, headers=HEADERS)
            resp = await client.send(req, stream=True)
            ct = resp.headers.get("content-type", "").lower()
            if resp.status_code == 200 and ("audio" in ct or "mpeg" in ct):
                state["stream"] = url
                return resp
            await resp.aclose()
        except Exception as e:
            log.warning("Stream %s nie działa: %r", url, e)
    raise RuntimeError("Żaden stream nie odpowiada")


@app.get("/")
@app.get("/debug")
def health_check():
    return {
        "status": "online",
        "now_playing": state["song"],
        "cover": state["cover"],
        "cover_version": state["cover_version"],
        "last_updated": state["last_updated"],
        "stream": state["stream"],
        "logo_loaded": state["logo_bytes"] is not None,
    }


@app.get("/cover.jpg")
async def cover():
    if state["cover_bytes"]:
        return Response(
            content=state["cover_bytes"],
            media_type=state["cover_type"],
            headers={"Cache-Control": "no-cache"},
        )
    return RedirectResponse(state["cover"], status_code=302)


@app.get("/logo.png")
async def logo():
    if state["logo_bytes"]:
        return Response(
            content=state["logo_bytes"],
            media_type=state["logo_type"],
            headers={"Cache-Control": "public, max-age=86400"},
        )
    return RedirectResponse(ANTYRADIO_LOGO, status_code=302)


def icy_headers(request: Request, icy_requested: bool) -> dict:
    h = {
        "Content-Type": "audio/mpeg",
        "icy-name": "Antyradio",
        "icy-genre": "Rock",
        "icy-url": STATION_URL,
        "icy-logo": f"{base_url(request)}/logo.png",
        "icy-br": "128",
        "Accept-Ranges": "none",
        "Cache-Control": "no-cache, no-store, must-revalidate",
    }
    if icy_requested:
        h["icy-metaint"] = str(META_INTERVAL)
    return h


@app.head("/antyradio.mp3")
async def head_audio(request: Request):
    icy = request.headers.get("icy-metadata", "0").strip() == "1"
    return Response(status_code=200, headers=icy_headers(request, icy))


@app.get("/antyradio.mp3")
async def stream_audio(request: Request):
    icy_requested = request.headers.get("icy-metadata", "0").strip() == "1"
    log.info("Nowy słuchacz, icy-metadata=%s, UA=%s", icy_requested, request.headers.get("user-agent"))

    async def audio_generator():
        buffer = bytearray()
        last_sent = None
        client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0),
            follow_redirects=True,
        )
        resp = None
        try:
            resp = await open_upstream(client)
            async for chunk in resp.aiter_bytes(chunk_size=4096):
                if not chunk:
                    continue
                if not icy_requested:
                    yield chunk
                    continue
                buffer.extend(chunk)
                while len(buffer) >= META_INTERVAL:
                    yield bytes(buffer[:META_INTERVAL])
                    del buffer[:META_INTERVAL]
                    snapshot = (state["song"], state["cover_version"])
                    if snapshot != last_sent:
                        yield build_icy_metadata(state["song"], cover_url(request))
                        last_sent = snapshot
                    else:
                        yield b"\x00"  # brak zmian
        except Exception as e:
            log.warning("Przerwany stream: %r", e)
        finally:
            if resp is not None:
                await resp.aclose()
            await client.aclose()

    return StreamingResponse(
        audio_generator(),
        media_type="audio/mpeg",
        headers=icy_headers(request, icy_requested),
    )
