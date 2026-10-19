import asyncio
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

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}

ANTYRADIO_LOGO = "https://gfx.antyradio.pl/design/antyradio/images/logo.png"
META_INTERVAL = 16000
# Jeśli serwer stoi za reverse proxy, ustaw np. PUBLIC_URL=http://192.168.1.10:8000
PUBLIC_URL = os.getenv("PUBLIC_URL", "").rstrip("/")

state = {
    "song": "Antyradio Warszawa",
    "artist": "",
    "title": "",
    "cover": ANTYRADIO_LOGO,
    "cover_version": 0,
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


async def fetch_track() -> tuple[str, str, str] | None:
    """Zwraca (artist, title, źródło) albo None."""
    ts = int(time.time())

    try:
        r = await http.get(f"https://api.allradios.pl/nowplaying/antyradio?_t={ts}", headers=HEADERS)
        if r.status_code == 200:
            track = (r.json().get("track") or "").strip()
            if " - " in track:
                a, t = track.split(" - ", 1)
                return (*clean_artist_title(a, t), "AllRadios")
        else:
            log.warning("AllRadios HTTP %s", r.status_code)
    except Exception as e:
        log.warning("AllRadios błąd: %r", e)

    try:
        r = await http.get(f"https://player.antyradio.pl/api/v1/now-playing?_t={ts}", headers=HEADERS)
        if r.status_code == 200:
            d = r.json()
            a, t = (d.get("artist") or "").strip(), (d.get("title") or "").strip()
            if a and t:
                return (*clean_artist_title(a, t), "Antyradio Player")
        else:
            log.warning("Antyradio Player HTTP %s", r.status_code)
    except Exception as e:
        log.warning("Antyradio Player błąd: %r", e)

    return None


async def fetch_cover(artist: str, title: str) -> str:
    """Szuka okładki w iTunes Search API; fallback na logo stacji."""
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
        log.warning("Okładka błąd: %r", e)
    return ANTYRADIO_LOGO


async def refresh_metadata():
    result = await fetch_track()
    if not result:
        return
    artist, title, source = result
    song = f"{artist} - {title}"
    state["last_updated"] = f"{source} ({time.strftime('%H:%M:%S')})"
    if song != state["song"]:
        state.update(song=song, artist=artist, title=title)
        state["cover"] = await fetch_cover(artist, title)
        state["cover_version"] += 1
        log.info("Teraz gra: %s | okładka: %s", song, state["cover"])


async def update_metadata_loop():
    while True:
        try:
            await refresh_metadata()
        except Exception as e:
            log.exception("Błąd pętli metadanych: %r", e)
        await asyncio.sleep(15)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http
    http = httpx.AsyncClient(timeout=5.0, follow_redirects=True)
    task = asyncio.create_task(update_metadata_loop())
    yield
    task.cancel()
    await http.aclose()


app = FastAPI(lifespan=lifespan)


def cover_url(request: Request) -> str:
    base = PUBLIC_URL or str(request.base_url).rstrip("/")
    return f"{base}/cover.jpg?v={state['cover_version']}"


def build_icy_metadata(title: str, url: str) -> bytes:
    def esc(s: str) -> str:
        return s.replace("'", "").replace(";", "")

    meta = f"StreamTitle='{esc(title)[:150]}';StreamUrl='{esc(url)}';".encode("utf-8", errors="replace")
    if len(meta) > 255 * 16:  # limit protokołu
        meta = f"StreamTitle='{esc(title)[:150]}';".encode("utf-8", errors="replace")
    blocks = (len(meta) + 15) // 16
    return bytes([blocks]) + meta.ljust(blocks * 16, b"\x00")


async def open_upstream(client: httpx.AsyncClient) -> httpx.Response:
    # najpierw ostatnio działający, potem reszta
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
        "last_updated": state["last_updated"],
        "stream": state["stream"],
    }


@app.get("/cover.jpg")
async def cover():
    return RedirectResponse(state["cover"], status_code=302)


def icy_headers(icy_requested: bool) -> dict:
    h = {
        "Content-Type": "audio/mpeg",
        "icy-name": "Antyradio",
        "icy-genre": "Rock",
        "icy-logo": ANTYRADIO_LOGO,
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
    return Response(status_code=200, headers=icy_headers(icy))


@app.get("/antyradio.mp3")
async def stream_audio(request: Request):
    icy_requested = request.headers.get("icy-metadata", "0").strip() == "1"

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

    return StreamingResponse(audio_generator(), media_type="audio/mpeg", headers=icy_headers(icy_requested))
