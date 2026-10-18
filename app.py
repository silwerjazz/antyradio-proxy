import asyncio
import re
import time
import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse

app = FastAPI()

STREAM_URLS = [
    "https://an.cdn.eurozet.pl/ant-web.mp3",
    "https://an01.cdn.eurozet.pl/ant-web.mp3",
    "https://an02.cdn.eurozet.pl/ant-web.mp3",
]

HEADERS = {
    "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 11; Build/RQ3A.211001.001)",
    "Accept": "application/json",
}

ANTYRADIO_LOGO = "https://gfx.antyradio.pl/design/antyradio/images/logo.png"
META_INTERVAL = 16000
current_song = "Antyradio"
last_updated = "Nigdy"
active_stream_url = STREAM_URLS[0]


def clean_artist_title(artist: str, title: str) -> str:
    artist = re.sub(r"\s*\([^)]*\)", "", artist).strip()
    title = re.sub(r"\s*\([^)]*\)", "", title).strip()
    if artist.isupper() and len(artist) > 3:
        artist = artist.title()
    if title.isupper() and len(title) > 3:
        title = title.title()
    return f"{artist} - {title}"


async def fetch_rds_data():
    global current_song, last_updated
    ts = int(time.time())

    async with httpx.AsyncClient(timeout=5.0, follow_redirects=True, verify=False) as client:
        # Oficjalne mobilne API Eurozetu (zwraca zawsze aktualny utwór bez blokad)
        try:
            resp = await client.get(f"https://api.eurozet.pl/playlist/ant?_t={ts}", headers=HEADERS)
            if resp.status_code == 200:
                data = resp.json()
                # Szukamy aktualnie granego utworu w odpowiedzi JSON
                now = data.get("now", data.get("current", {}))
                artist = now.get("artist", "").strip()
                title = now.get("title", "").strip()
                if artist and title:
                    current_song = clean_artist_title(artist, title)
                    last_updated = f"Eurozet Mobile API ({time.strftime('%H:%M:%S')})"
                    return
        except Exception:
            pass

        # Awaryjne zapytanie do endpointu widgetu playera
        try:
            resp = await client.get(f"https://player.antyradio.pl/api/v1/now-playing?_t={ts}", headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code == 200:
                data = resp.json()
                artist = data.get("artist", "").strip()
                title = data.get("title", "").strip()
                if artist and title:
                    current_song = clean_artist_title(artist, title)
                    last_updated = f"Player API ({time.strftime('%H:%M:%S')})"
                    return
        except Exception:
            pass


async def update_metadata_loop():
    while True:
        try:
            await fetch_rds_data()
        except Exception:
            pass
        await asyncio.sleep(15)


async def find_working_stream(client: httpx.AsyncClient) -> str:
    global active_stream_url
    for url in STREAM_URLS:
        try:
            async with client.stream("GET", url, headers={"User-Agent": "Mozilla/5.0"}) as r:
                ct = r.headers.get("content-type", "").lower()
                if r.status_code == 200 and ("audio" in ct or "mpeg" in ct):
                    active_stream_url = url
                    return url
        except Exception:
            continue
    return STREAM_URLS[0]


@app.on_event("startup")
async def startup_event():
    async def initial_fetch():
        try:
            async with httpx.AsyncClient(timeout=5.0, verify=False) as client:
                await find_working_stream(client)
        except Exception:
            pass
        await fetch_rds_data()
        asyncio.create_task(update_metadata_loop())

    asyncio.create_task(initial_fetch())


def build_icy_metadata(title: str) -> bytes:
    clean_title = title[:100].replace("'", "").replace(";", "")
    meta_str = f"StreamTitle='{clean_title}';"
    meta_bytes = meta_str.encode("utf-8", errors="replace")
    length_blocks = (len(meta_bytes) + 15) // 16
    padded_meta = meta_bytes.ljust(length_blocks * 16, b"\x00")
    return bytes([length_blocks]) + padded_meta


@app.get("/")
@app.get("/debug")
def health_check():
    return {
        "status": "online",
        "now_playing": current_song,
        "last_updated": last_updated,
        "stream": active_stream_url,
    }


@app.head("/antyradio.mp3")
async def head_audio(request: Request):
    icy_requested = request.headers.get("icy-metadata", "0").strip() == "1"
    headers = {
        "Content-Type": "audio/mpeg",
        "icy-name": "Antyradio",
        "icy-genre": "Rock",
        "icy-logo": ANTYRADIO_LOGO,
        "icy-br": "128",
        "Accept-Ranges": "none",
        "Cache-Control": "no-cache, no-store",
    }
    if icy_requested:
        headers["icy-metaint"] = str(META_INTERVAL)
    return Response(status_code=200, headers=headers)


@app.get("/antyradio.mp3")
async def stream_audio(request: Request):
    icy_requested = request.headers.get("icy-metadata", "0").strip() == "1"

    async def audio_generator():
        buffer = bytearray()
        timeout = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0)
        client = httpx.AsyncClient(timeout=timeout, follow_redirects=True, verify=False)
        try:
            target_url = await find_working_stream(client)
            req = client.build_request("GET", target_url, headers={"User-Agent": "Mozilla/5.0"})
            resp = await client.send(req, stream=True)
            try:
                async for chunk in resp.aiter_bytes(chunk_size=4096):
                    if not chunk:
                        continue
                    if not icy_requested:
                        yield chunk
                    else:
                        buffer.extend(chunk)
                        while len(buffer) >= META_INTERVAL:
                            yield bytes(buffer[:META_INTERVAL])
                            del buffer[:META_INTERVAL]
                            yield build_icy_metadata(current_song)
            finally:
                await resp.aclose()
        except Exception:
            pass
        finally:
            await client.aclose()

    headers = {
        "Content-Type": "audio/mpeg",
        "icy-name": "Antyradio",
        "icy-genre": "Rock",
        "icy-logo": ANTYRADIO_LOGO,
        "icy-br": "128",
        "Accept-Ranges": "none",
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    if icy_requested:
        headers["icy-metaint"] = str(META_INTERVAL)

    return StreamingResponse(audio_generator(), media_type="audio/mpeg", headers=headers)
