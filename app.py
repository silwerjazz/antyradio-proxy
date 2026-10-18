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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "*/*",
}

ANTYRADIO_LOGO = "https://gfx.antyradio.pl/design/antyradio/images/logo.png"
META_INTERVAL = 16000
current_song = "Antyradio Warszawa"
last_updated = "Nigdy"
active_stream_url = STREAM_URLS[0]


def clean_artist_title(raw_text: str) -> str:
    cleaned = re.sub(r"\s*\([^)]*\)", "", raw_text).strip()
    if " - " in cleaned:
        artist, title = cleaned.split(" - ", 1)
        artist, title = artist.strip(), title.strip()
        if artist.isupper() and len(artist) > 3:
            artist = artist.title()
        if title.isupper() and len(title) > 3:
            title = title.title()
        return f"{artist} - {title}"
    return cleaned


async def fetch_rds_data():
    global current_song, last_updated
    ts = int(time.time())

    async with httpx.AsyncClient(timeout=5.0, follow_redirects=True, verify=False) as client:
        # Oficjalne otwarte API OnlineRadioBox dla Antyradia
        try:
            resp = await client.get(f"https://scraper2.onlineradiobox.com/pl.antyradio94?l=0&_t={ts}", headers=HEADERS)
            if resp.status_code == 200:
                data = resp.json()
                title = data.get("title", "").strip()
                if title and "antyradio" not in title.lower():
                    current_song = clean_artist_title(title)
                    last_updated = f"ORB API ({time.strftime('%H:%M:%S')})"
                    return
        except Exception:
            pass

        # Zapasowe publiczne API radio
        try:
            resp = await client.get(f"https://api.allradios.pl/nowplaying/antyradio?_t={ts}", headers=HEADERS)
            if resp.status_code == 200:
                data = resp.json()
                track = data.get("track", "").strip()
                if track and " - " in track:
                    current_song = clean_artist_title(track.split(" - ", 1)[0], track.split(" - ", 1)[1])
                    last_updated = f"AllRadios ({time.strftime('%H:%M:%S')})"
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
            async with client.stream("GET", url, headers=HEADERS) as r:
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
            req = client.build_request("GET", target_url, headers=HEADERS)
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
