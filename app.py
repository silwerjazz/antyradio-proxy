import asyncio
import re
import time
import httpx
from bs4 import BeautifulSoup
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
    "Cache-Control": "no-cache, no-store",
    "Pragma": "no-cache",
}

ANTYRADIO_LOGO = "https://gfx.antyradio.pl/design/antyradio/images/logo.png"
META_INTERVAL = 16000
current_song = "Antyradio"
last_updated = "Nigdy"
active_stream_url = STREAM_URLS[0]


def clean_artist_title(raw_track: str) -> str:
    """Formatuje wykonawcę i tytuł tak, aby Replaio pobierało okładki płyt."""
    # Usuń dopiski typu (AC), (Radio Edit) itp.
    cleaned = re.sub(r"\s*\([^)]*\)", "", raw_track).strip()
    if " - " in cleaned:
        artist, title = cleaned.split(" - ", 1)
        artist, title = artist.strip(), title.strip()
        # Zamień CAPS LOCK (np. METALLICA) na Metallica
        if artist.isupper() and len(artist) > 3:
            artist = artist.title()
        if title.isupper() and len(title) > 3:
            title = title.title()
        return f"{artist} - {title}"
    return cleaned


async def fetch_rds_data(client: httpx.AsyncClient):
    global current_song, last_updated
    ts = int(time.time())

    # Źródło 1: Bezpośrednie API Live JSON z OnlineRadioBox (odświeżane w czasie rzeczywistym)
    try:
        resp = await client.get(
            f"https://scraper2.onlineradiobox.com/pl.antyradio94?l=0&_t={ts}",
            headers=HEADERS,
        )
        if resp.status_code == 200:
            data = resp.json()
            title = data.get("title", "").strip()
            if title and "antyradio" not in title.lower():
                current_song = clean_artist_title(title)
                last_updated = f"API ORB ({time.strftime('%H:%M:%S')})"
                return
    except Exception:
        pass

    # Źródło 2 (Fallback): Parsowanie tabeli na żywo z ominięciem cache
    try:
        resp = await client.get(
            f"https://onlineradiobox.com/pl/antyradio94/playlist/?_t={ts}",
            headers=HEADERS,
        )
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            first_row = soup.select_one("table.tablelist-schedule tr td.track_history_item")
            if first_row:
                track = first_row.get_text(strip=True)
                if track and "antyradio" not in track.lower():
                    current_song = clean_artist_title(track)
                    last_updated = f"HTML ORB ({time.strftime('%H:%M:%S')})"
                    return
    except Exception:
        pass

    # Źródło 3 (Fallback 2): Małopolskie-Media Playlista Antyradia
    try:
        resp = await client.get(
            f"https://malopolskie-media.info/playlista/ogolnopolskie/antyradio.html?_t={ts}",
            headers=HEADERS,
        )
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            first_track = soup.select_one(".utwor, .playlist-item, tr td a")
            if first_track:
                track = first_track.get_text(strip=True)
                if track and " - " in track:
                    current_song = clean_artist_title(track)
                    last_updated = f"MM ({time.strftime('%H:%M:%S')})"
    except Exception:
        pass


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


async def update_metadata_loop():
    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True, verify=False) as client:
        await find_working_stream(client)
        while True:
            await fetch_rds_data(client)
            await asyncio.sleep(15)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(update_metadata_loop())


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
