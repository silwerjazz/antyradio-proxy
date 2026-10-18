import asyncio
import json
import re
import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse

app = FastAPI()

# Działający strumień Antyradia
STREAM_URL = "https://an.cdn.eurozet.pl/ant-web.mp3"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Icy-MetaData": "0",
}

META_INTERVAL = 16000
current_song = "Antyradio"
current_cover = ""

# Zmienne diagnostyczne dla /debug
last_error = "Brak błędów"
total_bytes_sent = 0
last_client_headers = {}


async def fetch_rds_data(client: httpx.AsyncClient):
    global current_song, current_cover

    # 1. Próba pobrania tytułu i okładki z czytnika Eurozetu
    try:
        resp = await client.get("https://rds.eurozet.pl/reader/var/antyradio.json", headers=HEADERS)
        if resp.status_code == 200:
            match = re.search(r"\{.*\}", resp.text.strip(), re.DOTALL)
            if match:
                data = json.loads(match.group(0))
                now = data.get("now", {})
                artist = now.get("artist", "").strip()
                title = now.get("title", "").strip()
                cover = now.get("img", "") or now.get("image", "") or now.get("cover", "")
                if cover:
                    current_cover = cover
                if artist and title and "ANTYRADIO" not in artist.upper():
                    current_song = f"{artist} - {title}"
                    return
    except Exception:
        pass

    # 2. Fallback: OnlineRadioBox (tytuł)
    try:
        resp = await client.get(
            "https://onlineradiobox.com/pl/antyradio94/playlist/",
            headers=HEADERS,
        )
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            first_row = soup.select_one("table.tablelist-schedule tr td.track_history_item")
            if first_row:
                track = first_row.get_text(strip=True)
                if track and "Antyradio" not in track:
                    current_song = track
    except Exception:
        pass


async def update_metadata_loop():
    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True, verify=False) as client:
        while True:
            await fetch_rds_data(client)
            await asyncio.sleep(15)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(update_metadata_loop())


def build_icy_metadata(title: str, cover_url: str = "") -> bytes:
    clean_title = title[:100].replace("'", "").replace(";", "")
    if cover_url:
        meta_str = f"StreamTitle='{clean_title}';StreamUrl='{cover_url}';"
    else:
        meta_str = f"StreamTitle='{clean_title}';"
    meta_bytes = meta_str.encode("utf-8", errors="replace")
    length_blocks = (len(meta_bytes) + 15) // 16
    padded_meta = meta_bytes.ljust(length_blocks * 16, b"\x00")
    return bytes([length_blocks]) + padded_meta


@app.get("/")
def health_check():
    return {"status": "online", "now_playing": current_song}


@app.get("/debug")
async def debug_info():
    """Endpoint diagnostyczny pokazujący status strumienia i ewentualne błędy."""
    stream_test = "Nieznany"
    first_chunk_size = 0
    try:
        async with httpx.AsyncClient(timeout=5.0, follow_redirects=True, verify=False) as client:
            async with client.stream("GET", STREAM_URL, headers=HEADERS) as r:
                stream_test = f"HTTP {r.status_code} ({r.headers.get('content-type')})"
                async for chunk in r.aiter_bytes(chunk_size=1024):
                    first_chunk_size = len(chunk)
                    break
    except Exception as e:
        stream_test = f"Błąd: {repr(e)}"

    return {
        "now_playing": current_song,
        "cover_url": current_cover,
        "eurozet_stream_test": stream_test,
        "first_chunk_received_bytes": first_chunk_size,
        "total_bytes_sent_to_players": total_bytes_sent,
        "last_stream_error": last_error,
        "last_player_headers": last_client_headers,
    }


@app.head("/antyradio.mp3")
async def head_audio(request: Request):
    """Odpowiedź na zapytanie testowe HEAD z Replaio / VLC."""
    icy_requested = request.headers.get("icy-metadata", "0").strip() == "1"
    headers = {
        "Content-Type": "audio/mpeg",
        "icy-name": "Antyradio Warszawa (RDS)",
        "icy-genre": "Rock",
        "icy-br": "128",
        "Accept-Ranges": "none",
        "Cache-Control": "no-cache, no-store",
    }
    if icy_requested:
        headers["icy-metaint"] = str(META_INTERVAL)
    return Response(status_code=200, headers=headers)


@app.get("/antyradio.mp3")
async def stream_audio(request: Request):
    global last_error, total_bytes_sent, last_client_headers
    last_client_headers = dict(request.headers)
    icy_requested = request.headers.get("icy-metadata", "0").strip() == "1"

    async def audio_generator():
        global last_error, total_bytes_sent
        buffer = bytearray()
        timeout = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0)
        client = httpx.AsyncClient(timeout=timeout, follow_redirects=True, verify=False, http2=False)
        try:
            req = client.build_request("GET", STREAM_URL, headers=HEADERS)
            resp = await client.send(req, stream=True)
            try:
                async for chunk in resp.aiter_bytes(chunk_size=4096):
                    if not chunk:
                        continue
                    total_bytes_sent += len(chunk)
                    if not icy_requested:
                        yield chunk
                    else:
                        buffer.extend(chunk)
                        while len(buffer) >= META_INTERVAL:
                            yield bytes(buffer[:META_INTERVAL])
                            del buffer[:META_INTERVAL]
                            yield build_icy_metadata(current_song, current_cover)
            finally:
                await resp.aclose()
        except Exception as e:
            last_error = repr(e)
        finally:
            await client.aclose()

    headers = {
        "Content-Type": "audio/mpeg",
        "icy-name": "Antyradio Warszawa (RDS)",
        "icy-genre": "Rock",
        "icy-br": "128",
        "Accept-Ranges": "none",
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    if icy_requested:
        headers["icy-metaint"] = str(META_INTERVAL)

    return StreamingResponse(audio_generator(), media_type="audio/mpeg", headers=headers)
