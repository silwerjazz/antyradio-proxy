import asyncio
import json
import re
import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

app = FastAPI()

# Potwierdzony, działający strumień Antyradia (status 200 audio/mpeg)
STREAM_URL = "https://an.cdn.eurozet.pl/ant-web.mp3"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "*/*",
}

META_INTERVAL = 16000
current_song = "Antyradio"


async def fetch_rds_title(client: httpx.AsyncClient) -> str:
    # Źródło 1: OnlineRadioBox
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
                    return track
    except Exception:
        pass

    # Źródło 2: Bezpośredni czytnik RDS Eurozetu
    try:
        resp = await client.get("https://rds.eurozet.pl/reader/var/antyradio.json", headers=HEADERS)
        if resp.status_code == 200:
            match = re.search(r"\{.*\}", resp.text.strip(), re.DOTALL)
            if match:
                data = json.loads(match.group(0))
                now = data.get("now", {})
                artist = now.get("artist", "").strip()
                title = now.get("title", "").strip()
                if artist and title and "ANTYRADIO" not in artist.upper():
                    return f"{artist} - {title}"
    except Exception:
        pass

    return ""


async def update_metadata_loop():
    global current_song
    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True, verify=False) as client:
        while True:
            title = await fetch_rds_title(client)
            if title:
                current_song = title
            await asyncio.sleep(15)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(update_metadata_loop())


def build_icy_metadata(title: str) -> bytes:
    # Bezpieczne kodowanie znaków dla odtwarzaczy Android Auto / Replaio
    clean_title = title[:100].replace("'", "").replace(";", "")
    meta_str = f"StreamTitle='{clean_title}';"
    meta_bytes = meta_str.encode("utf-8", errors="replace")
    length_blocks = (len(meta_bytes) + 15) // 16
    padded_meta = meta_bytes.ljust(length_blocks * 16, b"\x00")
    return bytes([length_blocks]) + padded_meta


@app.get("/")
def health_check():
    return {"status": "online", "now_playing": current_song}


@app.get("/antyradio.mp3")
@app.head("/antyradio.mp3")
async def stream_audio(request: Request):
    # FastAPI przechowuje nagłówki małymi literami
    icy_header = request.headers.get("icy-metadata", "0").strip()
    icy_requested = (icy_header == "1")

    async def audio_generator():
        buffer = bytearray()
        timeout = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0)
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, verify=False) as client:
            try:
                async with client.stream("GET", STREAM_URL, headers=HEADERS) as resp:
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
            except Exception:
                pass

    headers = {
        "Content-Type": "audio/mpeg",
        "icy-name": "Antyradio (RDS)",
        "icy-genre": "Rock",
        "icy-br": "128",
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
        "Expires": "0",
        "X-Accel-Buffering": "no",  # Wyłącza buforowanie proxy na Renderze!
    }
    if icy_requested:
        headers["icy-metaint"] = str(META_INTERVAL)

    return StreamingResponse(audio_generator(), media_type="audio/mpeg", headers=headers)
