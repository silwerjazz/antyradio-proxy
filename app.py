import asyncio
import json
import re
import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

app = FastAPI()

# Lista strumieni Antyradia (główny + zapasowe serwery bez blokad geo/IP)
STREAM_URLS = [
    "https://an.cdn.eurozet.pl/ant-waw.mp3",
    "https://an.cdn.eurozet.pl/ant-web.mp3",
    "http://n-22-12.dcs.redcdn.pl/sc/o2/Eurozet/live/antyradio.livx?audio=5",
    "http://ant-waw-01.cdn.eurozet.pl:8602/"
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Connection": "keep-alive"
}

META_INTERVAL = 16000
current_song = "Antyradio Warszawa"


async def fetch_rds_title(client: httpx.AsyncClient) -> str:
    # Źródło 1: OnlineRadioBox (najpewniejsze z zagranicznych IP)
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
    clean_title = title[:120].replace("'", "")
    meta_str = f"StreamTitle='{clean_title}';"
    meta_bytes = meta_str.encode("utf-8", errors="replace")
    length_blocks = (len(meta_bytes) + 15) // 16
    padded_meta = meta_bytes.ljust(length_blocks * 16, b"\x00")
    return bytes([length_blocks]) + padded_meta


@app.get("/")
def health_check():
    return {"status": "online", "now_playing": current_song}


@app.get("/debug")
async def debug_streams():
    """Sprawdza, który serwer Antyradia wpuszcza połączenie z Rendera."""
    results = {}
    async with httpx.AsyncClient(timeout=5.0, follow_redirects=True, verify=False) as client:
        for url in STREAM_URLS:
            try:
                async with client.stream("GET", url, headers=HEADERS) as r:
                    results[url] = {"status": r.status_code, "content_type": r.headers.get("content-type")}
            except Exception as e:
                results[url] = {"error": str(e)}
    return {"now_playing": current_song, "streams_check": results}


@app.get("/antyradio.mp3")
async def stream_audio(request: Request):
    icy_requested = request.headers.get("Icy-MetaData") == "1"

    async def audio_generator():
        buffer = bytearray()
        async with httpx.AsyncClient(timeout=None, follow_redirects=True, verify=False) as client:
            for stream_url in STREAM_URLS:
                try:
                    async with client.stream("GET", stream_url, headers=HEADERS) as resp:
                        if resp.status_code != 200:
                            continue
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
                        break
                except Exception:
                    continue

    headers = {
        "Content-Type": "audio/mpeg",
        "icy-name": "Antyradio Warszawa (RDS)",
        "icy-genre": "Rock",
        "Cache-Control": "no-cache, no-store",
        "Connection": "keep-alive",
    }
    if icy_requested:
        headers["icy-metaint"] = str(META_INTERVAL)

    return StreamingResponse(audio_generator(), headers=headers)
