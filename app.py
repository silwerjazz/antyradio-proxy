import asyncio
import json
import re
import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

app = FastAPI()

# Strumień źródłowy Antyradio Warszawa
STREAM_URL = "https://an.cdn.eurozet.pl/ant-waw.mp3"
META_INTERVAL = 16000  # Co ile bajtów audio wstrzykiwać tekst RDS

current_song = "Antyradio Warszawa"


async def fetch_rds_title(client: httpx.AsyncClient) -> str:
    # Źródło 1: Bezpośredni czytnik RDS Eurozetu
    try:
        resp = await client.get("https://rds.eurozet.pl/reader/var/antyradio.json")
        if resp.status_code == 200:
            text = resp.text.strip()
            match = re.search(r"\{.*\}", text, re.DOTALL)
            if match:
                data = json.loads(match.group(0))
                now = data.get("now", {})
                artist = now.get("artist", "").strip()
                title = now.get("title", "").strip()
                if artist and title and "ANTYRADIO" not in artist.upper():
                    return f"{artist} - {title}"
    except Exception:
        pass

    # Źródło 2 (Fallback): Parsowanie na żywo z OnlineRadioBox
    try:
        resp = await client.get(
            "https://onlineradiobox.com/pl/antyradio94/playlist/",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
        )
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            first_row = soup.select_one("table.tablelist-schedule tr td.track_history_item")
            if first_row:
                track = first_row.get_text(strip=True)
                if track:
                    return track
    except Exception:
        pass

    return ""


async def update_metadata_loop():
    """Pętla w tle aktualizująca tytuł utworu co 15 sekund."""
    global current_song
    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
        while True:
            title = await fetch_rds_title(client)
            if title:
                current_song = title
            await asyncio.sleep(15)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(update_metadata_loop())


def build_icy_metadata(title: str) -> bytes:
    """Formatuje tytuł do binarnej ramki ICY-MetaData (wielokrotność 16 bajtów)."""
    meta_str = f"StreamTitle='{title[:120].replace(chr(39), '')}';"
    meta_bytes = meta_str.encode("utf-8", errors="replace")
    length_blocks = (len(meta_bytes) + 15) // 16
    padded_meta = meta_bytes.ljust(length_blocks * 16, b"\x00")
    return bytes([length_blocks]) + padded_meta


@app.get("/")
def health_check():
    """Strona statusu (przydatna do podglądu i wybudzania kontenera)."""
    return {"status": "online", "now_playing": current_song}


@app.get("/antyradio.mp3")
async def stream_audio(request: Request):
    """Główny endpoint strumienia audio z wstrzykiwanym RDS-em."""
    icy_requested = request.headers.get("Icy-MetaData") == "1"

    async def audio_generator():
        buffer = bytearray()
        async with httpx.AsyncClient(timeout=None, follow_redirects=True) as client:
            async with client.stream("GET", STREAM_URL) as resp:
                async for chunk in resp.aiter_bytes(chunk_size=4096):
                    if not icy_requested:
                        yield chunk
                    else:
                        buffer.extend(chunk)
                        while len(buffer) >= META_INTERVAL:
                            # Wyślij dokładnie 16000 bajtów czystego MP3
                            yield bytes(buffer[:META_INTERVAL])
                            del buffer[:META_INTERVAL]
                            # Wstrzyknij ramkę z tytułem piosenki
                            yield build_icy_metadata(current_song)

    headers = {
        "Content-Type": "audio/mpeg",
        "icy-name": "Antyradio Warszawa (RDS)",
        "icy-genre": "Rock",
        "Cache-Control": "no-cache, no-store",
    }
    if icy_requested:
        headers["icy-metaint"] = str(META_INTERVAL)

    return StreamingResponse(audio_generator(), headers=headers)
