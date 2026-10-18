import asyncio
import json
import re
import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse

app = FastAPI()

# Lista strumieni (główny + zapasowe węzły CDN Eurozetu)
STREAM_URLS = [
    "https://an.cdn.eurozet.pl/ant-web.mp3",
    "https://an01.cdn.eurozet.pl/ant-web.mp3",
    "https://an02.cdn.eurozet.pl/ant-web.mp3",
    "https://n-22-12.dcs.redcdn.pl/sc/o2/Eurozet/live/antyradio.livx?audio=5",
]

# Czyste nagłówki przeglądarki (bez Icy-MetaData, który wywoływał błąd 404 na CDN!)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Referer": "https://player.antyradio.pl/",
}

META_INTERVAL = 16000
current_song = "Antyradio"
current_cover = ""

last_error = "Brak błędów"
total_bytes_sent = 0
active_stream_url = STREAM_URLS[0]


async def fetch_rds_data(client: httpx.AsyncClient):
    global current_song, current_cover

    # 1. Czytnik RDS Eurozetu (Tytuł + Okładka)
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

    # 2. Fallback: OnlineRadioBox
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


async def find_working_stream(client: httpx.AsyncClient) -> str:
    """Znajduje pierwszy strumień zwracający HTTP 200 i audio/mpeg."""
    global active_stream_url
    for url in STREAM_URLS:
        try:
            async with client.stream("GET", url, headers=HEADERS) as r:
                ct = r.headers.get("content-type", "").lower()
                if r.status_code == 200 and ("audio" in ct or "mpeg" in ct or "aac" in ct or "octet" in ct):
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
    return {"status": "online", "now_playing": current_song, "cover": current_cover}


@app.get("/debug")
async def debug_info():
    results = {}
    async with httpx.AsyncClient(timeout=5.0, follow_redirects=True, verify=False) as client:
        for url in STREAM_URLS:
            try:
                async with client.stream("GET", url, headers=HEADERS) as r:
                    results[url] = {
                        "status": r.status_code,
                        "content_type": r.headers.get("content-type"),
                    }
            except Exception as e:
                results[url] = {"error": repr(e)}

    return {
        "now_playing": current_song,
        "cover_url": current_cover,
        "selected_stream": active_stream_url,
        "total_bytes_sent_to_players": total_bytes_sent,
        "last_stream_error": last_error,
        "streams_check": results,
    }


@app.head("/antyradio.mp3")
async def head_audio(request: Request):
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
    global last_error, total_bytes_sent
    icy_requested = request.headers.get("icy-metadata", "0").strip() == "1"

    async def audio_generator():
        global last_error, total_bytes_sent
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
