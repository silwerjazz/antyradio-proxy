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
# nagłówki do streamu źródłowego: prosimy o metadane ICY
UP_HEADERS = {**HEADERS, "Accept": "*/*", "Icy-MetaData": "1"}

ANTYRADIO_LOGO = "https://gfx.antyradio.pl/design/antyradio/images/logo.png"
META_INTERVAL = 16000
# Jeśli serwer stoi za reverse proxy / w innej sieci niż odtwarzacz,
# ustaw np. PUBLIC_URL=http://192.168.1.10:8000
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
background_tasks:
