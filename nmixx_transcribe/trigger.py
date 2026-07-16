"""FastAPI router for the WebSub (PubSubHubbub) callback."""
import logging
import xml.etree.ElementTree as ET
from typing import Awaitable, Callable

from fastapi import APIRouter, Query, Request, Response

from nmixx_transcribe.config import WEBSUB_VERIFY_TOKEN, YOUTUBE_CHANNEL_ID

log = logging.getLogger(__name__)

ATOM_NS = {"atom": "http://www.w3.org/2005/Atom", "yt": "http://www.youtube.com/xml/schemas/2015"}
MAX_BODY_BYTES = 1_000_000  # hub payloads are tiny; reject anything grossly oversized

OnVideo = Callable[[str], Awaitable[None]]


def make_router(on_video: OnVideo) -> APIRouter:
    router = APIRouter()

    @router.get("/youtube/websub")
    async def verify(
        hub_mode: str = Query(alias="hub.mode"),
        hub_challenge: str = Query(alias="hub.challenge"),
        hub_verify_token: str = Query(default="", alias="hub.verify_token"),
    ):
        if hub_verify_token != WEBSUB_VERIFY_TOKEN:
            return Response(status_code=404)
        return Response(content=hub_challenge, media_type="text/plain")

    @router.post("/youtube/websub")
    async def notify(request: Request):
        # Never return 5xx here: the hub retries failed deliveries, and a bad payload or a
        # callback bug would otherwise cause a retry storm on the same notification.
        body = await request.body()
        if len(body) > MAX_BODY_BYTES:
            log.warning("websub POST body too large (%d bytes), dropping", len(body))
            return Response(status_code=204)
        try:
            root = ET.fromstring(body)
        except ET.ParseError:
            log.warning("websub POST body is not valid XML, dropping")
            return Response(status_code=204)
        for entry in root.findall("atom:entry", ATOM_NS):
            channel_id_el = entry.find("yt:channelId", ATOM_NS)
            video_id_el = entry.find("yt:videoId", ATOM_NS)
            if channel_id_el is None or video_id_el is None:
                continue
            if channel_id_el.text != YOUTUBE_CHANNEL_ID:
                continue
            try:
                await on_video(video_id_el.text)
            except Exception:
                log.exception("on_video callback failed for video_id=%s", video_id_el.text)
        return Response(status_code=204)

    return router
