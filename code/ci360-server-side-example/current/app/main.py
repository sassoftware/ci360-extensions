import os
import sys
import logging
from fastapi import FastAPI, Request, Response, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse

from events.identity import identity_event
from events.data import data_event
from events.content import ci360_content_request
from profiles.profile_manager import PROFILES, resolve_profile_and_idtype
from profiles.session import get_session_id, save_profile_info, SESSION_PROFILES
from helpers.logger import log_json
from helpers.utils import (
    current_timestamp, clean_html_tags, rewrite_offer_links,
    build_load_event_payload, build_custom_event_payload,
    build_spot_viewable_payload, build_spot_clicked_payload
)
from helpers.config_loader import CONFIG

health_check_counter = 0
last_config_snapshot = {}

LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
LOG_PATH = os.path.join(LOG_DIR, "mcp_server.log")
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler(sys.stdout)
    ]
)
VERSION = "2.1.3"

app = FastAPI(title="CI360 MCP Orchestrated Content Server Sticky Profile", version=VERSION)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=86400
)

@app.post("/server")
async def proxy_content_request(request: Request, response: Response, requestbody: dict):
    chatid = get_session_id(request)
    if not request.cookies.get("chatid"):
        response.set_cookie(key="chatid", value=chatid, max_age=86400)
    incoming_user = requestbody.get("user", "")
    log_json("Incoming User Value", {"userfield": incoming_user})
    log_json("Session Profiles Pre-Update", SESSION_PROFILES)
    normalized_user = incoming_user.strip().lower() if incoming_user else None

    if normalized_user in ["off", "logout", "leave", "anonymous"]:
        SESSION_PROFILES.pop(chatid, None)
        session_profile = "anonymous"
    elif normalized_user in PROFILES:
        SESSION_PROFILES[chatid] = normalized_user
        session_profile = normalized_user
    else:
        session_profile = SESSION_PROFILES.get(chatid, "anonymous")
    log_json("Selected Session Profile", session_profile)

    profilename, id_type, id_value = resolve_profile_and_idtype(session_profile)
    log_json("Resolved Profile Info", {"profilename": profilename, "id_type": id_type, "id_value": id_value})

    topic = requestbody.get("topic")
    chaturi = str(request.url)
    spot_id = CONFIG["spotId"]

    if profilename != "anonymous" and id_type != "_ci360_id" and id_value:
        await identity_event(chatid, id_value)
        await data_event(chatid, build_load_event_payload(chaturi))

    if topic:
        await data_event(chatid, build_custom_event_payload(chaturi, topic))

    # --- IMPROVED: Always call the API ---
    params = {
        "id_type": id_type,
        "spot_id": spot_id,
        "topic": topic,  # topic can be None or ""
    }
    if id_type != "_ci360_id" and id_value:
        params["id_value"] = id_value

    contentresult = await ci360_content_request(params)
    api_contents = []
    real_content_shown = False

    # Improved fallback: use API response if possible
    if contentresult and "contents" in contentresult and len(contentresult["contents"]) > 0:
        for item in contentresult["contents"]:
            real_content = bool(item.get("has_content") or item.get("hascontent"))
            raw_content = item.get("content", "")
            cleaned = clean_html_tags(raw_content)
            rewritten_content = rewrite_offer_links(cleaned, item) if real_content else cleaned
            entry = dict(item)
            entry["content"] = rewritten_content
            api_contents.append(entry)
            real_content_shown = real_content_shown or real_content
    else:
        # Fallback response if API didn't return any content
        api_contents.append({
            "spot_id": spot_id,
            "has_content": False,
            "content": "No content returned by API.",
            "profile": profilename,
            "topic": topic
        })

    # Tracking and session saving
    if real_content_shown and api_contents and api_contents[0].get("spot_id"):
        spotdata = {
            "spot_id": api_contents[0].get("spot_id"),
            "task_id": api_contents[0].get("task_id"),
            "creative_id": api_contents[0].get("creative_id"),
        }
        await data_event(chatid, build_spot_viewable_payload(chaturi, spotdata))

    save_profile_info(id_type, id_value, topic, profilename, chatid)
    log_json("Session Profiles Post-Update", SESSION_PROFILES)

    return {
        "contents": api_contents,
        "id_type": id_type,
        "id_value": id_value if id_type != "_ci360_id" else "",
        "profile": profilename if real_content_shown else None,
        "topic": topic if real_content_shown else None
    }


@app.get("/health")
async def health_check(response: Response):
    global health_check_counter, last_config_snapshot
    health_check_counter += 1

    params = {
        "domain": CONFIG.get("domain"),
        "tenantId": CONFIG.get("tenantId"),
        "spotId": CONFIG.get("spotId"),
        "apiEventKey": CONFIG.get("apiEventKey"),
        "channel": CONFIG.get("channel"),
        "mcpBaseUrl": CONFIG.get("mcpBaseUrl")
    }
    changed = params != last_config_snapshot
    show_parameters = False
    if health_check_counter == 1 or changed or health_check_counter % 10 == 0:
        last_config_snapshot = dict(params)
        show_parameters = True

    health_data = {
        "status": "ok",
        "version": VERSION,
        "health_check_calls": health_check_counter,
        "parameters_changed": changed
    }
    if show_parameters:
        health_data["parameters"] = params

    log_json("Health Check Data", health_data)
    response.headers["Content-Type"] = "application/json"
    return health_data

@app.get("/click")
async def click_redirect(
    target: str = Query(...),
    spot_id: str = Query(None),
    task_id: str = Query(None),
    creative_id: str = Query(None),
    request: Request = None
):
    spotdata = {
        "spot_id": spot_id,
        "task_id": task_id,
        "creative_id": creative_id,
    }
    uri = str(request.url) if request else ""
    await data_event(get_session_id(request), build_spot_clicked_payload(uri, spotdata))
    return RedirectResponse(target)

