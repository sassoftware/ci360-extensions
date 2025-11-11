import httpx
import json
from helpers.config_loader import CONFIG
from helpers.logger import log_json

NDJSON_HEADERS = {"Content-Type": "application/x-ndjson"}

async def data_event(visitor_id: str, payload: dict):
    url = f"{CONFIG['domain']}/t/events/e/{CONFIG['tenantId']}/{visitor_id}/"
    log_json("API Call URL", {"url": url})
    log_json("API Call Payload", payload)
    ndjson = json.dumps(payload) + "\n"
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(url, headers=NDJSON_HEADERS, data=ndjson)
            log_json("API Call Response", {"status": response.status_code, "text": response.text})
        except Exception as e:
            log_json("API Call Error", {"error": str(e)}, level="error")
