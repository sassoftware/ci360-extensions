import httpx
from helpers.config_loader import CONFIG
from helpers.logger import log_json

NDJSON_HEADERS = {"Content-Type": "application/x-ndjson"}

async def identity_event(visitor_id: str, username: str):
    url = (f"{CONFIG['domain']}/t/events/d/"
           f"{CONFIG['tenantId']}/{visitor_id}/id_type=customer_id/id_value={username}")
    log_json("API Call URL", {"url": url})
    log_json("API Call Payload", {"payload": "(empty body)"})
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(url, headers=NDJSON_HEADERS, data="")
            log_json("API Call Response", {"status": response.status_code, "text": response.text})
        except Exception as e:
            log_json("API Call Error", {"error": str(e)}, level="error")
