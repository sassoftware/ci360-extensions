import httpx
from helpers.config_loader import CONFIG
from helpers.logger import log_json

JSON_HEADERS = {"Content-Type": "application/json"}

async def ci360_content_request(params: dict):
    import asyncio

    max_retries = int(CONFIG.get("contentApiRetries", 1))
    delay_between = 0.5  # seconds; can be set via config if desired

    url = f"{CONFIG['domain']}/t/content/{CONFIG['tenantId']}"
    url += f"/id_type={params['id_type']}"
    if params['id_type'] != "_ci360_id" and params.get("id_value"):
        url += f"/id_value={params['id_value']}"
    url += f"/spotid={params['spot_id']}"
    if params.get("topic"):
        url += f"/topic={params['topic']}"

    last_error = None
    for attempt in range(1, max_retries + 2):  # retry N times (1 initial + N retried)
        log_json("API Call URL (try {})".format(attempt), {"url": url})
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(url, headers=JSON_HEADERS)
                log_json("API Call Response", {"status": response.status_code, "text": response.text})
                if response.status_code == 200:
                    try:
                        result = response.json()
                    except Exception:
                        result = {"error": f"Could not parse JSON: {response.text}"}
                    return result
                last_error = response.text
        except Exception as e:
            last_error = str(e)

        log_json("API Call Error (try {})".format(attempt), {"error": last_error}, level="error")
        if attempt < max_retries + 1:
            await asyncio.sleep(delay_between)
    return {"error": last_error or "Unknown API error"}
