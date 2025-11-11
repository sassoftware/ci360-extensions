from datetime import datetime
from urllib.parse import quote_plus
from helpers.config_loader import CONFIG
import re
def current_timestamp():
    return datetime.utcnow().isoformat() + "Z"

def clean_html_tags(content):
    # Remove <data ...> tags common in content offers
    content = re.sub(r"<data [^>]*></data>", "", content)
    # Optionally: Strip any other HTML tags if present
    # content = re.sub(r"<[^>]+>", "", content)
    return content

def rewrite_offer_links(content, spot_data):
    import re
    from urllib.parse import quote_plus
    from helpers.config_loader import CONFIG

    base_url = CONFIG.get("mcpBaseUrl", "http://localhost:8000")

    def url_replacer(match):
        url = match.group(2)  # This is the FULL URL from inside the markdown ()
        display_text = match.group(1)
        # This encodes the entire URL (including ? and everything after)
        redirect = (
            f"{base_url}/click?target={quote_plus(url)}"
            f"&spot_id={quote_plus(str(spot_data.get('spot_id', '')))}"
            f"&task_id={quote_plus(str(spot_data.get('task_id', '')))}"
            f"&creative_id={quote_plus(str(spot_data.get('creative_id', '')))}"
        )
        return f"[{display_text}]({redirect})"

    # This captures everything inside (), including query params
    return re.sub(r'\[([^\]]+)\]\(([^)]+)\)', url_replacer, content)


def build_load_event_payload(uri):
    from helpers.config_loader import CONFIG
    return {
        "eventName": "load",
        "channel": CONFIG.get("channel", "web"),
        "clientTimeStamp": current_timestamp(),
        "uri": uri
    }

def build_custom_event_payload(uri, topic):
    from helpers.config_loader import CONFIG
    return {
        "eventName": "custom",
        "channel": CONFIG.get("channel", "web"),
        "clientTimeStamp": current_timestamp(),
        "uri": uri,
        "custom": {"topic": topic}
    }

def build_spot_viewable_payload(uri, spotdata):
    from helpers.config_loader import CONFIG
    return {
        "eventName": "spot_viewable",
        "channel": CONFIG.get("channel", "web"),
        "clientTimeStamp": current_timestamp(),
        "uri": uri,
        "spot": spotdata
    }

def build_spot_clicked_payload(uri, spotdata):
    from helpers.config_loader import CONFIG
    return {
        "eventName": "spot_clicked",
        "channel": CONFIG.get("channel", "web"),
        "clientTimeStamp": current_timestamp(),
        "uri": uri,
        "spot": spotdata
    }
