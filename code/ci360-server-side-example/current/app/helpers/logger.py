import logging
import json

def log_json(msg, obj, level="info"):
    text = f"{msg}:\n{json.dumps(obj, indent=2, ensure_ascii=False)}"
    getattr(logging, level)(text)
