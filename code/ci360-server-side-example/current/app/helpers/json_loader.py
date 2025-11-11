import json
import logging

def safe_load_json(path, default=None):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception as e:
        logging.warning(f"Could not load JSON from {path}. Using default value. Error: {str(e)}")
        return default
