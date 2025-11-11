import json
import os

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "../config/config.json")

def load_config():
    try:
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return {}
            
CONFIG = load_config()
