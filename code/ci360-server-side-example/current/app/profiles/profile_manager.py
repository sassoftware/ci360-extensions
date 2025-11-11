import os
from helpers.json_loader import safe_load_json

PROFILE_PATH = os.path.join(os.path.dirname(__file__), '../config/profile.json')
PROFILES = safe_load_json(PROFILE_PATH, {})

def resolve_profile_and_idtype(profile_name):
    key = profile_name.lower() if profile_name else "anonymous"
    entry = PROFILES.get(key)
    if entry:
        return key, entry.get("id_type", "_ci360_id"), entry.get("id_value")
    else:
        return "anonymous", "_ci360_id", None
