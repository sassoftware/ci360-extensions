import uuid

SESSION_PROFILES = {}

def get_session_id(request):
    chatid = request.cookies.get("chatid")
    if not chatid:
        chatid = str(uuid.uuid4())
    return chatid

def save_profile_info(idtype, idvalue, topic, profile, visitorid):
    # Persist to SESSION_PROFILES in-memory dictionary. Extend if you use a database.
    SESSION_PROFILES[visitorid] = {
        "idtype": idtype,
        "idvalue": idvalue,
        "topic": topic,
        "profile": profile
    }
