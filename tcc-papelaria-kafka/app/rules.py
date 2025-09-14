import re

def first_match_intent(text, intents_cfg):
    for rule in intents_cfg:
        for pat in rule["patterns"]:
            if re.search(pat, text):
                return rule["intent"]
    return None