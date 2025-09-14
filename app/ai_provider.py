import requests
from jinja2 import Template

class OllamaProvider:
    def __init__(self, model="llama3.1", url="http://localhost:11434/api/chat", temperature=0.2):
        self.model, self.url, self.temperature = model, url, temperature

    def chat_json(self, system_prompt, user, history=None):
        history = history or []
        payload = {
            "model": self.model,
            "messages": [*history, {"role":"system","content":system_prompt},{"role":"user","content":user}],
            "stream": False,
            "options": {"temperature": self.temperature}
        }
        r = requests.post(self.url, json=payload, timeout=120)
        r.raise_for_status()
        return r.json()["message"]["content"]

def render_prompt(path, **kwargs):
    with open(path, "r", encoding="utf-8") as f:
        return Template(f.read()).render(**kwargs)