# import os
# import requests
# from jinja2 import Template

# class OllamaProvider:
#     def __init__(self, model=None, url=None, temperature=0.2):
#         self.model = model or os.getenv("LLM_MODEL", "qwen2.5:3b-instruct-q4_K_M")#"llama3.1")
#         # pega do ambiente (Docker) e usa localhost se rodar local
#         self.url = url or os.getenv("OLLAMA_URL", "http://localhost:11434/api/chat")
#         self.temperature = temperature
#         self.num_ctx = int(os.getenv("LLM_NUM_CTX", "1536"))
#         self.num_predict = int(os.getenv("LLM_NUM_PREDICT", "160"))
#         self.keep_alive = os.getenv("LLM_KEEP_ALIVE", "2h")

#     def chat_json(self, system_prompt, user, history=None):
#         history = history or []
#         payload = {
#             "model": self.model,
#             "messages": [
#                 *history,
#                 {"role": "system", "content": system_prompt},
#                 {"role": "user", "content": user}
#             ],
#             "stream": False,
#             "options": {
#                 "temperature": self.temperature,
#                 "top_p": 0.9,
#                 #"num_ctx": 2048,
#                 #"num_predict": 256,
#                 "repeat_penalty": 1.05,
#                 "format": "json"  # garante JSON puro
#             }
#         }
#         r = requests.post(self.url, json=payload, timeout=120)
#         r.raise_for_status()
#         return r.json()["message"]["content"]

# def render_prompt(path, **kwargs):
#     with open(path, "r", encoding="utf-8") as f:
#         return Template(f.read()).render(**kwargs)



# import os
# import requests
# from jinja2 import Template

# class _UseGenerateFallback(Exception):
#     pass

# class OllamaProvider:
#     def __init__(self, model=None, url=None, temperature=0.2):
#         self.model = model or os.getenv("LLM_MODEL", "qwen2.5:3b-instruct-q4_K_M")
#         self.url = url or os.getenv("OLLAMA_URL", "http://localhost:11434/api/chat")
#         self.temperature = temperature
#         self.num_ctx = int(os.getenv("LLM_NUM_CTX", "1536"))
#         self.num_predict = int(os.getenv("LLM_NUM_PREDICT", "160"))
#         self.keep_alive = os.getenv("LLM_KEEP_ALIVE", "2h")
#         self.timeout = 60

#     def _build_chat_payload(self, system_prompt, user, history):
#         return {
#             "model": self.model,
#             "messages": [*(history or []),
#                          {"role": "system", "content": system_prompt},
#                          {"role": "user", "content": user}],
#             "stream": False,
#             "keep_alive": self.keep_alive,
#             "options": {
#                 "temperature": self.temperature,
#                 "top_p": 0.9,
#                 "repeat_penalty": 1.05,
#                 "num_ctx": self.num_ctx,
#                 "num_predict": self.num_predict,
#                 "format": "json"
#             }
#         }

#     def _build_generate_payload(self, system_prompt, user, history):
#         # “achatamos” as mensagens em um único prompt
#         parts = []
#         if system_prompt:
#             parts.append(f"[system]\n{system_prompt}")
#         for m in (history or []):
#             role = m.get("role", "user")
#             parts.append(f"[{role}]\n{m.get('content','')}")
#         parts.append(f"[user]\n{user}")
#         prompt = "\n\n".join(parts)
#         return {
#             "model": self.model,
#             "prompt": prompt,
#             "stream": False,
#             "keep_alive": self.keep_alive,
#             "options": {
#                 "temperature": self.temperature,
#                 "top_p": 0.9,
#                 "repeat_penalty": 1.05,
#                 "num_ctx": self.num_ctx,
#                 "num_predict": self.num_predict,
#                 "format": "json"
#             }
#         }

#     def chat_json(self, system_prompt, user, history=None):
#         # 1) tenta /api/chat
#         try:
#             payload = self._build_chat_payload(system_prompt, user, history)
#             r = requests.post(self.url, json=payload, timeout=self.timeout)
#             if r.status_code == 404:
#                 raise _UseGenerateFallback()
#             r.raise_for_status()
#             return r.json()["message"]["content"]
#         except _UseGenerateFallback:
#             # 2) fallback: troca para /api/generate
#             gen_url = self.url.replace("/api/chat", "/api/generate")
#             payload = self._build_generate_payload(system_prompt, user, history)
#             r = requests.post(gen_url, json=payload, timeout=self.timeout)
#             r.raise_for_status()
#             return r.json()["response"]

# def render_prompt(path, **kwargs):
#     with open(path, "r", encoding="utf-8") as f:
#         return Template(f.read()).render(**kwargs)



# app/ai_provider.py
import os
import requests
from jinja2 import Template

class _UseGenerateFallback(Exception):
    """Sinaliza para usar /api/generate quando /api/chat não existir (404)."""
    pass

def _to_generate_url(chat_url: str) -> str:
    if "/api/chat" in chat_url:
        return chat_url.replace("/api/chat", "/api/generate")
    # fallback defensivo: tenta mesma base com /api/generate
    if chat_url.endswith("/"):
        return chat_url + "api/generate"
    return chat_url + "/api/generate"

class OllamaProvider:
    """
    Provider para Ollama com:
      - leitura de params via env (modelo/temperatura/tokens/etc.)
      - fallback automático (/api/chat -> /api/generate)
      - saída forçada em JSON (options.format = "json")
    """
    def __init__(self, model=None, url=None, temperature=0.2):
        # Perfil "Natural"
        self.model = model or os.getenv("LLM_MODEL", "qwen2.5:7b-instruct-q4_K_M")
        self.url = url or os.getenv("OLLAMA_URL", "http://localhost:11434/api/chat")

        # Knobs
        self.temperature = float(os.getenv("LLM_TEMPERATURE", temperature))
        self.num_ctx = int(os.getenv("LLM_NUM_CTX", "2048"))
        self.num_predict = int(os.getenv("LLM_NUM_PREDICT", "200"))
        self.keep_alive = os.getenv("LLM_KEEP_ALIVE", "2h")
        self.num_thread = int(os.getenv("LLM_NUM_THREAD", "0"))  # 0 = auto

        self.timeout = int(os.getenv("LLM_TIMEOUT_S", "60"))

    # -------- payload builders --------
    def _build_chat_payload(self, system_prompt, user, history):
        return {
            "model": self.model,
            "messages": [
                *(history or []),
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user},
            ],
            "stream": False,
            "keep_alive": self.keep_alive,
            "options": {
                "temperature": self.temperature,
                "top_p": 0.9,
                "repeat_penalty": 1.05,
                "num_ctx": self.num_ctx,
                "num_predict": self.num_predict,
                "num_thread": self.num_thread,
                "format": "json",
            },
        }

    def _build_generate_payload(self, system_prompt, user, history):
        # Achata as mensagens num único prompt (estilo /api/generate)
        parts = []
        if system_prompt:
            parts.append(f"[system]\n{system_prompt}")
        for m in (history or []):
            role = m.get("role", "user")
            parts.append(f"[{role}]\n{m.get('content','')}")
        parts.append(f"[user]\n{user}")
        prompt = "\n\n".join(parts)
        return {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "keep_alive": self.keep_alive,
            "options": {
                "temperature": self.temperature,
                "top_p": 0.9,
                "repeat_penalty": 1.05,
                "num_ctx": self.num_ctx,
                "num_predict": self.num_predict,
                "num_thread": self.num_thread,
                "format": "json",
            },
        }

    # -------- API principal --------
    def chat_json(self, system_prompt, user, history=None):
        # 1) tenta /api/chat
        try:
            payload = self._build_chat_payload(system_prompt, user, history)
            r = requests.post(self.url, json=payload, timeout=self.timeout)
            if r.status_code == 404:
                #raise _UseGenerateFallback()
                txt = r.text.lower()
                # se for 404 por modelo inexistente, avisa claramente
                if "model" in txt and "not found" in txt:
                    raise RuntimeError(f"Modelo '{self.model}' não está disponível no serviço Ollama. Rode: `docker compose exec ollama ollama pull {self.model}`")
                raise _UseGenerateFallback()
            r.raise_for_status()
            r.raise_for_status()
            data = r.json()
            return data["message"]["content"]
        except _UseGenerateFallback:
            # 2) fallback: /api/generate
            gen_url = _to_generate_url(self.url)
            gen_payload = self._build_generate_payload(system_prompt, user, history)
            r = requests.post(gen_url, json=gen_payload, timeout=self.timeout)
            r.raise_for_status()
            data = r.json()
            return data["response"]

# -------- util de prompt --------
def render_prompt(path, **kwargs):
    with open(path, "r", encoding="utf-8") as f:
        return Template(f.read()).render(**kwargs)