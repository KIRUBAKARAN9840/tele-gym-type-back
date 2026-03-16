#kb_store.py
import os
import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional
import numpy as np
import orjson
from openai import AsyncOpenAI

# paths
DATA_DIR = os.getenv("DATA_DIR", "./data")
os.makedirs(DATA_DIR, exist_ok=True)
TXT_PATH = f"{DATA_DIR}/kb_texts.json"
VEC_PATH = f"{DATA_DIR}/kb_vecs.npy"

EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")

# --------- tiny helper (same as your project uses) ----------
# If you already have this elsewhere, you can import it instead.
def rough_tokens(s: str) -> int:
    return max(1, len(s) // 4)

def chunk_text(text: str, max_tok: int = 350) -> List[str]:
    parts, buf, c = [], [], 0
    for line in text.split("\n"):
        t = rough_tokens(line)
        if c + t > max_tok and buf:
            parts.append("\n".join(buf).strip()); buf, c = [], 0
        if line.strip():
            buf.append(line.strip()); c += t
    if buf:
        parts.append("\n".join(buf).strip())
    return parts
# ------------------------------------------------------------

class _KBStore:
    """
    File-backed KB with lazy OpenAI binding.

    - Always sets texts/meta/vecs in __init__ (prevents AttributeError).
    - _oai is optional; bind in startup via KB.bind_oai(app.state.oai)
      OR let it lazily build from env if OPENAI_API_KEY is set.
    """
    def __init__(self):
        self.texts: List[str] = []
        self.meta:  List[Dict[str, Any]] = []
        self.vecs:  Optional[np.ndarray] = None
        self._oai: Optional[AsyncOpenAI] = None
        self.load()

    def bind_oai(self, oai: AsyncOpenAI):
        self._oai = oai

    def _ensure_oai(self):
        if self._oai is None:
            # last-resort lazy init (uses env); better to call bind_oai in startup
            api_key = os.getenv("OPENAI_API_KEY", "")
            if not api_key:
                raise RuntimeError(
                    "KB OpenAI client not bound and OPENAI_API_KEY not set. "
                    "Call KB.bind_oai(app.state.oai) during startup."
                )
            self._oai = AsyncOpenAI(api_key=api_key)

    async def _embed(self, arr: List[str]) -> np.ndarray:
        self._ensure_oai()
        res = await self._oai.embeddings.create(model=EMBEDDING_MODEL, input=arr)
        M = np.array([d.embedding for d in res.data], dtype=np.float32)
        M /= np.linalg.norm(M, axis=1, keepdims=True) + 1e-12
        return M

    async def upsert(self, src: str, raw: str) -> int:
        ch = chunk_text(raw)
        if not ch:
            return 0
        # dedupe by sha1 to avoid re-embedding duplicates
        deduped: List[str] = []
        seen = {hashlib.sha1(t.encode("utf-8")).hexdigest() for t in self.texts}
        for t in ch:
            h = hashlib.sha1(t.encode("utf-8")).hexdigest()
            if h not in seen:
                deduped.append(t); seen.add(h)
        if not deduped:
            return 0
        V = await self._embed(deduped)
        self.vecs = V if self.vecs is None else np.vstack([self.vecs, V])
        start = len(self.texts)
        self.texts.extend(deduped)
        self.meta.extend([{"source": src, "chunk": i + start} for i in range(len(deduped))])
        self._persist()
        return len(deduped)

    async def search(self, q: str, k: int = 4):
        if not self.texts or self.vecs is None:
            return []
        v = (await self._embed([q]))[0]
        sims = (self.vecs @ v).tolist()
        idx = sorted(range(len(sims)), key=lambda i: sims[i], reverse=True)[:k]
        return [{"text": self.texts[i], "meta": self.meta[i], "score": sims[i]} for i in idx]

    def _persist(self):
        Path(TXT_PATH).write_bytes(orjson.dumps({"texts": self.texts, "meta": self.meta}))
        if self.vecs is not None:
            np.save(VEC_PATH, self.vecs)

    def load(self):
        if Path(TXT_PATH).exists():
            d = orjson.loads(Path(TXT_PATH).read_bytes())
            # keep shape even if files are empty/corrupt
            self.texts = d.get("texts", []) or []
            self.meta  = d.get("meta", []) or []
        if Path(VEC_PATH).exists():
            try:
                self.vecs = np.load(VEC_PATH)
            except Exception:
                self.vecs = None

# Export a real singleton instance
KB = _KBStore()
