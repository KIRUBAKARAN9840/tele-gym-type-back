import os, httpx, asyncio, logging, random
from fastapi import UploadFile, HTTPException
from dotenv import load_dotenv
load_dotenv()

ASR_PROVIDER = os.getenv("ASR_PROVIDER", "groq").lower()
GROQ_BASE_URL = os.getenv("GROQ_BASE_URL", "https://api.groq.com").rstrip("/")
GROQ_ASR_MODEL = os.getenv("GROQ_ASR_MODEL", "whisper-large-v3")
LOGGER = logging.getLogger(__name__)


class GroqAPIKeyPool:
    """Pool of Groq API keys with automatic rotation"""

    def __init__(self):
        keys = [
            os.getenv("GROQ_API_KEY"),      # Primary key
            os.getenv("GROQ_API_KEY_2"),    # Optional
            os.getenv("GROQ_API_KEY_3"),    # Optional
        ]
        # Filter out None/empty keys
        self.api_keys = [k for k in keys if k and k.strip()]

        if not self.api_keys:
            LOGGER.warning("No Groq API keys configured! Set GROQ_API_KEY in .env")
        else:
            LOGGER.info(f"[Groq Pool] Initialized with {len(self.api_keys)} API key(s)")

        self.current_index = 0

    def get_key(self) -> str:
        """Get next API key in round-robin fashion"""
        if not self.api_keys:
            return None
        if len(self.api_keys) == 1:
            return self.api_keys[0]
        # Multiple keys - rotate
        key = self.api_keys[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.api_keys)
        return key

    def get_random_key(self) -> str:
        """Get random API key for better distribution"""
        if not self.api_keys:
            return None
        return random.choice(self.api_keys)

    @property
    def has_keys(self) -> bool:
        return len(self.api_keys) > 0


# Global pool instance
_groq_pool = None


def get_groq_pool() -> GroqAPIKeyPool:
    """Get or create global Groq pool"""
    global _groq_pool
    if _groq_pool is None:
        _groq_pool = GroqAPIKeyPool()
    return _groq_pool


def get_groq_api_key() -> str:
    """Get next Groq API key from pool"""
    return get_groq_pool().get_key()


# Legacy single key support (for backwards compatibility)
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

async def backoff_call(fn,*,retries=4,base=0.5,factor=2.0):
    delay=base; last=None
    for _ in range(retries):
        try:
            return await fn()
        except HTTPException:
            raise
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            if status == 429 or status >= 500:
                last = e
            else:
                raise
        except Exception as e:
            last = e
        await asyncio.sleep(delay)
        delay *= factor
    raise last

def _extract_error_detail(resp: httpx.Response) -> str:
    try:
        payload = resp.json()
    except ValueError:
        payload = None

    if isinstance(payload, dict):
        err = payload.get("error")
        if isinstance(err, dict):
            code = err.get("code")
            message = err.get("message") or err.get("error")
            if code and message:
                return f"[{code}] {message}"
            if message:
                return message
        if payload.get("message"):
            return str(payload["message"])
        return str(payload)

    text = resp.text.strip()
    return text or f"HTTP {resp.status_code}"

async def transcribe_groq(file:UploadFile, http: httpx.AsyncClient, prompt: str = None)->str:
    api_key = get_groq_api_key()
    if not api_key:
        raise HTTPException(500,"GROQ_API_KEY not set")
    if ASR_PROVIDER!="groq":
        raise HTTPException(500,"ASR_PROVIDER must be 'groq'")
    filename = file.filename or "audio.wav"
    content_type = file.content_type or "audio/wav"
    try:
        await file.seek(0)
    except Exception:
        LOGGER.debug("UploadFile seek unsupported; continuing without rewind")
    file_bytes = await file.read()
    size = len(file_bytes)
    LOGGER.info("Received ASR upload name=%s content_type=%s bytes=%d", filename, content_type, size)
    if not size:
        raise HTTPException(400, "Uploaded audio file is empty")
    url=f"{GROQ_BASE_URL}/openai/v1/audio/transcriptions"
    async def _call():
        files={"file":(filename,file_bytes,content_type)}
        data={"model":GROQ_ASR_MODEL}
        if prompt:
            data["prompt"] = prompt
        headers={"Authorization":f"Bearer {api_key}"}
        r=await http.post(url,data=data,files=files,headers=headers)
        r.raise_for_status(); return r.json()
    try:
        j=await backoff_call(_call)
    except httpx.HTTPStatusError as exc:
        detail = _extract_error_detail(exc.response)
        LOGGER.warning("Groq ASR request failed with %s: %s", exc.response.status_code, detail)
        raise HTTPException(status_code=exc.response.status_code, detail=f"Groq ASR error: {detail}") from exc
    except httpx.RequestError as exc:
        LOGGER.error("Groq ASR request error: %s", exc, exc_info=True)
        raise HTTPException(status_code=502, detail="Unable to reach Groq ASR service") from exc
    return (j.get("text") or "").strip()

async def transcribe_audio(file:UploadFile, *, http: httpx.AsyncClient, prompt: str = None)->str:
    """Generic transcribe function - can accept custom prompt for domain-specific recognition"""
    return await transcribe_groq(file, http=http, prompt=prompt)
