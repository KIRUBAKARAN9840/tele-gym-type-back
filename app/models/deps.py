# deps.py
from fastapi import Request
import httpx
from openai import OpenAI
import orjson
import redis.asyncio as redis

class MemoryStoreRedis:
    def __init__(self, r: redis.Redis, keep: int = 20):
        self.r = r; self.keep = keep
    def _k_hist(self, uid: str) -> str: return f"chat:{uid}:history"
    def _k_pend(self, uid: str) -> str: return f"chat:{uid}:pending"
    async def add(self, uid: str, role: str, content: str):
        await self.r.rpush(self._k_hist(uid), orjson.dumps({"role":role,"content":content}))
        await self.r.ltrim(self._k_hist(uid), -self.keep, -1)
        await self.r.expire(self._k_hist(uid), 60*60*24)
    async def recent(self, uid: str):
        arr = await self.r.lrange(self._k_hist(uid), 0, -1)
        out=[]
        for b in arr[-self.keep:]:
            try: out.append(orjson.loads(b))
            except: pass
        return out
    async def get_pending(self, uid: str):
        b = await self.r.get(self._k_pend(uid))
        return orjson.loads(b) if b else {}
    async def set_pending(self, uid: str, obj):
        if obj is None: await self.r.delete(self._k_pend(uid))
        else: await self.r.set(self._k_pend(uid), orjson.dumps(obj), ex=60*60*24)
    
    # NEW METHOD: Add this clear_pending method
    async def clear_pending(self, uid: str):
        """Clear pending state for a user by deleting the pending key"""
        await self.r.delete(self._k_pend(uid))

    async def clear_chat_on_exit(self, uid: str):
        """Clear both chat history and pending state when user exits chatbot"""
        await self.r.delete(self._k_hist(uid))
        await self.r.delete(self._k_pend(uid))

# These MUST take only Request. Do not add params or annotations here.
def get_http(request: Request) -> httpx.AsyncClient:
    return request.app.state.http

def get_oai(request: Request) -> OpenAI:
    return request.app.state.oai

def get_mem(request: Request) -> MemoryStoreRedis:
    if not hasattr(request.app.state, "mem"):
        request.app.state.mem = MemoryStoreRedis(request.app.state.rds, keep=20)
    return request.app.state.mem

def get_redis(request: Request) -> redis.Redis:
    return request.app.state.rds