# app/utils/openai_sync.py

import os
import random
import logging
from typing import Optional, List, Dict, Any
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class SyncOpenAIPool:

    def __init__(self):
        # Load all API keys from environment with their tier weights
        # Weight represents relative capacity (Tier 3 = 5000 RPM, Tier 1 = 500 RPM)
        key_configs = [
            {"key": os.getenv("OPENAI_API_KEY"), "weight": 10, "tier": "Tier 3"},      # Primary - Tier 3 (5000 RPM)
            {"key": os.getenv("OPENAI_API_KEY_2"), "weight": 1, "tier": "Tier 1"},     # Secondary - Tier 1 (500 RPM)
            {"key": os.getenv("OPENAI_API_KEY_3"), "weight": 1, "tier": "Tier 1"},     # Secondary - Tier 1 (500 RPM)
            {"key": os.getenv("OPENAI_API_KEY_4"), "weight": 1, "tier": "Tier 1"},     # Optional - Tier 1 (500 RPM)
        ]

        # Filter out None/empty keys
        self.key_configs = [c for c in key_configs if c["key"] and c["key"].strip()]

        if not self.key_configs:
            raise ValueError("No OpenAI API keys configured! Set OPENAI_API_KEY in .env")

        # Create sync client pool
        self.clients = [OpenAI(api_key=c["key"]) for c in self.key_configs]

        # Build weighted selection list
        # Each key appears in the list proportional to its weight
        self.weighted_indices = []
        for i, config in enumerate(self.key_configs):
            self.weighted_indices.extend([i] * config["weight"])

        self.current_index = 0
        self.weighted_position = 0

        # Log initialization
        total_weight = sum(c["weight"] for c in self.key_configs)
        logger.info(f"[Sync OpenAI Pool] Initialized with {len(self.clients)} API key(s)")
        for i, config in enumerate(self.key_configs):
            percentage = (config["weight"] / total_weight) * 100
            logger.info(f"  Key {i+1} ({config['tier']}): {percentage:.1f}% of requests")

    def get_client(self) -> OpenAI:
        """Get next client using weighted rotation (Tier 3 gets more traffic)"""
        if len(self.clients) == 1:
            return self.clients[0]

        # Weighted round-robin
        idx = self.weighted_indices[self.weighted_position]
        self.weighted_position = (self.weighted_position + 1) % len(self.weighted_indices)
        return self.clients[idx]

    def get_primary_client(self) -> OpenAI:
        """Always get the primary (Tier 3) client - for critical requests"""
        return self.clients[0]

    def get_random_client(self) -> OpenAI:
        """Get random client with weighted probability"""
        idx = random.choice(self.weighted_indices)
        return self.clients[idx]

    @property
    def total_capacity_rpm(self) -> int:
        """Estimated total RPM capacity across all keys"""
        capacity = 5000  # Primary Tier 3
        capacity += (len(self.clients) - 1) * 500  # Secondary Tier 1 keys
        return capacity


_sync_pool = None


def get_sync_openai_pool() -> SyncOpenAIPool:
    """Get or create global sync OpenAI pool"""
    global _sync_pool
    if _sync_pool is None:
        _sync_pool = SyncOpenAIPool()
    return _sync_pool


def get_sync_openai_client() -> OpenAI:
    """Get next sync OpenAI client from pool (weighted rotation)"""
    return get_sync_openai_pool().get_client()


def sync_openai_call(
    client: OpenAI,
    model: str = "gpt-4o-mini",
    messages: List[Dict[str, Any]] = None,
    temperature: float = 0.7,
    max_tokens: Optional[int] = None,
    response_format: Optional[Dict[str, str]] = None,
    **kwargs
):

    if messages is None:
        messages = []

   
    call_kwargs = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
    }

    if max_tokens is not None:
        call_kwargs["max_tokens"] = max_tokens

    if response_format is not None:
        call_kwargs["response_format"] = response_format


    call_kwargs.update(kwargs)

    # Remove 'stream' if present and False (we don't support streaming here)
    call_kwargs.pop("stream", None)

    return client.chat.completions.create(**call_kwargs)


def sync_openai_vision_call(
    client: OpenAI,
    image_bytes: bytes,
    content_type: str = "image/jpeg",
    prompt: str = "What's in this image?",
    model: str = "gpt-4o",
    max_tokens: int = 1000,
    temperature: float = 0.1,
    **kwargs
):
    """
    Make a sync OpenAI Vision API call.

    Args:
        client: Sync OpenAI client
        image_bytes: Image data as bytes
        content_type: MIME type of the image
        prompt: Text prompt for image analysis
        model: Vision model to use
        max_tokens: Max tokens in response
        temperature: Sampling temperature
        **kwargs: Additional arguments

    Returns:
        OpenAI chat completion response
    """
    import base64

    # Encode image to base64
    image_b64 = base64.b64encode(image_bytes).decode('utf-8')

    messages = [
        {
            "role": "user",
            "content": [
                {"type": "text", "text": prompt},
                {
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:{content_type};base64,{image_b64}",
                        "detail": "low"  # Use low detail for faster processing
                    }
                }
            ]
        }
    ]

    return client.chat.completions.create(
        model=model,
        messages=messages,
        max_tokens=max_tokens,
        temperature=temperature,
        **kwargs
    )
