# app/utils/openai_pool.py
"""
OpenAI API Key Pool - Weighted rotation based on tier capacity
- Primary key (Tier 3): 5,000 RPM - used ~83% of time
- Secondary keys (Tier 1): 500 RPM each - used ~8.5% each
"""
import os
import random
import logging
from openai import AsyncOpenAI
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class OpenAIPool:
    """Pool of OpenAI clients with weighted key rotation based on tier capacity"""

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

        # Create client pool
        self.clients = [AsyncOpenAI(api_key=c["key"]) for c in self.key_configs]

        # Build weighted selection list
        # Each key appears in the list proportional to its weight
        self.weighted_indices = []
        for i, config in enumerate(self.key_configs):
            self.weighted_indices.extend([i] * config["weight"])

        self.current_index = 0
        self.weighted_position = 0

        # Log initialization
        total_weight = sum(c["weight"] for c in self.key_configs)
        logger.info(f"[OpenAI Pool] Initialized with {len(self.clients)} API key(s)")
        for i, config in enumerate(self.key_configs):
            percentage = (config["weight"] / total_weight) * 100
            logger.info(f"  Key {i+1} ({config['tier']}): {percentage:.1f}% of requests")

    def get_client(self) -> AsyncOpenAI:
        """Get next client using weighted rotation (Tier 3 gets more traffic)"""
        if len(self.clients) == 1:
            return self.clients[0]

        # Weighted round-robin
        idx = self.weighted_indices[self.weighted_position]
        self.weighted_position = (self.weighted_position + 1) % len(self.weighted_indices)
        return self.clients[idx]

    def get_primary_client(self) -> AsyncOpenAI:
        """Always get the primary (Tier 3) client - for critical requests"""
        return self.clients[0]

    def get_random_client(self) -> AsyncOpenAI:
        """Get random client with weighted probability"""
        idx = random.choice(self.weighted_indices)
        return self.clients[idx]

    def get_secondary_client(self) -> AsyncOpenAI:
        """Get a secondary (Tier 1) client - to offload from primary"""
        if len(self.clients) > 1:
            return random.choice(self.clients[1:])
        return self.clients[0]

    @property
    def total_capacity_rpm(self) -> int:
        """Estimated total RPM capacity across all keys"""
        # Tier 3 = 5000, Tier 1 = 500 each
        capacity = 5000  # Primary Tier 3
        capacity += (len(self.clients) - 1) * 500  # Secondary Tier 1 keys
        return capacity


# Global pool instance
_pool = None


def get_openai_pool() -> OpenAIPool:
    """Get or create global OpenAI pool"""
    global _pool
    if _pool is None:
        _pool = OpenAIPool()
    return _pool


def get_openai_client() -> AsyncOpenAI:
    """Get next OpenAI client from pool (weighted rotation)"""
    return get_openai_pool().get_client()


def get_primary_openai_client() -> AsyncOpenAI:
    """Get primary (Tier 3) client for critical requests"""
    return get_openai_pool().get_primary_client()
