
from typing import Any
from openai import AsyncOpenAI
from app.utils.ai_retry import ai_call_with_retry


async def async_openai_call(
    oai_client: AsyncOpenAI,
    **kwargs
) -> Any:
 
    # Determine service name from model if available
    model = kwargs.get("model", "unknown")
    service_name = f"openai-{model}"

    # Wrap the OpenAI call with retry logic
    return await ai_call_with_retry(
        lambda: oai_client.chat.completions.create(**kwargs),
        max_attempts=5,  # 5 attempts for high reliability
        base_delay=1.0,  # Start with 1 second
        max_delay=30.0,  # Max 30 seconds between retries
        service_name=service_name,
        use_circuit_breaker=True,
    )
