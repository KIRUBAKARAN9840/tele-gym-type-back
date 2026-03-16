"""
Distributed Tracing with OpenTelemetry for Fittbot API

Provides end-to-end request tracing across:
- HTTP requests
- Database queries
- External API calls (Razorpay, OpenAI, etc.)
- Celery tasks
- Redis operations

View traces in:
- Jaeger UI (http://localhost:16686)
- AWS X-Ray
- Grafana Tempo
- Any OTLP-compatible backend
"""

import os
import logging
from typing import Optional
from contextlib import contextmanager

logger = logging.getLogger("app.tracing")

# Global tracer instance
_tracer = None
_trace_enabled = False


def init_tracing(
    service_name: str = "fittbot-api",
    environment: str = "development",
    otlp_endpoint: Optional[str] = None,
    sample_rate: float = 1.0,
) -> bool:
    """
    Initialize OpenTelemetry tracing.

    Args:
        service_name: Name of the service for trace identification
        environment: Environment (development, staging, production)
        otlp_endpoint: OTLP collector endpoint (e.g., "http://localhost:4317")
        sample_rate: Sampling rate (0.0 to 1.0, default 1.0 = 100%)

    Returns:
        True if tracing was successfully initialized
    """
    global _tracer, _trace_enabled

    try:
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION
        from opentelemetry.sdk.trace.sampling import TraceIdRatioBased

        # Create resource with service info
        resource = Resource.create({
            SERVICE_NAME: service_name,
            SERVICE_VERSION: os.getenv("APP_VERSION", "1.0.0"),
            "deployment.environment": environment,
        })

        # Create tracer provider with sampling
        sampler = TraceIdRatioBased(sample_rate)
        provider = TracerProvider(resource=resource, sampler=sampler)

        # Configure exporter based on environment
        if otlp_endpoint:
            try:
                from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
                exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
                provider.add_span_processor(BatchSpanProcessor(exporter))
                logger.info(f"OpenTelemetry OTLP exporter configured: {otlp_endpoint}")
            except ImportError:
                logger.warning("OTLP exporter not available, install opentelemetry-exporter-otlp")
        else:
            # Console exporter for development
            try:
                from opentelemetry.sdk.trace.export import ConsoleSpanExporter
                if environment == "development":
                    provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
                    logger.info("OpenTelemetry console exporter configured (development mode)")
            except ImportError:
                pass

        # Set the tracer provider
        trace.set_tracer_provider(provider)
        _tracer = trace.get_tracer(service_name)
        _trace_enabled = True

        # Auto-instrument FastAPI, SQLAlchemy, Redis, httpx
        _auto_instrument()

        logger.info(f"OpenTelemetry tracing initialized for {service_name} ({environment})")
        return True

    except ImportError as e:
        logger.warning(f"OpenTelemetry not available: {e}")
        logger.info("Install with: pip install opentelemetry-api opentelemetry-sdk opentelemetry-exporter-otlp")
        return False
    except Exception as e:
        logger.error(f"Failed to initialize tracing: {e}")
        return False


def _auto_instrument():
    """Auto-instrument common libraries."""

    # FastAPI instrumentation
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        FastAPIInstrumentor().instrument()
        logger.info("FastAPI instrumentation enabled")
    except ImportError:
        logger.debug("FastAPI instrumentation not available")

    # SQLAlchemy instrumentation
    try:
        from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
        SQLAlchemyInstrumentor().instrument()
        logger.info("SQLAlchemy instrumentation enabled")
    except ImportError:
        logger.debug("SQLAlchemy instrumentation not available")

    # Redis instrumentation
    try:
        from opentelemetry.instrumentation.redis import RedisInstrumentor
        RedisInstrumentor().instrument()
        logger.info("Redis instrumentation enabled")
    except ImportError:
        logger.debug("Redis instrumentation not available")

    # httpx instrumentation (for external API calls)
    try:
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
        HTTPXClientInstrumentor().instrument()
        logger.info("HTTPX instrumentation enabled")
    except ImportError:
        logger.debug("HTTPX instrumentation not available")

    # requests instrumentation
    try:
        from opentelemetry.instrumentation.requests import RequestsInstrumentor
        RequestsInstrumentor().instrument()
        logger.info("Requests instrumentation enabled")
    except ImportError:
        logger.debug("Requests instrumentation not available")

    # Celery instrumentation
    try:
        from opentelemetry.instrumentation.celery import CeleryInstrumentor
        CeleryInstrumentor().instrument()
        logger.info("Celery instrumentation enabled")
    except ImportError:
        logger.debug("Celery instrumentation not available")


def get_tracer():
    """Get the configured tracer instance."""
    global _tracer
    if _tracer is None:
        try:
            from opentelemetry import trace
            _tracer = trace.get_tracer("fittbot-api")
        except ImportError:
            return None
    return _tracer


def is_tracing_enabled() -> bool:
    """Check if tracing is enabled."""
    return _trace_enabled


@contextmanager
def create_span(
    name: str,
    attributes: Optional[dict] = None,
    kind: Optional[str] = None,
):
    """
    Create a traced span for manual instrumentation.

    Usage:
        with create_span("process_payment", {"payment_id": "123"}):
            # Your code here
            pass
    """
    tracer = get_tracer()
    if tracer is None:
        yield None
        return

    try:
        from opentelemetry.trace import SpanKind

        span_kind = SpanKind.INTERNAL
        if kind == "client":
            span_kind = SpanKind.CLIENT
        elif kind == "server":
            span_kind = SpanKind.SERVER
        elif kind == "producer":
            span_kind = SpanKind.PRODUCER
        elif kind == "consumer":
            span_kind = SpanKind.CONSUMER

        with tracer.start_as_current_span(name, kind=span_kind) as span:
            if attributes:
                for key, value in attributes.items():
                    span.set_attribute(key, value)
            yield span

    except Exception as e:
        logger.debug(f"Span creation failed: {e}")
        yield None


def add_span_attribute(key: str, value):
    """Add an attribute to the current span."""
    try:
        from opentelemetry import trace
        span = trace.get_current_span()
        if span:
            span.set_attribute(key, value)
    except Exception:
        pass


def add_span_event(name: str, attributes: Optional[dict] = None):
    """Add an event to the current span."""
    try:
        from opentelemetry import trace
        span = trace.get_current_span()
        if span:
            span.add_event(name, attributes=attributes or {})
    except Exception:
        pass


def record_exception(exception: Exception):
    """Record an exception on the current span."""
    try:
        from opentelemetry import trace
        span = trace.get_current_span()
        if span:
            span.record_exception(exception)
            span.set_status(trace.StatusCode.ERROR, str(exception))
    except Exception:
        pass


def get_current_trace_id() -> Optional[str]:
    """Get the current trace ID for correlation."""
    try:
        from opentelemetry import trace
        span = trace.get_current_span()
        if span and span.get_span_context().is_valid:
            return format(span.get_span_context().trace_id, "032x")
    except Exception:
        pass
    return None


def get_current_span_id() -> Optional[str]:
    """Get the current span ID."""
    try:
        from opentelemetry import trace
        span = trace.get_current_span()
        if span and span.get_span_context().is_valid:
            return format(span.get_span_context().span_id, "016x")
    except Exception:
        pass
    return None


# Decorator for tracing functions
def traced(name: Optional[str] = None, attributes: Optional[dict] = None):
    """
    Decorator to automatically trace a function.

    Usage:
        @traced("process_payment")
        async def process_payment(payment_id: str):
            ...
    """
    def decorator(func):
        import functools
        import asyncio

        span_name = name or f"{func.__module__}.{func.__name__}"

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            with create_span(span_name, attributes):
                return await func(*args, **kwargs)

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            with create_span(span_name, attributes):
                return func(*args, **kwargs)

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper

    return decorator


# Integration helpers for specific services

def trace_external_api_call(service: str, endpoint: str, method: str = "POST"):
    """Decorator for tracing external API calls."""
    def decorator(func):
        import functools

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            with create_span(
                f"external_api.{service}",
                attributes={
                    "http.method": method,
                    "http.url": endpoint,
                    "service.name": service,
                },
                kind="client"
            ) as span:
                try:
                    result = await func(*args, **kwargs)
                    if span and hasattr(result, "status_code"):
                        span.set_attribute("http.status_code", result.status_code)
                    return result
                except Exception as e:
                    if span:
                        record_exception(e)
                    raise

        return wrapper
    return decorator


def trace_db_query(operation: str, table: str):
    """Context manager for tracing database queries."""
    return create_span(
        f"db.{operation}",
        attributes={
            "db.operation": operation,
            "db.table": table,
            "db.system": "mysql",
        },
        kind="client"
    )


def trace_celery_task(task_name: str, queue: str):
    """Context manager for tracing Celery tasks."""
    return create_span(
        f"celery.{task_name}",
        attributes={
            "celery.task_name": task_name,
            "celery.queue": queue,
        },
        kind="consumer"
    )
