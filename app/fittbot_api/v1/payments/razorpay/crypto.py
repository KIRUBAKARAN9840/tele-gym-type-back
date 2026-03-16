import base64
import hmac
import hashlib
from typing import Dict


def auth_header(key_id: str, key_secret: str) -> Dict[str, str]:
    token = f"{key_id}:{key_secret}".encode()
    return {"Authorization": "Basic " + base64.b64encode(token).decode()}


def _hmac_hex(secret: str, message_bytes: bytes) -> str:
    return hmac.new(secret.encode(), message_bytes, hashlib.sha256).hexdigest()


def _secure_eq(a: str, b: str) -> bool:
    return hmac.compare_digest(a or "", b or "")


def verify_checkout_subscription_sig(key_secret: str, payment_id: str, subscription_id: str, signature: str) -> bool:
    msg = f"{payment_id}|{subscription_id}".encode()
    return _secure_eq(_hmac_hex(key_secret, msg), signature)


def verify_webhook_sig(webhook_secret: str, raw_body: bytes, header_sig: str) -> bool:
    return _secure_eq(_hmac_hex(webhook_secret, raw_body), header_sig)

