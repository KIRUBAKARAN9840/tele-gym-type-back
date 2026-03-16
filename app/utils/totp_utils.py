import pyotp
import qrcode
import secrets
import json
import io
import os
from base64 import b64encode, b64decode
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.padding import PKCS7
from passlib.context import CryptContext

from app.config.settings import settings

AES_KEY = settings.aes_secret_key.encode()
BLOCK_SIZE = 128
BACKUP_CODE_COUNT = 10

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def encrypt_totp_secret(secret: str) -> str:
    """Encrypt TOTP secret using AES-CBC (same pattern as aes_encryption.py)."""
    iv = os.urandom(16)
    padder = PKCS7(BLOCK_SIZE).padder()
    padded = padder.update(secret.encode()) + padder.finalize()
    cipher = Cipher(algorithms.AES(AES_KEY), modes.CBC(iv))
    ct_bytes = cipher.encryptor().update(padded) + cipher.encryptor().finalize()
    return f"{b64encode(iv).decode().rstrip('=')}:{b64encode(ct_bytes).decode().rstrip('=')}"


def decrypt_totp_secret(encrypted_str: str) -> str:
    """Decrypt TOTP secret from AES-CBC."""
    iv_b64, ct_b64 = encrypted_str.split(":")
    iv = b64decode(iv_b64 + "==")
    ct = b64decode(ct_b64 + "==")
    cipher = Cipher(algorithms.AES(AES_KEY), modes.CBC(iv))
    decryptor = cipher.decryptor()
    padded = decryptor.update(ct) + decryptor.finalize()
    unpadder = PKCS7(BLOCK_SIZE).unpadder()
    pt = unpadder.update(padded) + unpadder.finalize()
    return pt.decode("utf-8")


def generate_totp_secret() -> str:
    """Generate a random base32-encoded TOTP secret."""
    return pyotp.random_base32()


def verify_totp_code(secret: str, code: str) -> bool:
    """Verify a TOTP code. Allows 1 window of drift (30s before/after)."""
    totp = pyotp.TOTP(secret)
    return totp.verify(code, valid_window=1)


def generate_provisioning_uri(secret: str, admin_email: str, admin_name: str) -> str:
    """Generate otpauth:// URI for authenticator apps."""
    totp = pyotp.TOTP(secret)
    label = admin_email or admin_name
    return totp.provisioning_uri(name=label, issuer_name="Fittbot Admin")


def generate_qr_code_base64(provisioning_uri: str) -> str:
    """Generate QR code as base64-encoded PNG string."""
    qr = qrcode.QRCode(version=1, box_size=10, border=4)
    qr.add_data(provisioning_uri)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buffer = io.BytesIO()
    img.save(buffer, format="PNG")
    buffer.seek(0)
    return b64encode(buffer.read()).decode("utf-8")


def generate_backup_codes() -> tuple:
    """Generate backup codes. Returns (plaintext_codes, json_hashed_codes)."""
    codes = [secrets.token_hex(4).upper() for _ in range(BACKUP_CODE_COUNT)]
    hashed = [pwd_context.hash(code) for code in codes]
    return codes, json.dumps(hashed)


def verify_backup_code(code: str, hashed_codes_json: str) -> tuple:
    """Verify a backup code. Returns (is_valid, updated_hashed_codes_json).
    A used code is removed from the list."""
    hashed_list = json.loads(hashed_codes_json)
    for i, hashed in enumerate(hashed_list):
        if pwd_context.verify(code, hashed):
            hashed_list.pop(i)
            return True, json.dumps(hashed_list)
    return False, hashed_codes_json
