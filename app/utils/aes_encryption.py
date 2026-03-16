from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.padding import PKCS7
from base64 import b64encode, b64decode
import os

from app.config.settings import settings

AES_KEY = settings.aes_secret_key.encode()
BLOCK_SIZE = 128 


def encrypt_gym_id(gym_id: int) -> str:
    iv = os.urandom(16)
    padder = PKCS7(BLOCK_SIZE).padder()
    padded = padder.update(str(gym_id).encode()) + padder.finalize()
    cipher = Cipher(algorithms.AES(AES_KEY), modes.CBC(iv))
    encryptor = cipher.encryptor()
    ct_bytes = encryptor.update(padded) + encryptor.finalize()
    return f"{b64encode(iv).decode().rstrip('=')}:{b64encode(ct_bytes).decode().rstrip('=')}"


def decrypt_gym_id(encrypted_str: str) -> int:
    try:
        iv, ct = encrypted_str.split(":")
        iv = b64decode(iv + "==")
        ct = b64decode(ct + "==")
        cipher = Cipher(algorithms.AES(AES_KEY), modes.CBC(iv))
        decryptor = cipher.decryptor()
        padded = decryptor.update(ct) + decryptor.finalize()
        unpadder = PKCS7(BLOCK_SIZE).unpadder()
        pt = unpadder.update(padded) + unpadder.finalize()
        return int(pt.decode("utf-8"))
    except Exception as e:
        raise ValueError(f"Invalid gym_id decryption: {str(e)}")
