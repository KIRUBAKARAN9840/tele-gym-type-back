"""
QR Code Generator for Gym IDs
Uses the same AES encryption as client_scanning/verify.py
Usage: python qr_generator.py <gym_id>
"""

import sys
import os


import qrcode
from qrcode.image.styledpil import StyledPilImage
from qrcode.image.styles.moduledrawers.pil import RoundedModuleDrawer
from PIL import Image



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



def generate_gym_qr(gym_id):

    for i in gym_id:
   
        output_path = f"gym_{i}_qr.png"

        encrypted = encrypt_gym_id(i)
        print(f"Encrypted value: {encrypted}")
        print(f"Decrypted back:  {decrypt_gym_id(encrypted)}")

        qr = qrcode.QRCode(
            version=2,
            error_correction=qrcode.constants.ERROR_CORRECT_H,
            box_size=30,
            border=2,
        )
        qr.add_data(encrypted)
        qr.make(fit=True)

        img = qr.make_image(
            image_factory=StyledPilImage,
            module_drawer=RoundedModuleDrawer(radius_ratio=0.8),
        )
        img = img.convert("RGB")
        img = img.resize((512, 512), Image.LANCZOS)


        img_out = img.convert("L")
        img_out.save(output_path, "PNG", optimize=True)

        size_kb = os.path.getsize(output_path) / 1024
        print(f"Saved: {output_path}  ({img_out.size[0]}x{img_out.size[1]}, {size_kb:.1f} KB)")


if __name__ == "__main__":


    gid = [1]
    generate_gym_qr(gid)
