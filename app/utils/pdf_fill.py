"""
PDF prefilling utilities using ReportLab and pypdf.
Handles template caching, coordinate-based text overlay, and PDF generation.
"""
import io
import os
import json
import hashlib
from typing import Dict, Any, Optional
from pathlib import Path

from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from pypdf import PdfReader, PdfWriter

from app.config.settings import settings
from app.utils.s3_pdf_utils import s3_download_bytes

# Local cache directory for PDF templates (in worker)
TEMPLATE_CACHE_DIR = "/tmp/fittbot_pdf_templates"

# Path to coords directory (relative to this file)
COORDS_DIR = Path(__file__).parent / "coords"


def _ensure_dir(path: str) -> None:
    """Create directory if it doesn't exist."""
    os.makedirs(path, exist_ok=True)


def get_template_local_path() -> str:
    """
    Cache the template PDF in the worker local disk.
    Cache key includes template version so future versions don't clash.

    Returns:
        Local file path to cached template PDF
    """
    _ensure_dir(TEMPLATE_CACHE_DIR)
    filename = f"gym_agreement_{settings.pdf_template_version}.pdf"
    local_path = os.path.join(TEMPLATE_CACHE_DIR, filename)

    # Return cached file if it exists and has content
    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
        return local_path

    # Download from S3 and cache locally
    data = s3_download_bytes(settings.pdf_template_s3_key)
    with open(local_path, "wb") as f:
        f.write(data)

    return local_path


def load_coords(version: str = None) -> Dict[str, Any]:
    """
    Load field coordinates JSON for the given template version.

    Args:
        version: Template version (defaults to settings.pdf_template_version)

    Returns:
        Dictionary of page -> field -> coordinate mappings
    """
    version = version or settings.pdf_template_version
    coords_path = COORDS_DIR / f"agreement_coords_{version}.json"

    if not coords_path.exists():
        raise FileNotFoundError(f"Coordinates file not found: {coords_path}")

    with open(coords_path, "r", encoding="utf-8") as f:
        return json.load(f)


def sha256_bytes(data: bytes) -> str:
    """Calculate SHA256 hash of bytes data."""
    return hashlib.sha256(data).hexdigest()


def _draw_fields_on_page(
    page,  # pypdf PageObject
    fields: Dict[str, Dict[str, Any]],
    values: Dict[str, Any],
) -> None:
    """
    Draw text fields on a PDF page using coordinates.

    The fields dict uses top-left Y coordinates (like image coordinates).
    This function converts to PDF bottom-left coordinates.

    Args:
        page: pypdf PageObject to draw on
        fields: Dictionary mapping field_name -> {x, y, font_size, ...}
        values: Dictionary mapping field_name -> value to draw
    """
    packet = io.BytesIO()
    page_width = float(page.mediabox.width)
    page_height = float(page.mediabox.height)

    c = canvas.Canvas(packet, pagesize=(page_width, page_height))

    for field_name, spec in fields.items():
        # Skip if no value provided or value is empty
        if field_name not in values or values[field_name] in (None, ""):
            continue

        x = float(spec["x"])
        y_top = float(spec["y"])  # top-left coordinate
        font_size = int(spec.get("font_size", 10))
        font_name = spec.get("font_name", "Helvetica")

        # Convert top-left y to PDF coordinates (bottom-left origin)
        y_pdf = page_height - y_top

        c.setFont(font_name, font_size)
        c.drawString(x, y_pdf, str(values[field_name]))

    c.save()
    packet.seek(0)

    # Merge the overlay onto the page
    overlay = PdfReader(packet)
    if overlay.pages:
        page.merge_page(overlay.pages[0])


def generate_prefilled_pdf(prefill: Dict[str, Any], version: str = None) -> bytes:
    """
    Generate a prefilled PDF from the template.

    Args:
        prefill: Dictionary of field values to fill
        version: Template version (defaults to settings)

    Returns:
        Prefilled PDF as bytes
    """
    version = version or settings.pdf_template_version
    template_path = get_template_local_path()
    coords = load_coords(version)

    reader = PdfReader(template_path)
    writer = PdfWriter()

    # Map page indices to coordinate keys
    # You can customize this mapping based on your template structure
    page_map = {}
    for page_key in coords.keys():
        # Extract page number from key like "page_1", "page_5"
        if page_key.startswith("page_"):
            try:
                page_num = int(page_key.split("_")[1])
                page_map[page_num - 1] = page_key  # Convert to 0-indexed
            except (IndexError, ValueError):
                continue

    # Process each page
    for idx, page in enumerate(reader.pages):
        page_key = page_map.get(idx)
        if page_key and page_key in coords:
            _draw_fields_on_page(page, coords[page_key], prefill)
        writer.add_page(page)

    # Write to bytes
    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()


def get_s3_key_for_agreement(gym_id: int, agreement_id: str, version: str = None) -> str:
    """
    Generate S3 key for storing a prefilled agreement PDF.

    Args:
        gym_id: Gym ID
        agreement_id: Agreement UUID
        version: Template version

    Returns:
        S3 key string
    """
    version = version or settings.pdf_template_version
    return f"{settings.pdf_agreements_prefix}/{gym_id}/{agreement_id}/agreement_{version}.pdf"


def clear_template_cache(version: str = None) -> bool:
    """
    Clear cached template for a specific version.

    Args:
        version: Template version to clear (defaults to current version)

    Returns:
        True if cache was cleared
    """
    version = version or settings.pdf_template_version
    filename = f"gym_agreement_{version}.pdf"
    local_path = os.path.join(TEMPLATE_CACHE_DIR, filename)

    if os.path.exists(local_path):
        os.remove(local_path)
        return True
    return False
