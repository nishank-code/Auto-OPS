"""PDF merge utility — combines multiple PDF byte-strings into one file."""

import io
import logging
from pathlib import Path

from pypdf import PdfWriter, PdfReader

log = logging.getLogger("pdf")


def merge_pdfs(pdf_bytes_list: list[bytes]) -> bytes:
    writer = PdfWriter()
    for raw in pdf_bytes_list:
        if not raw:
            continue
        try:
            for page in PdfReader(io.BytesIO(raw)).pages:
                writer.add_page(page)
        except Exception as e:
            log.warning(f"Skipping unreadable PDF chunk ({len(raw)} bytes): {e}")
    buf = io.BytesIO()
    writer.write(buf)
    return buf.getvalue()


def save_pdf(data: bytes, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    log.info(f"  ✓ Saved: {path.name} ({len(data):,} bytes)")
