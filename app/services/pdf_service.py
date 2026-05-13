import io
import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.request import urlopen

from PIL import Image, ImageOps
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas

LOGO_PATH = os.path.join(os.path.dirname(__file__), "logo.png")


CLINIC_TITLE = "Hahnemann's Homoeo Pharmacy"
CLINIC_ADDRESS_LINE1 = "53 Boral Main Road, Garia"
CLINIC_ADDRESS_LINE2 = "Kolkata 700084 – India"


def _safe(value: Optional[str]) -> str:
    return (value or "").strip()


def _join_qualifications(value: Any) -> str:
    if isinstance(value, list):
        return ", ".join(str(item).strip() for item in value if str(item).strip())
    return _safe(str(value)) if value else ""


def _wrap_lines(pdf: canvas.Canvas, text: str, *, font_name: str, font_size: float, max_width: float) -> List[str]:
    text = _safe(text)
    if not text:
        return []

    words = text.split()
    lines: List[str] = []
    current = ""

    for word in words:
        candidate = word if not current else f"{current} {word}"
        if pdf.stringWidth(candidate, font_name, font_size) <= max_width:
            current = candidate
        else:
            if current:
                lines.append(current)
            current = word

    if current:
        lines.append(current)

    return lines


def _draw_wrapped_text(
    pdf: canvas.Canvas,
    text: str,
    *,
    x: float,
    y: float,
    font_name: str,
    font_size: float,
    max_width: float,
    line_height: float,
) -> float:
    lines = _wrap_lines(pdf, text, font_name=font_name, font_size=font_size, max_width=max_width)
    pdf.setFont(font_name, font_size)
    for line in lines:
        pdf.drawString(x, y, line)
        y -= line_height
    return y


def _load_signature(signature_url: Optional[str]) -> ImageReader | None:
    if not signature_url:
        return None

    try:
        with urlopen(signature_url, timeout=8) as response:
            raw = response.read()
        image = Image.open(io.BytesIO(raw))
        image = ImageOps.exif_transpose(image)
        image = image.convert("RGBA")

        bbox = image.getbbox()
        if bbox:
            image = image.crop(bbox)

        target_height = 150
        scale = target_height / max(image.height, 1)
        resized = image.resize(
            (max(1, int(image.width * scale)), target_height),
            Image.Resampling.LANCZOS,
        )
        return ImageReader(resized)
    except Exception:
        return None


def _draw_page_background(pdf: canvas.Canvas, width: float, height: float) -> None:
    """Fill entire page with light lime green background."""
    pdf.saveState()
    pdf.setFillColor(colors.HexColor("#F0FFF0"))
    pdf.rect(0, 0, width, height, fill=1, stroke=0)
    pdf.restoreState()


def _draw_watermark(pdf: canvas.Canvas, width: float, height: float) -> None:
    """Draw a large, semi-transparent logo watermark in the center of the page."""
    if not os.path.exists(LOGO_PATH):
        return
    try:
        img = Image.open(LOGO_PATH).convert("RGBA")
        # Make the image semi-transparent
        alpha = img.split()[3]
        alpha = alpha.point(lambda p: int(p * 0.08))
        img.putalpha(alpha)

        # Fit watermark within A4 page with padding
        img_w, img_h = img.size
        aspect = img_w / img_h
        max_wm_w = width - 40 * mm   # 20mm padding each side
        max_wm_h = height - 40 * mm  # 20mm padding top/bottom
        # Scale to fit within bounds while preserving aspect ratio
        if aspect >= 1:
            # Wider than tall
            wm_w = min(max_wm_w, max_wm_h * aspect)
            wm_h = wm_w / aspect
        else:
            # Taller than wide
            wm_h = min(max_wm_h, max_wm_w / aspect)
            wm_w = wm_h * aspect

        reader = ImageReader(img)
        x = (width - wm_w) / 2
        y = (height - wm_h) / 2
        pdf.drawImage(reader, x, y, width=wm_w, height=wm_h, mask="auto")
    except Exception:
        pass


def build_prescription_pdf_bytes(
    *,
    rx_id: Optional[str],
    created_at: datetime,
    doctor: Dict[str, Any],
    patient: Dict[str, Any],
    appointment: Dict[str, Any],
    items: List[Dict[str, Any]],
    chief_complaints: Optional[str],
    diagnosis: Optional[str],
    advice: Optional[str],
    signature_url: Optional[str] = None,
) -> bytes:
    buf = io.BytesIO()
    pdf = canvas.Canvas(buf, pagesize=A4)
    width, height = A4

    left = 18 * mm
    right = width - 18 * mm
    top = height - 18 * mm
    bottom = 18 * mm
    content_width = right - left
    section_gap = 10 * mm

    appointment_date = appointment.get("scheduled_at")
    if isinstance(appointment_date, datetime):
        date_label = appointment_date.strftime("%d-%m-%Y")
    elif appointment_date:
        try:
            date_label = datetime.fromisoformat(str(appointment_date).replace("Z", "+00:00")).strftime("%d-%m-%Y")
        except ValueError:
            date_label = str(appointment_date)
    else:
        date_label = created_at.strftime("%d-%m-%Y")

    doctor_name = _safe(doctor.get("full_name") or "Doctor")
    qualifications = _join_qualifications(doctor.get("qualifications"))
    registration_no = _safe(doctor.get("registration_no"))

    def draw_header(current_y: float) -> float:
        _draw_page_background(pdf, width, height)
        _draw_watermark(pdf, width, height)

        doctor_header_size = 13

        # Pharmacy name — bigger and bolder
        pdf.setFillColor(colors.HexColor("#284B47"))
        pdf.setFont("Helvetica-Bold", 15)
        pdf.drawString(left, current_y, CLINIC_TITLE)

        # Address below pharmacy name
        addr_y = current_y - 4.5 * mm
        pdf.setFont("Helvetica", 8)
        pdf.drawString(left, addr_y, CLINIC_ADDRESS_LINE1)
        addr_y -= 3.5 * mm
        pdf.drawString(left, addr_y, CLINIC_ADDRESS_LINE2)

        # Doctor section on the right
        pdf.setFillColor(colors.black)
        pdf.setFont("Helvetica-Bold", doctor_header_size)
        pdf.drawRightString(right, current_y, doctor_name)

        line_y = current_y - 5 * mm
        if qualifications:
            pdf.setFont("Helvetica-Bold", 9.5)
            pdf.drawRightString(right, line_y, qualifications)
            line_y -= 4.5 * mm

        if registration_no:
            pdf.setFont("Helvetica", 9)
            pdf.drawRightString(right, line_y, f"Reg no. {registration_no}")

        rule_y = current_y - 14 * mm
        pdf.setLineWidth(0.9)
        pdf.line(left, rule_y, right, rule_y)
        return rule_y - 6 * mm

    def ensure_space(current_y: float, needed: float) -> float:
        if current_y - needed >= bottom + 32 * mm:
            return current_y
        pdf.showPage()
        return draw_header(top)

    y = draw_header(top)

    # Consistent label gap: measure each label width and add a fixed 2mm padding
    label_gap = 2 * mm

    pdf.setFillColor(colors.black)
    pdf.setFont("Helvetica-Bold", 9)
    pdf.drawString(left, y, "Name:")
    name_val_x = left + pdf.stringWidth("Name:", "Helvetica-Bold", 9) + label_gap
    pdf.setFont("Helvetica", 9)
    pdf.drawString(name_val_x, y, _safe(patient.get("name")) or "-")

    row_two_y = y - 6 * mm

    # Row 2: Age | Sex | Phone | Date — weighted columns
    col1 = left
    col2 = left + 22 * mm
    col3 = left + 44 * mm
    col4 = left + 110 * mm

    left_fields = [
        (col1, "Age:", str(patient.get("age") or "-"), "Helvetica", 9),
        (col2, "Sex:", _safe(str(patient.get("sex") or "")) or "-", "Helvetica", 9),
        (col3, "Phone:", _safe(patient.get("phone")) or "-", "Helvetica", 9),
    ]

    for col_x, label, value, val_font, val_size in left_fields:
        pdf.setFont("Helvetica-Bold", 9)
        pdf.drawString(col_x, row_two_y, label)
        val_x = col_x + pdf.stringWidth(label, "Helvetica-Bold", 9) + label_gap
        pdf.setFont(val_font, val_size)
        pdf.drawString(val_x, row_two_y, value)

    # Date right-aligned
    date_str = f"Date: {date_label}"
    pdf.setFont("Helvetica-Bold", 10)
    pdf.drawRightString(right, row_two_y, date_str)

    separator_y = row_two_y - 5 * mm
    pdf.setLineWidth(0.6)
    pdf.line(left, separator_y, right, separator_y)
    y = separator_y - 15 * mm

    def draw_section(title: str, body: str, current_y: float, *, min_height: float = 20 * mm) -> float:
        current_y = ensure_space(current_y, min_height)
        pdf.setFont("Helvetica-BoldOblique", 11)
        pdf.drawString(left, current_y, title)
        current_y -= 7 * mm
        if body:
            current_y = _draw_wrapped_text(
                pdf,
                body,
                x=left,
                y=current_y,
                font_name="Helvetica",
                font_size=10,
                max_width=content_width,
                line_height=5.2 * mm,
            )
        else:
            current_y -= 14 * mm
        return current_y - section_gap

    y = draw_section("C/O", _safe(chief_complaints), y, min_height=30 * mm)
    y = draw_section("DIAGNOSIS", _safe(diagnosis), y, min_height=28 * mm)

    y = ensure_space(y, 40 * mm)
    pdf.setFont("Helvetica-BoldOblique", 11)
    pdf.drawString(left, y, "Rx")
    y -= 8 * mm

    if items:
        for index, item in enumerate(items, start=1):
            medicine_name = f"{index}. {_safe(item.get('name')) or '-'}"
            dosage = _safe(item.get("dosage"))
            # Build medicine line with dosage on same line
            if dosage:
                medicine_line = f"{medicine_name} {dosage}"
            else:
                medicine_line = medicine_name
            meta = [part for part in [_safe(item.get("frequency")), _safe(item.get("duration"))] if part]
            instructions = _safe(item.get("instructions"))
            needed = 14 * mm + (8 * mm if meta else 0) + (8 * mm if instructions else 0)
            y = ensure_space(y, needed)

            pdf.setFont("Helvetica-Bold", 10.5)
            y = _draw_wrapped_text(
                pdf,
                medicine_line,
                x=left,
                y=y,
                font_name="Helvetica-Bold",
                font_size=10.5,
                max_width=content_width,
                line_height=5.2 * mm,
            )

            if meta:
                pdf.setFont("Helvetica", 9.5)
                y = _draw_wrapped_text(
                    pdf,
                    " | ".join(meta),
                    x=left + 6 * mm,
                    y=y + 0.5 * mm,
                    font_name="Helvetica",
                    font_size=9.5,
                    max_width=content_width - 6 * mm,
                    line_height=4.7 * mm,
                )

            if instructions:
                pdf.setFont("Helvetica-Oblique", 9.5)
                y = _draw_wrapped_text(
                    pdf,
                    instructions,
                    x=left + 6 * mm,
                    y=y + 0.5 * mm,
                    font_name="Helvetica-Oblique",
                    font_size=9.5,
                    max_width=content_width - 6 * mm,
                    line_height=4.7 * mm,
                )

            y -= 2 * mm
    else:
        y -= 20 * mm

    y -= 6 * mm
    y = draw_section("ADVICE / NOTES", _safe(advice), y, min_height=36 * mm)

    signature_reader = _load_signature(signature_url)
    signature_block_height = 34 * mm
    y = ensure_space(y, signature_block_height)

    signature_width = 34 * mm
    signature_height = 16 * mm
    signature_x = right - signature_width
    signature_y = bottom + 12 * mm

    if signature_reader:
        pdf.drawImage(
            signature_reader,
            signature_x,
            signature_y,
            width=signature_width,
            height=signature_height,
            mask="auto",
            preserveAspectRatio=True,
            anchor="s",
        )

    text_y = signature_y - 2 * mm
    pdf.setFont("Helvetica", 8.5)
    
    # Calculate center of the signature block for text alignment
    block_center = signature_x + (signature_width / 2)
    
    pdf.drawCentredString(block_center, text_y, doctor_name)
    if qualifications:
        text_y -= 4 * mm
        pdf.drawCentredString(block_center, text_y, qualifications)
    if registration_no:
        text_y -= 4 * mm
        pdf.drawCentredString(block_center, text_y, f"Reg no. {registration_no}")

    pdf.save()
    return buf.getvalue()
