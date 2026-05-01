import io
import os
from datetime import datetime
from typing import Any, Dict, Optional

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas


CLINIC_TITLE = "Hahnemann's Homoeo Pharmacy"
CLINIC_ADDRESS = "53 Boral Main Road, Garia, Kolkata 700084 \u2013 India"
CLINIC_EMAIL = "support@hphomeo.com"
CLINIC_WEBSITE = "www.hphomeo.com"

# Brand colours (from the sample receipt)
BRAND_DARK = colors.HexColor("#1B2A4A")   # dark navy header/footer
BRAND_ACCENT = colors.HexColor("#2D8F5E") # green for PAID badge & total row
BRAND_LIGHT_BG = colors.HexColor("#F5F7FA")  # light grey card backgrounds
BRAND_TABLE_HEADER = colors.HexColor("#1B2A4A")  # dark header row
WHITE = colors.white

LOGO_PATH = os.path.join(os.path.dirname(__file__), "..", "static", "images", "logo.png")


def _safe(value: Optional[str]) -> str:
    return (value or "").strip()


def _format_amount(amount: float) -> str:
    return f"\u20b9 {amount:,.2f}"


def _load_logo() -> ImageReader | None:
    try:
        if os.path.isfile(LOGO_PATH):
            return ImageReader(LOGO_PATH)
    except Exception:
        pass
    return None


def build_receipt_pdf_bytes(
    *,
    receipt_id: str,
    receipt_date: datetime,
    patient_name: str,
    doctor_name: str,
    doctor_registration_no: str,
    consultation_fee: float,
    payment_method: str,
) -> bytes:
    """Generate a payment receipt PDF matching the hpHomeo brand design."""

    # Sanitise fee — must be a non-negative finite number
    if not isinstance(consultation_fee, (int, float)) or consultation_fee < 0:
        consultation_fee = 0.0

    buf = io.BytesIO()
    pdf = canvas.Canvas(buf, pagesize=A4)
    width, height = A4

    left = 20 * mm
    right = width - 20 * mm
    content_width = right - left

    # ── Top green accent bar ─────────────────────────────────
    pdf.setFillColor(BRAND_ACCENT)
    pdf.rect(0, height - 6 * mm, width, 6 * mm, fill=True, stroke=False)

    # ── Header section (logo + clinic info) ──────────────────
    header_y = height - 28 * mm

    logo = _load_logo()
    if logo:
        pdf.drawImage(
            logo,
            left,
            header_y - 4 * mm,
            width=18 * mm,
            height=18 * mm,
            mask="auto",
            preserveAspectRatio=True,
        )

    text_x = left + 22 * mm
    pdf.setFillColor(colors.black)
    pdf.setFont("Helvetica-Bold", 16)
    pdf.drawString(text_x, header_y + 8 * mm, CLINIC_TITLE)

    pdf.setFont("Helvetica", 8.5)
    pdf.setFillColor(colors.HexColor("#555555"))
    pdf.drawString(text_x, header_y + 1 * mm, CLINIC_ADDRESS)
    pdf.drawString(text_x, header_y - 5 * mm, f"{CLINIC_EMAIL}  |  {CLINIC_WEBSITE}")

    # ── Divider line ─────────────────────────────────────────
    div_y = header_y - 14 * mm
    pdf.setStrokeColor(colors.HexColor("#E0E0E0"))
    pdf.setLineWidth(0.8)
    pdf.line(left, div_y, right, div_y)

    # ── "Payment Receipt" title + Date/Receipt No ────────────
    title_y = div_y - 18 * mm
    pdf.setFillColor(colors.black)
    pdf.setFont("Helvetica-Bold", 24)
    pdf.drawString(left, title_y, "Payment Receipt")

    # Date
    date_label = receipt_date.strftime("%d %B %Y")
    pdf.setFont("Helvetica", 9)
    pdf.setFillColor(colors.HexColor("#777777"))
    pdf.drawRightString(right, title_y + 10 * mm, "Date")
    pdf.setFont("Helvetica-Bold", 11)
    pdf.setFillColor(colors.black)
    pdf.drawRightString(right, title_y + 3 * mm, date_label)

    # Receipt No
    pdf.setFont("Helvetica", 9)
    pdf.setFillColor(colors.HexColor("#777777"))
    pdf.drawRightString(right, title_y - 5 * mm, "Receipt No.")
    pdf.setFont("Helvetica-Bold", 11)
    pdf.setFillColor(colors.black)
    pdf.drawRightString(right, title_y - 12 * mm, _safe(receipt_id))

    # ── Patient & Doctor info cards ──────────────────────────
    cards_y = title_y - 32 * mm
    card_height = 18 * mm
    card_width = content_width / 2 - 3 * mm

    # Patient card
    pdf.setFillColor(BRAND_LIGHT_BG)
    pdf.roundRect(left, cards_y - card_height, card_width, card_height, 3 * mm, fill=True, stroke=False)
    # Left blue accent bar on patient card
    pdf.setFillColor(colors.HexColor("#4A90D9"))
    pdf.rect(left, cards_y - card_height, 1.2 * mm, card_height, fill=True, stroke=False)

    pdf.setFont("Helvetica", 7.5)
    pdf.setFillColor(colors.HexColor("#999999"))
    pdf.drawString(left + 5 * mm, cards_y - 5 * mm, "PATIENT NAME")
    pdf.setFont("Helvetica-Bold", 12)
    pdf.setFillColor(colors.black)
    pdf.drawString(left + 5 * mm, cards_y - 12 * mm, _safe(patient_name) or "-")

    # Doctor card
    doctor_card_x = left + card_width + 6 * mm
    pdf.setFillColor(BRAND_LIGHT_BG)
    pdf.roundRect(doctor_card_x, cards_y - card_height, card_width, card_height, 3 * mm, fill=True, stroke=False)
    # Left blue accent bar on doctor card
    pdf.setFillColor(colors.HexColor("#4A90D9"))
    pdf.rect(doctor_card_x, cards_y - card_height, 1.2 * mm, card_height, fill=True, stroke=False)

    pdf.setFont("Helvetica", 7.5)
    pdf.setFillColor(colors.HexColor("#999999"))
    pdf.drawString(doctor_card_x + 5 * mm, cards_y - 5 * mm, "CONSULTING DOCTOR")
    pdf.setFont("Helvetica-Bold", 12)
    pdf.setFillColor(colors.black)
    pdf.drawString(doctor_card_x + 5 * mm, cards_y - 12 * mm, f"Dr. {_safe(doctor_name)}" if not _safe(doctor_name).startswith("Dr") else _safe(doctor_name))

    # Registration number under doctor name
    if _safe(doctor_registration_no):
        pdf.setFont("Helvetica", 8.5)
        pdf.setFillColor(colors.HexColor("#666666"))
        pdf.drawString(doctor_card_x + 5 * mm, cards_y - 17 * mm, f"Reg. No: {_safe(doctor_registration_no)}")

    # ── Description / Amount table ───────────────────────────
    table_y = cards_y - card_height - 20 * mm

    # Table header row
    header_height = 12 * mm
    pdf.setFillColor(BRAND_TABLE_HEADER)
    pdf.roundRect(left, table_y - header_height, content_width, header_height, 2 * mm, fill=True, stroke=False)

    pdf.setFont("Helvetica-Bold", 9)
    pdf.setFillColor(WHITE)
    pdf.drawString(left + 6 * mm, table_y - 8 * mm, "DESCRIPTION")
    pdf.drawRightString(right - 6 * mm, table_y - 8 * mm, "AMOUNT")

    # Consultation fee row
    row_y = table_y - header_height - 14 * mm
    pdf.setFont("Helvetica", 11)
    pdf.setFillColor(colors.black)
    pdf.drawString(left + 6 * mm, row_y, "Consultation Fee")
    pdf.drawRightString(right - 6 * mm, row_y, _format_amount(consultation_fee))

    # Separator line
    sep_y = row_y - 6 * mm
    pdf.setStrokeColor(colors.HexColor("#E0E0E0"))
    pdf.setLineWidth(0.5)
    pdf.line(left + 4 * mm, sep_y, right - 4 * mm, sep_y)

    # Total row (green background)
    total_row_y = sep_y - 14 * mm
    total_height = 14 * mm
    pdf.setFillColor(colors.HexColor("#E8F5E9"))
    pdf.roundRect(left, total_row_y, content_width, total_height, 2 * mm, fill=True, stroke=False)

    pdf.setFont("Helvetica-Bold", 12)
    pdf.setFillColor(colors.black)
    pdf.drawString(left + 6 * mm, total_row_y + 4 * mm, "Total Paid")
    pdf.setFont("Helvetica-Bold", 16)
    pdf.drawRightString(right - 6 * mm, total_row_y + 3 * mm, _format_amount(consultation_fee))

    # Payment method row
    method_row_y = total_row_y - 16 * mm
    method_height = 12 * mm
    pdf.setFillColor(BRAND_LIGHT_BG)
    pdf.roundRect(left, method_row_y, content_width, method_height, 2 * mm, fill=True, stroke=False)

    pdf.setFont("Helvetica", 8)
    pdf.setFillColor(colors.HexColor("#999999"))
    pdf.drawString(left + 6 * mm, method_row_y + 4 * mm, "PAYMENT METHOD")

    pdf.setFont("Helvetica-Bold", 11)
    pdf.setFillColor(colors.black)
    pdf.drawString(left + 42 * mm, method_row_y + 4 * mm, _safe(payment_method) or "Razorpay")

    # PAID badge
    badge_text = "PAID"
    badge_w = 18 * mm
    badge_h = 7 * mm
    badge_x = right - badge_w - 4 * mm
    badge_y = method_row_y + 2.5 * mm
    pdf.setFillColor(BRAND_ACCENT)
    pdf.roundRect(badge_x, badge_y, badge_w, badge_h, 3 * mm, fill=True, stroke=False)
    pdf.setFont("Helvetica-Bold", 10)
    pdf.setFillColor(WHITE)
    pdf.drawCentredString(badge_x + badge_w / 2, badge_y + 2 * mm, badge_text)

    # ── Thank you section ────────────────────────────────────
    thank_y = method_row_y - 28 * mm
    pdf.setFont("Helvetica-Bold", 16)
    pdf.setFillColor(colors.black)
    thank_text = "Thank you for your payment!"
    pdf.drawCentredString(width / 2, thank_y, thank_text)

    pdf.setFont("Helvetica", 9)
    pdf.setFillColor(colors.HexColor("#777777"))
    pdf.drawCentredString(
        width / 2,
        thank_y - 8 * mm,
        f"For queries, reach us at {CLINIC_EMAIL} or visit {CLINIC_WEBSITE}",
    )

    # ── Footer ───────────────────────────────────────────────
    footer_y = 12 * mm
    pdf.setFont("Helvetica", 7)
    pdf.setFillColor(colors.HexColor("#AAAAAA"))
    pdf.drawCentredString(
        width / 2,
        footer_y,
        "This is a computer-generated receipt and does not require a signature.  |  hpHomeo \u2013 Holistic Healthcare Platform",
    )

    # ── Bottom dark bar ──────────────────────────────────────
    pdf.setFillColor(BRAND_DARK)
    pdf.rect(0, 0, width, 6 * mm, fill=True, stroke=False)

    pdf.save()
    return buf.getvalue()
