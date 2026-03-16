import os
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase.pdfmetrics import registerFontFamily

from app.models.fittbot_models import Client
from ..models.payments import Payment
from ..models.subscriptions import Subscription


def _ensure_fonts() -> tuple[str, str]:
    font_reg = "Helvetica"
    font_bold = "Helvetica-Bold"
    nirmala_reg = r"C:\\Windows\\Fonts\\Nirmala.ttf"
    nirmala_bold = r"C:\\Windows\\Fonts\\NirmalaB.ttf"
    if os.path.exists(nirmala_reg) and os.path.exists(nirmala_bold):
        try:
            pdfmetrics.registerFont(TTFont("NirmalaUI", nirmala_reg))
            pdfmetrics.registerFont(TTFont("NirmalaUI-Bold", nirmala_bold))
            registerFontFamily(
                "NirmalaUI",
                normal="NirmalaUI",
                bold="NirmalaUI-Bold",
                italic="NirmalaUI",
                boldItalic="NirmalaUI-Bold",
            )
            font_reg = "NirmalaUI"
            font_bold = "NirmalaUI-Bold"
        except Exception:
            pass
    return font_reg, font_bold


def _gst_breakup_inclusive(total_rupees: float, gst_rate: float = 0.18) -> tuple[float, float, float]:
    if total_rupees is None:
        return 0.0, 0.0, 0.0
    base = round(total_rupees / (1.0 + gst_rate), 2)
    cgst = round(base * (gst_rate / 2), 2)
    sgst = round(base * (gst_rate / 2), 2)
    return base, cgst, sgst


def generate_receipt_pdf(
    *,
    invoice_no: str,
    company_email: str,
    client_name: str,
    client_email: Optional[str],
    client_contact: Optional[str],
    description: str,
    service_start: Optional[datetime],
    service_end: Optional[datetime],
    total_minor: int,
    currency: str,
    reference_id: Optional[str],
    payment_method: Optional[str],
    place_of_supply: str = "Karnataka",
    out_dir: str = "tmp",
) -> str:
    os.makedirs(out_dir, exist_ok=True)
    file_path = os.path.join(out_dir, f"{invoice_no}.pdf")

    font_reg, font_bold = _ensure_fonts()

    doc = SimpleDocTemplate(
        file_path, pagesize=A4, leftMargin=40, rightMargin=40, topMargin=40, bottomMargin=30
    )
    styles = getSampleStyleSheet()
    styles.add(
        ParagraphStyle(
            name="CompanyTitle",
            fontName=font_bold,
            fontSize=22,
            leading=26,
            textColor=colors.red,
            spaceAfter=12,
            spaceBefore=6,
        )
    )
    styles.add(ParagraphStyle(name="NormalSmall", fontName=font_reg, fontSize=10, leading=14))
    styles.add(
        ParagraphStyle(
            name="NormalBold",
            fontName=font_bold,
            fontSize=10,
            leading=14,
            spaceBefore=8,
            spaceAfter=4,
        )
    )

    content: list = []
    # Use service start date instead of current date
    bill_date_str = service_start.strftime("%d/%m/%Y, %H:%M") if service_start else datetime.now().strftime("%d/%m/%Y, %H:%M")
    

    # Header block: logo on left; TAX INVOICE and right-aligned Invoice No on right
    styles.add(ParagraphStyle(name="RightSmall", fontName=font_reg, fontSize=10, leading=14, alignment=2))
    styles.add(ParagraphStyle(name="InvoiceHeader", fontName=font_bold, fontSize=16, leading=18, alignment=2))

    logo_path = os.path.join(os.path.dirname(__file__), "logo.png")
    if os.path.exists(logo_path):
        left_top = Image(logo_path, width=120, height=100)
        #left_top = Image(logo_path, width=140, height=35)
    else:
        left_top = Paragraph("FITTBOT", styles["CompanyTitle"])  # fallback
    right_top = Paragraph("TAX INVOICE", styles["InvoiceHeader"]) 
    right_bottom = Paragraph(f"Invoice No: <b>{invoice_no}</b>", styles["RightSmall"]) 

    header_table = Table(
        [[left_top, right_top],
         [Paragraph("", styles["NormalSmall"]), right_bottom],
         [Paragraph("", styles["NormalSmall"]), Paragraph(bill_date_str, styles["RightSmall"]) ]],
        colWidths=[300, 200],
    )
    header_table.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN", (1, 0), (1, 1), "RIGHT"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))
    content.append(header_table)

    company_address = (
        "NFCTECH Fitness Private Limited<br/>"
        "No 945, 28th Main Road<br/>"
        "Putlanpalya, Jayanagara 9th Block, Jayanagar<br/>"
        "Bangalore<br/>"
        "Karnataka 560041, India<br/>"
        "GSTIN: 29AAKCN1522H1ZG<br/>"
        f"{company_email}"
    )
    billed_details = [
        client_name or "-",
        client_email or "-",
        client_contact or "-",
        "Bangalore",
    ]
    billed_text = "<b>Billed To</b><br/>" + "<br/>".join(billed_details)
    address_table = Table(
        [
            [
                Paragraph(company_address, styles["NormalSmall"]),
                Paragraph(billed_text, styles["NormalSmall"]),
            ]
        ],
        colWidths=[300, 200],
    )
    address_table.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                ("TOPPADDING", (0, 0), (-1, -1), 0),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
            ]
        )
    )
    content.append(address_table)
    content.append(Spacer(1, 12))

    total_rupees = round((total_minor or 0) / 100.0, 2)
    # Use exact GST breakdown for 199 plan: base=163.18, cgst=sgst=17.91 each
    if total_rupees == 199.00:
        base, cgst, sgst = 163.18, 17.91, 17.91
    else:
        base, cgst, sgst = _gst_breakup_inclusive(total_rupees, 0.18)

    svc_period = (
        f"{service_start.strftime('%d/%m/%Y')}–{service_end.strftime('%d/%m/%Y')}"
        if service_start and service_end
        else "N/A"
    )

    table_data = [
        ["Date", "Description", "Service Period", "Amount"],
        [service_start.strftime("%d/%m/%Y") if service_start else datetime.now().strftime("%d/%m/%Y"), description, svc_period, f"₹ {base:,.2f}"],
        ["", "CGST (9%)", "", f"₹ {cgst:,.2f}"],
        ["", "SGST (9%)", "", f"₹ {sgst:,.2f}"],
        ["", "", "SUBTOTAL", f"₹ {base:,.2f}"],
        ["", "", "TAX TOTAL", f"₹ {cgst+sgst:,.2f}"],
        ["", "", "TOTAL", f"₹ {total_rupees:,.2f}"],
    ]

    col_widths = [60, 185, 130, 105]
    table = Table(table_data, colWidths=col_widths)
    table.setStyle(
        TableStyle(
            [
                ("FONTNAME", (0, 0), (-1, 0), font_bold),
                ("FONTNAME", (0, 1), (-1, -1), font_reg),
                ("FONTSIZE", (0, 0), (-1, -1), 10),
                ("ALIGN", (0, 0), (-1, 0), "LEFT"),
                ("ALIGN", (3, 0), (3, -1), "RIGHT"),
                ("ALIGN", (2, 4), (2, 6), "RIGHT"),
                ("BOX", (0, 0), (-1, -1), 0.5, colors.black),
                ("LINEBELOW", (0, 0), (-1, 0), 0.6, colors.black),
                ("LINEBELOW", (0, 1), (-1, 1), 0.4, colors.lightgrey),
                ("LINEBELOW", (0, 2), (-1, 2), 0.4, colors.lightgrey),
                ("LINEABOVE", (0, 4), (-1, 4), 0.6, colors.black),
                ("LINEABOVE", (0, 6), (-1, 6), 0.6, colors.black),
                ("FONTNAME", (2, 4), (2, 6), font_bold),
                ("FONTNAME", (3, 6), (3, 6), font_bold),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (3, 0), (3, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    content.append(table)
    content.append(Spacer(1, 20))

    method_disp = (payment_method or 'unknown').upper()
    content.append(Paragraph(f"Payment Method: {method_disp}", styles["NormalSmall"]))
    content.append(Spacer(1, 6))
    content.append(Paragraph(f"Reference ID: {reference_id or ''}", styles["NormalSmall"]))
    content.append(Spacer(1, 6))
    content.append(Spacer(1, 20))
    content.append(Paragraph("Website: https://fittbotbusiness.com", styles["NormalSmall"]))

    doc.build(content)
    return file_path


def _smtp_send(to_email: str, pdf_path: str) -> bool:
    import smtplib
    from email.message import EmailMessage

    sender_email = os.getenv("SMTP_EMAIL")
    sender_password = os.getenv("SMTP_PASSWORD")
    server_host = os.getenv("SMTP_SERVER")
    server_port = int(os.getenv("SMTP_PORT") or 587)

    msg = EmailMessage()
    msg["Subject"] = "Your Invoice from Fittbot"
    msg.set_content("Dear Team,\n\nPlease find attached the invoice.\n\nRegards,\nFittbot")
    msg["From"] = sender_email
    msg["To"] = to_email

    with open(pdf_path, "rb") as f:
        data = f.read()
        msg.add_attachment(data, maintype="application", subtype="pdf", filename=os.path.basename(pdf_path))

    try:
        server = smtplib.SMTP(server_host, server_port)
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)
        server.quit()
        return True
    except Exception:
        return False


def send_subscription_receipt(
    db: Session,
    *,
    customer_id: str,
    subscription: Subscription,
    payment: Optional[Payment],
    invoice_no: Optional[str] = None,
    to_email: str = "martin@fittbot.com",
) -> Optional[str]:
    client = db.query(Client).filter(Client.client_id == customer_id).first()
    client_name = getattr(client, "name", None) or f"Client {customer_id}"
    client_email = getattr(client, "email", None)
    client_contact = getattr(client, "contact", None)

    total_minor = payment.amount_minor if payment else 0
    method = None
    try:
        meta = getattr(payment, "payment_metadata", None) or {}
        method = meta.get("method") or meta.get("type")
    except Exception:
        method = None

    inv_no = invoice_no or datetime.now().strftime("2025-0000-0001")

    # Prefer Razorpay order_id stored in payment metadata as reference, else fallback to provider_payment_id
    ref_id = None
    try:
        if payment:
            meta = getattr(payment, "payment_metadata", {}) or {}
            ref_id = payment.order_id
    except Exception:
        ref_id = getattr(payment, "provider_payment_id", None) if payment else None

    pdf_path = generate_receipt_pdf(
        invoice_no=inv_no,
        company_email="support@fittbot.com",
        client_name=client_name,
        client_email=client_email,
        client_contact=client_contact,
        description="Fittbot Business App",
        service_start=subscription.active_from,
        service_end=subscription.active_until,
        total_minor=total_minor,
        currency=payment.currency if payment else "INR",
        reference_id=ref_id,
        payment_method=method,
    )

    ok = _smtp_send(to_email, pdf_path)
    return pdf_path if ok else None


def send_receipt_with_amount(
    db: Session,
    *,
    customer_id: str,
    subscription: Subscription,
    total_minor: int,
    currency: str,
    reference_id: Optional[str],
    method: Optional[str],
    invoice_no: str,
    to_email: str = "martin@fittbot.com",
) -> Optional[str]:
    """Generate and send a one-off receipt using explicit amount/method.
    Use this when a Payment row is not yet recorded (e.g., payment.authorized).
    """
    client = db.query(Client).filter(Client.client_id == customer_id).first()
    client_name = getattr(client, "name", None) or f"Client {customer_id}"
    client_email = getattr(client, "email", None)
    client_contact = getattr(client, "contact", None)

    pdf_path = generate_receipt_pdf(
        invoice_no=invoice_no,
        company_email="naveen@fittbot.com",
        client_name=client_name,
        client_email=client_email,
        client_contact=client_contact,
        description="Fittbot Business App",
        service_start=subscription.active_from,
        service_end=subscription.active_until,

        total_minor=total_minor,
        currency=currency or "INR",
        reference_id=reference_id,
        payment_method=method,
        place_of_supply="Karnataka",
    )

    ok = _smtp_send(to_email, pdf_path)
    return pdf_path if ok else None
