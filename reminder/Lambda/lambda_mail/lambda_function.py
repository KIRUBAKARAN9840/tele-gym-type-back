import os
import json
import logging
import boto3

from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText

from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Table, TableStyle,
    Spacer, Indenter
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

# Use IAM role credentials (no hardcoded keys needed)
SES_CLIENT = boto3.client("ses", region_name="ap-south-1")
SOURCE_EMAIL = "support@fittbot.com"


def generate_invoice_pdf(data: dict, filename: str) -> None:
    doc = SimpleDocTemplate(
        filename,
        pagesize=A4,
        leftMargin=20*mm, rightMargin=20*mm,
        topMargin=20*mm, bottomMargin=20*mm
    )
    styles = getSampleStyleSheet()
    primary    = colors.HexColor("#ff5757")
    lightpink  = colors.HexColor("#fff1f1")
    lightgrey  = colors.HexColor("#fcfcfc")

    bordercol  = colors.HexColor("#eeeeee")
    gutter     = 12 * mm
    half_w     = (doc.width - gutter) / 2

    # --- Paragraph styles ---
    title = ParagraphStyle("Title", styles["Heading1"],
                           fontSize=20, textColor=primary, spaceAfter=10)
    subtitle = ParagraphStyle("Subtitle", styles["Normal"],
                              fontSize=14, textColor=colors.HexColor("#444444"),
                              italic=True, spaceAfter=10)
    label = ParagraphStyle("Label", styles["Normal"],
                           fontSize=10, textColor=colors.HexColor("#444444"),
                           spaceAfter=2)
    value = ParagraphStyle("Value", styles["Normal"],
                           fontSize=10, textColor=colors.black, spaceAfter=2)
    section = ParagraphStyle("Section", styles["Heading2"],
                             fontSize=14, textColor=primary, spaceAfter=10)
    final_tot = ParagraphStyle("FinalTotal", styles["Heading2"],
                               fontSize=14, textColor=primary, spaceBefore=6)
    thankyou = ParagraphStyle("ThankYou", styles["Normal"],
                              fontSize=10, textColor=primary,
                              alignment=1, spaceBefore=16)

    story = []

    # --- Header ---
    invoice_type = data.get("invoice_type", "")

    if invoice_type == "enquiry_estimate":
        story.append(Paragraph("Estimate", title))
        story.append(Paragraph("Gym Membership Estimate", subtitle))
        story.append(Paragraph(f"Estimate No: {data['invoice_number']}", label))
    else:
        story.append(Paragraph("RECEIPT", title))
        story.append(Paragraph("Gym Membership Receipt", subtitle))
        story.append(Paragraph(f"Invoice No: {data['invoice_number']}", label))

    # underline
    ul = Table([[""]], colWidths=[doc.width])
    ul.setStyle(TableStyle([
        ("LINEBELOW", (0,0), (-1,-1), 2, primary),
        ("BOTTOMPADDING", (0,0), (-1,-1), 2),
    ]))
    story.append(ul)
    story.append(Spacer(1, 9*mm))

    # --- CLIENT box style ---
    if data.get("client_email") is not None:
        client_rows = [
            [Paragraph("CLIENT",
                    ParagraphStyle("h", styles["Normal"],
                                    fontSize=12, textColor=primary,
                                    fontName="Helvetica-Bold")),
            ""],
            [Paragraph("Name:", label), Paragraph(data["client_name"], value)],
            [Paragraph("Contact:", label), Paragraph(data["client_contact"], value)],
            [Paragraph("Email:", label), Paragraph(data["client_email"], value)],
        ]
    else:
        client_rows = [
            [Paragraph("CLIENT",
                    ParagraphStyle("h", styles["Normal"],
                                    fontSize=12, textColor=primary,
                                    fontName="Helvetica-Bold")),
            ""],
            [Paragraph("Name:", label), Paragraph(data["client_name"], value)],
            [Paragraph("Contact:", label), Paragraph(data["client_contact"], value)]
        ]

    client_tbl = Table(client_rows, colWidths=[30*mm, half_w-30*mm])
    client_tbl.setStyle(TableStyle([
        ("BACKGROUND",   (0,0), (-1,-1),     lightpink),
        ("LINEBEFORE",   (0,0), (0,-1),      4, primary),
        ("LINEBELOW",    (0,0), (-1,0),      0.5, bordercol),
        ("LEFTPADDING",  (0,0), (-1,-1),     6),
        ("RIGHTPADDING", (0,0), (-1,-1),     6),
        ("TOPPADDING",   (0,0), (-1,-1),     6),
        ("BOTTOMPADDING",(0,0), (-1,-1),     6),
    ]))

    # --- PROVIDER box style ---
    gst_type = data.get("gst_type", "")

    if gst_type == "no_gst":
        prov_rows = [
            [Paragraph("PROVIDER",
                    ParagraphStyle("h2", styles["Normal"],
                                    fontSize=12, textColor=primary,
                                    fontName="Helvetica-Bold")),
            ""],
            [Paragraph("Gym:", label), Paragraph(data["gym_name"], value)],
            [Paragraph("Location:", label), Paragraph(data["gym_location"], value)],
            [Paragraph("Contact:", label), Paragraph(data["gym_contact"], value)],
        ]
    else:
        prov_rows = [
            [Paragraph("PROVIDER",
                    ParagraphStyle("h2", styles["Normal"],
                                    fontSize=12, textColor=primary,
                                    fontName="Helvetica-Bold")),
            ""],
            [Paragraph("Gym:", label), Paragraph(data["gym_name"], value)],
            [Paragraph("Location:", label), Paragraph(data["gym_location"], value)],
            [Paragraph("Contact:", label), Paragraph(data["gym_contact"], value)],
            [Paragraph("GST No:", label), Paragraph(data["gst_number"], value)]
        ]

    prov_tbl = Table(prov_rows, colWidths=[30*mm, half_w-30*mm])
    prov_tbl.setStyle(TableStyle([
        ("BACKGROUND",   (0,0), (-1,-1),     colors.white),
        ("BOX",          (0,0), (-1,-1),     1, bordercol),
        ("LINEBELOW",    (0,0), (-1,0),      0.5, bordercol),
        ("LEFTPADDING",  (0,0), (-1,-1),     6),
        ("RIGHTPADDING", (0,0), (-1,-1),     6),
        ("TOPPADDING",   (0,0), (-1,-1),     6),
        ("BOTTOMPADDING",(0,0), (-1,-1),     6),
    ]))

    discounted_price = int(data.get("discounted_price", 0))
    info_tbl = Table([[client_tbl, "", prov_tbl]],
                     colWidths=[half_w, gutter, half_w])
    info_tbl.setStyle(TableStyle([("VALIGN", (0,0), (-1,-1), "TOP")]))
    story.append(info_tbl)
    story.append(Spacer(1, 11*mm))

    # --- SERVICES ---
    story.append(Paragraph("Services", section))
    indent_amt = half_w + gutter
    story.append(Indenter(left=10))

    invoice_type = data.get("invoice_type", "")

    if invoice_type == "enquiry_estimate":
        svc_data = [
            ["Description", "HSN Code", "Admission fees", "Actual Price", "Discount", "Discounted Price"],
            [data["plan_description"], "999723", f"Rs. {data['admission_fees']:,.0f}", f"Rs. {data['fees']:,.0f}", f"Rs. {data['discount']:,.0f}", f"Rs. {data['discounted_price']:,.0f}"]
        ]
        svc_tbl = Table(
            svc_data,
            colWidths=[doc.width * 0.15, doc.width * 0.15, doc.width * 0.15, doc.width * 0.2, doc.width * 0.15, doc.width * 0.2]
        )
        svc_tbl.setStyle(TableStyle([
            ("BACKGROUND",   (0,0), (-1,0),   primary),
            ("TEXTCOLOR",    (0,0), (-1,0),   colors.white),
            ("ALIGN",        (2,1), (2,1),    "RIGHT"),
            ("GRID",         (0,0), (-1,-1),  1, bordercol),
            ("LEFTPADDING",  (0,0), (-1,-1),  6),
            ("RIGHTPADDING", (0,0), (-1,-1),  6),
            ("TOPPADDING",   (0,0), (-1,-1),  4),
            ("BOTTOMPADDING",(0,0), (-1,-1),  4),
        ]))

    elif discounted_price > 0:
        dp = True
        svc_data = [
            ["Description", "HSN Code", "Actual Price", "Discount", "Discounted Price"],
            [data["plan_description"], "999723", f"Rs. {data['fees']:,.0f}", f"Rs. {data['discounted_price']:,.0f}", f"Rs. {data['discounted_fees']:,.0f}"]
        ]
        svc_tbl = Table(
            svc_data,
            colWidths=[doc.width * 0.3, doc.width * 0.15, doc.width * 0.2, doc.width * 0.15, doc.width * 0.2]
        )
        svc_tbl.setStyle(TableStyle([
            ("BACKGROUND",   (0,0), (-1,0),   primary),
            ("TEXTCOLOR",    (0,0), (-1,0),   colors.white),
            ("ALIGN",        (2,1), (2,1),    "RIGHT"),
            ("GRID",         (0,0), (-1,-1),  1, bordercol),
            ("LEFTPADDING",  (0,0), (-1,-1),  6),
            ("RIGHTPADDING", (0,0), (-1,-1),  6),
            ("TOPPADDING",   (0,0), (-1,-1),  4),
            ("BOTTOMPADDING",(0,0), (-1,-1),  4),
        ]))

    else:
        dp = False
        svc_data = [
            ["Description", "HSN Code", "Price"],
            [data["plan_description"], "999723", f"Rs. {data['fees']:,.0f}"]
        ]
        svc_tbl = Table(
            svc_data,
            colWidths=[doc.width * 0.4, doc.width * 0.3, doc.width * 0.3]
        )
        svc_tbl.setStyle(TableStyle([
            ("BACKGROUND",   (0,0), (-1,0),   primary),
            ("TEXTCOLOR",    (0,0), (-1,0),   colors.white),
            ("ALIGN",        (2,1), (2,1),    "RIGHT"),
            ("GRID",         (0,0), (-1,-1),  1, bordercol),
            ("LEFTPADDING",  (0,0), (-1,-1),  6),
            ("RIGHTPADDING", (0,0), (-1,-1),  6),
            ("TOPPADDING",   (0,0), (-1,-1),  4),
            ("BOTTOMPADDING",(0,0), (-1,-1),  4),
        ]))

    story.append(svc_tbl)
    story.append(Indenter(left=-10))
    story.append(Spacer(1, 13*mm))

    # --- PAYMENT INFO style ---
    invoice_type = data.get("invoice_type", "")

    if invoice_type == "receipt":
        pay_rows = [
            [Paragraph("PAYMENT INFORMATION",
                    ParagraphStyle("h3", styles["Normal"],
                                    fontSize=12, textColor=primary,
                                    fontName="Helvetica-Bold")),
            ""],
            [Paragraph("Method:", label), Paragraph(data["payment_method"], value)],
        ]
        if data.get("payment_date"):
            pay_rows.append([Paragraph("Date:", label),
                            Paragraph(str(data["payment_date"]), value)])
        if data.get("payment_reference_number"):
            pay_rows.append([Paragraph("Ref:", label),
                            Paragraph(data["payment_reference_number"], value)])
        pay_tbl = Table(pay_rows, colWidths=[30*mm, half_w-30*mm])
        pay_tbl.setStyle(TableStyle([
            ("SPAN",        (0,0), (1,0)),
            ("BACKGROUND",   (0,0), (-1,-1),     colors.white),
            ("BOX",          (0,0), (-1,-1),     1, bordercol),
            ("LINEBELOW",    (0,0), (-1,0),      0.5, bordercol),
            ("LEFTPADDING",  (0,0), (-1,-1),     6),
            ("RIGHTPADDING", (0,0), (-1,-1),     6),
            ("TOPPADDING",   (0,0), (-1,-1),     6),
            ("BOTTOMPADDING",(0,0), (-1,-1),     6),
        ]))

    else:
        pay_rows = [
            [Paragraph("PAYMENT INFORMATION",
                    ParagraphStyle("h3", styles["Normal"],
                                    fontSize=12, textColor=primary,
                                    fontName="Helvetica-Bold")),
            ""]
        ]

        if data.get("account_holder"):
            pay_rows.append([Paragraph("Account Holder Name:", label),
                            Paragraph(data["account_holder"], value)])

        if data.get("bank_name"):
            pay_rows.append([Paragraph("Bank Details:", label),
                            Paragraph(data["bank_name"], value)])

        if data.get("account_number"):
            pay_rows.append([Paragraph("Account Number:", label),
                            Paragraph(data["bank_details"], value)])

        if data.get("ifsc_code"):
            pay_rows.append([Paragraph("IFSC Code:", label),
                            Paragraph(data["ifsc_code"], value)])

        if data.get("branch"):
            pay_rows.append([Paragraph("Branch:", label),
                            Paragraph(data["branch"], value)])

        if data.get("upi_id"):
            pay_rows.append([Paragraph("UPI ID:", label),
                            Paragraph(data["upi_id"], value)])

        pay_tbl = Table(pay_rows, colWidths=[30*mm, half_w-30*mm])
        pay_tbl.setStyle(TableStyle([
            ("SPAN",        (0,0), (1,0)),
            ("BACKGROUND",   (0,0), (-1,-1),     colors.white),
            ("BOX",          (0,0), (-1,-1),     1, bordercol),
            ("LINEBELOW",    (0,0), (-1,0),      0.5, bordercol),
            ("LEFTPADDING",  (0,0), (-1,-1),     6),
            ("RIGHTPADDING", (0,0), (-1,-1),     6),
            ("TOPPADDING",   (0,0), (-1,-1),     6),
            ("BOTTOMPADDING",(0,0), (-1,-1),     6),
        ]))

    # --- COST SUMMARY style ---
    fees = int(data["fees"])
    discount = int(data.get("discount", 0))
    discounted = int(data.get("discounted_price", fees))
    gst_rate = 0
    if data.get("gst_percentage"):
        gst_rate = int(data.get("gst_percentage"))
    individual_gst = gst_rate / 200
    base_price = 1 - gst_rate / 100

    admission_fee = int(data.get("admission_fees", 0))

    if gst_type == "inclusive":
        if discounted_price > 0:
            cost_rows = [
                [
                    Paragraph(
                        "COST SUMMARY",
                        ParagraphStyle(
                            "h4",
                            styles["Normal"],
                            fontSize=12,
                            textColor=primary,
                            fontName="Helvetica-Bold"
                        )
                    ),
                    ""
                ],
                [Paragraph("Base Price:", label), Paragraph(f"Rs. {(discounted) * base_price:,.0f}", value)],
                [Paragraph(f"CGST ({round(gst_rate/2,2)}%):", label), Paragraph(f"Rs. {(discounted) * individual_gst:,.0f}", value)],
                [Paragraph(f"SGST ({round(gst_rate/2,2)}%):", label), Paragraph(f"Rs. {(discounted) * individual_gst:,.0f}", value)],
                [Paragraph("Total:", label), Paragraph(f"Rs. {(discounted):,.0f}", value)],
            ]
            cost_rows.append([
                Paragraph("Amount Paid:", final_tot),
                Paragraph(f"Rs. {(discounted):,.0f}", final_tot)
            ])

        else:
            cost_rows = [
                [
                    Paragraph(
                        "COST SUMMARY",
                        ParagraphStyle(
                            "h4",
                            styles["Normal"],
                            fontSize=12,
                            textColor=primary,
                            fontName="Helvetica-Bold"
                        )
                    ),
                    ""
                ],
                [Paragraph("Base Price:", label), Paragraph(f"Rs. {(fees) * base_price:,.0f}", value)],
                [Paragraph("CGST (9%):", label), Paragraph(f"Rs. {(fees) * individual_gst:,.0f}", value)],
                [Paragraph("SGST (9%):", label), Paragraph(f"Rs. {(fees) * individual_gst:,.0f}", value)],
                [Paragraph("Total:", label), Paragraph(f"Rs. {(fees):,.0f}", value)],
            ]
            cost_rows.append([
                Paragraph("Amount Paid:", final_tot),
                Paragraph(f"Rs. {(fees):,.0f}", final_tot)
            ])

    elif gst_type == "exclusive":
        if discounted_price > 0:
            cost_rows = [
                [
                    Paragraph(
                        "COST SUMMARY",
                        ParagraphStyle(
                            "h4",
                            styles["Normal"],
                            fontSize=12,
                            textColor=primary,
                            fontName="Helvetica-Bold"
                        )
                    ),
                    ""
                ],
                [Paragraph("Base Price:", label), Paragraph(f"Rs. {(discounted):,.0f}", value)],
                [Paragraph("CGST (9%):", label), Paragraph(f"Rs. {(discounted) * individual_gst:,.0f}", value)],
                [Paragraph("SGST (9%):", label), Paragraph(f"Rs. {(discounted) * individual_gst:,.0f}", value)],
                [Paragraph("Total:", label), Paragraph(f"Rs. {(discounted) * 1.18:,.0f}", value)],
            ]
            cost_rows.append([
                Paragraph("Amount Paid:", final_tot),
                Paragraph(f"Rs. {(discounted) * 1.18:,.0f}", final_tot)
            ])

        else:
            cost_rows = [
                [
                    Paragraph(
                        "COST SUMMARY",
                        ParagraphStyle(
                            "h4",
                            styles["Normal"],
                            fontSize=12,
                            textColor=primary,
                            fontName="Helvetica-Bold"
                        )
                    ),
                    ""
                ],
                [Paragraph("Base Price:", label), Paragraph(f"Rs. {(fees):,.0f}", value)],
                [Paragraph("CGST (9%):", label), Paragraph(f"Rs. {(fees) * individual_gst:,.0f}", value)],
                [Paragraph("SGST (9%):", label), Paragraph(f"Rs. {(fees) * individual_gst:,.0f}", value)],
                [Paragraph("Total:", label), Paragraph(f"Rs. {(fees) * 1.18:,.0f}", value)],
            ]
            cost_rows.append([
                Paragraph("Amount Paid:", final_tot),
                Paragraph(f"Rs. {(fees) * 1.18:,.0f}", final_tot)
            ])

    elif gst_type == "no_gst":
        if discounted_price > 0:
            cost_rows = [
                [
                    Paragraph(
                        "COST SUMMARY",
                        ParagraphStyle(
                            "h4",
                            styles["Normal"],
                            fontSize=12,
                            textColor=primary,
                            fontName="Helvetica-Bold"
                        )
                    ),
                    ""
                ],
                [Paragraph("Total Price:", label), Paragraph(f"Rs. {(discounted):,.0f}", value)],
            ]
            cost_rows.append([
                Paragraph("Amount Paid:", final_tot),
                Paragraph(f"Rs.{(discounted):,.0f}", final_tot)
            ])

        else:
            cost_rows = [
                [
                    Paragraph(
                        "COST SUMMARY",
                        ParagraphStyle(
                            "h4",
                            styles["Normal"],
                            fontSize=12,
                            textColor=primary,
                            fontName="Helvetica-Bold"
                        )
                    ),
                    ""
                ],
                [Paragraph("Total Price:", label), Paragraph(f"Rs. {fees:,.0f}", value)],
            ]
            cost_rows.append([
                Paragraph("Amount Paid:", final_tot),
                Paragraph(f"Rs.{fees:,.0f}", final_tot)
            ])

    cost_tbl = Table(
        cost_rows,
        colWidths=[40*mm, half_w - 30*mm]
    )
    cost_tbl.setStyle(TableStyle([
        ("SPAN",        (0,0), (1,0)),
        ("BACKGROUND",  (0,0), (-1,-1),       colors.white),
        ("BOX",         (0,0), (-1,-1),       1, bordercol),
        ("LINEBELOW",   (0,0), (-1,0),        0.5, bordercol),
        ("LEFTPADDING", (0,0), (-1,-1),       6),
        ("RIGHTPADDING",(0,0), (-1,-1),       6),
        ("TOPPADDING",  (0,0), (-1,-1),       6),
        ("BOTTOMPADDING",(0,0),(-1,-1),       6),
    ]))

    # --- Place PAYMENT & COST side by side ---
    pc_tbl = Table(
        [[pay_tbl, "", cost_tbl]],
        colWidths=[half_w, gutter, half_w]
    )
    pc_tbl.setStyle(TableStyle([("VALIGN", (0,0), (-1,-1), "TOP")]))
    story.append(pc_tbl)
    story.append(Spacer(1, 16*mm))

    # --- Thank you note ---
    if invoice_type != "enquiry_estimate":
        story.append(Paragraph(
            f"Payment received. Thank you for choosing {data['gym_name']}!",
            thankyou
        ))

    doc.build(story)


def send_email_with_attachment(recipient: str, subject: str, body_text: str, attachment_path: str) -> None:
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = SOURCE_EMAIL
    msg['To'] = recipient

    msg.attach(MIMEText(body_text, 'plain'))

    with open(attachment_path, 'rb') as f:
        part = MIMEApplication(f.read())
        part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(attachment_path))
        msg.attach(part)

    SES_CLIENT.send_raw_email(
        Source=SOURCE_EMAIL,
        Destinations=[recipient],
        RawMessage={'Data': msg.as_string()}
    )
    LOGGER.info(f"Email sent to {recipient}")


def lambda_handler(event, context):
    try:
        invoice_data = event['invoice_data']
        recipient = invoice_data.get('client_email')
        invoice_no = invoice_data.get('invoice_number', 'receipt')
        pdf_path = f"/tmp/{invoice_no}.pdf"

        generate_invoice_pdf(invoice_data, pdf_path)
        LOGGER.info(f"Generated PDF at {pdf_path}")

        subject = f"Your Receipt from {invoice_data.get('gym_name')}"
        body = "Please find attached your gym membership receipt. Thank you for your business!"
        send_email_with_attachment(recipient, subject, body, pdf_path)

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Receipt sent successfully"})
        }

    except Exception as e:
        LOGGER.exception("Error in lambda_handler")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": str(e)})
        }
