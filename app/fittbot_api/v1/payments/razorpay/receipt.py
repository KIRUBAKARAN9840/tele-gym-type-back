from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase.pdfmetrics import registerFontFamily
import os

# Output path (change if you want)
file_path = "Fittbot_Invoice_Final_Clean.pdf"


FONT_REG = "Helvetica"
FONT_BOLD = "Helvetica-Bold"
nirmala_reg = r"C:\Windows\Fonts\Nirmala.ttf"
nirmala_bold = r"C:\Windows\Fonts\NirmalaB.ttf"
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
        FONT_REG = "NirmalaUI"
        FONT_BOLD = "NirmalaUI-Bold"
    except Exception:
        pass

# ---------------------------
# Document & styles
# ---------------------------
doc = SimpleDocTemplate(
    file_path, pagesize=A4, leftMargin=40, rightMargin=40, topMargin=40, bottomMargin=30
)
styles = getSampleStyleSheet()
styles.add(
    ParagraphStyle(
        name="CompanyTitle",
        fontName=FONT_BOLD,
        fontSize=22,
        leading=26,
        textColor=colors.red,
        spaceAfter=12,
        spaceBefore=6,
    )
)
styles.add(ParagraphStyle(name="NormalSmall", fontName=FONT_REG, fontSize=10, leading=14))
styles.add(
    ParagraphStyle(
        name="NormalBold",
        fontName=FONT_BOLD,
        fontSize=10,
        leading=14,
        spaceBefore=8,
        spaceAfter=4,
    )
)

content = []

# ---------------------------
# Header & brand
# ---------------------------25/08/25, 19:58 PM
content.append(Paragraph("25/08/25, 19:58 PM", styles["NormalSmall"]))
content.append(Spacer(1, 8))

content.append(Paragraph("FITTBOT", styles["CompanyTitle"]))

address = """NFCTECH Fitness Private Limited<br/>
No 945, 28th Main Road<br/>
Putlanpalya, Jayanagara 9th Block, Jayanagar<br/>
Bangalore<br/>
Karnataka 560041, India<br/>
Tax number: """
content.append(Paragraph(address, styles["NormalSmall"]))
content.append(Spacer(1, 6))

content.append(Paragraph("naveen@fittbot.com", styles["NormalSmall"]))
content.append(Spacer(1, 12))
content.append(Paragraph("Invoice No. &nbsp;&nbsp;&nbsp; Fitt-000-000-001", styles["NormalSmall"]))
content.append(Spacer(1, 16))

# ---------------------------
# Items table (4 columns)
# Totals labels in "Service Period" column; all amounts in "Amount" column
# ---------------------------
table_data = [
    ["Date", "Description", "Service Period", "Amount"],
    ["12/09/24", "Streaming Service", "12/09/24—11/10/24", "₹168.64"],
    ["", "CGST (9%)", "", "₹15.18"],
    ["", "SGST (9%)", "", "₹15.18"],
    ["", "", "SUBTOTAL", "₹168.64"],
    ["", "", "TAX TOTAL", "₹30.36"],
    ["", "", "TOTAL", "₹199"],
]

# Adjusted widths: push space to Amount column for crisp right alignment
col_widths = [60, 185, 130, 105]  # Date, Description, Service Period, Amount
table = Table(table_data, colWidths=col_widths)

table.setStyle(
    TableStyle(
        [
            # fonts
            ("FONTNAME", (0, 0), (-1, 0), FONT_BOLD),  # header bold
            ("FONTNAME", (0, 1), (-1, -1), FONT_REG),  # body regular
            ("FONTSIZE", (0, 0), (-1, -1), 10),

            # alignment
            ("ALIGN", (0, 0), (-1, 0), "LEFT"),     # header labels left
            ("ALIGN", (3, 0), (3, -1), "RIGHT"),    # Amount column right
            ("ALIGN", (2, 4), (2, 6), "RIGHT"),     # totals labels right in Service Period

            # borders/lines
            ("BOX", (0, 0), (-1, -1), 0.5, colors.black),            # outer border
            ("LINEBELOW", (0, 0), (-1, 0), 0.6, colors.black),       # header rule
            ("LINEBELOW", (0, 1), (-1, 1), 0.4, colors.lightgrey),   # row separators
            ("LINEBELOW", (0, 2), (-1, 2), 0.4, colors.lightgrey),
            ("LINEABOVE", (0, 4), (-1, 4), 0.6, colors.black),       # above SUBTOTAL
            ("LINEABOVE", (0, 6), (-1, 6), 0.6, colors.black),       # above TOTAL

            # emphasize totals
            ("FONTNAME", (2, 4), (2, 6), FONT_BOLD),                 # SUBTOTAL/TAX TOTAL/TOTAL labels
            ("FONTNAME", (3, 6), (3, 6), FONT_BOLD),                 # TOTAL amount bold

            # padding — extra right padding on Amount for a clean edge
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (3, 0), (3, -1), 8),                    # Amount column only
            ("TOPPADDING", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ]
    )
)

content.append(table)
content.append(Spacer(1, 20))

# ---------------------------
# Payment info
# ---------------------------
content.append(Paragraph("Payment Method: Debit Card  ••  •••• 4441", styles["NormalSmall"]))
content.append(Spacer(1, 6))
content.append(Paragraph("Payment Date: 25/08/25, 19:58 PM", styles["NormalSmall"]))
content.append(Spacer(1, 6))
content.append(Paragraph("Reference ID: ", styles["NormalSmall"]))
content.append(Spacer(1, 6))
content.append(Paragraph("Place of Supply: Karnataka", styles["NormalSmall"]))
content.append(Spacer(1, 20))

content.append(Paragraph("https://fittbot.com", styles["NormalSmall"]))

# Build
doc.build(content)
print("Wrote", file_path)
