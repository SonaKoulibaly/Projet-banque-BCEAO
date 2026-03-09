"""
utils/pdf_generator.py — Générateur de vrais rapports PDF avec ReportLab
"""
import io, math, os
from datetime import datetime
import pandas as pd
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable, Image
from reportlab.pdfgen import canvas as rl_canvas

NAVY    = colors.HexColor("#080C1A")
NAVY2   = colors.HexColor("#0D1530")
COPPER  = colors.HexColor("#C87941")
COPPER2 = colors.HexColor("#E8A96A")
CREAM   = colors.HexColor("#F5F0E8")
SUCCESS = colors.HexColor("#2ECC71")
DANGER  = colors.HexColor("#E74C3C")
MUTED   = colors.HexColor("#7A8FAA")
WHITE   = colors.white
LIGHT   = colors.HexColor("#F0EBE0")

def safe_val(v, pct=False, decimals=0):
    try:
        f = float(v)
        if math.isnan(f): return "N/D"
        if pct: return f"{f:.2f}%"
        if abs(f) >= 1_000_000: return f"{f/1_000_000:.2f} Mds"
        if abs(f) >= 1_000: return f"{f/1_000:.{decimals}f} K"
        return f"{f:.{decimals}f}"
    except: return "N/D"

class NumberedCanvas(rl_canvas.Canvas):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._saved_page_states = []
    def showPage(self):
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()
    def save(self):
        num_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            self._draw_footer(num_pages)
            super().showPage()
        super().save()
    def _draw_footer(self, page_count):
        w, h = A4
        self.setFillColor(NAVY)
        self.rect(0, 0, w, 1.2*cm, fill=1, stroke=0)
        self.setFillColor(COPPER)
        self.setFont("Helvetica", 7.5)
        self.drawString(1.5*cm, 0.45*cm, "Dashboard Banques Senegal · Source BCEAO · Python · Dash · MongoDB Atlas")
        self.setFillColor(CREAM)
        self.drawRightString(w - 1.5*cm, 0.45*cm, f"Page {self._pageNumber} / {page_count}")
        self.setStrokeColor(COPPER)
        self.setLineWidth(1.5)
        self.line(1.5*cm, h - 1.8*cm, w - 1.5*cm, h - 1.8*cm)

def generate_bank_pdf(df_global, sigle, annee):
    buf = io.BytesIO()
    logo_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "assets", "logo.png")
    doc = SimpleDocTemplate(buf, pagesize=A4,
        leftMargin=1.5*cm, rightMargin=1.5*cm, topMargin=2.5*cm, bottomMargin=2*cm,
        title=f"Rapport {sigle} - {annee}", author="Dashboard Banques Senegal")
    styles = getSampleStyleSheet()
    story  = []

    title_s   = ParagraphStyle("T", fontName="Helvetica-Bold", fontSize=20, textColor=NAVY, spaceAfter=4)
    sub_s     = ParagraphStyle("S", fontName="Helvetica", fontSize=10, textColor=MUTED, spaceAfter=10)
    section_s = ParagraphStyle("Sec", fontName="Helvetica-Bold", fontSize=12, textColor=NAVY2, spaceBefore=14, spaceAfter=5)
    body_s    = ParagraphStyle("B", fontName="Helvetica", fontSize=9, textColor=colors.HexColor("#333333"), leading=13)
    kpi_l_s   = ParagraphStyle("KL", fontName="Helvetica", fontSize=7.5, textColor=MUTED, alignment=TA_CENTER)

    df_b    = df_global[df_global["sigle"] == sigle].sort_values("annee")
    df_year = df_global[(df_global["sigle"] == sigle) & (df_global["annee"] == annee)]
    if df_b.empty:
        story.append(Paragraph(f"Aucune donnee pour {sigle}", styles["Normal"]))
        doc.build(story)
        return buf.getvalue()

    row       = df_year.iloc[0] if not df_year.empty else df_b.iloc[-1]
    groupe    = str(row.get("groupe_bancaire", "N/D"))
    bilan     = pd.to_numeric(row.get("bilan"), errors="coerce")
    rn        = pd.to_numeric(row.get("resultat_net"), errors="coerce")
    fp        = pd.to_numeric(row.get("fonds_propres"), errors="coerce")
    ressources= pd.to_numeric(row.get("ressources"), errors="coerce")
    emploi    = pd.to_numeric(row.get("emploi"), errors="coerce")
    effectif  = pd.to_numeric(row.get("effectif"), errors="coerce")
    agences   = pd.to_numeric(row.get("agences"), errors="coerce")
    pnb_v     = pd.to_numeric(row.get("pnb"), errors="coerce")
    charges   = pd.to_numeric(row.get("charges_generales"), errors="coerce")
    roa = (float(rn)/float(bilan)*100) if (pd.notna(bilan) and float(bilan or 0) != 0 and pd.notna(rn)) else None
    roe = (float(rn)/float(fp)*100)    if (pd.notna(fp) and float(fp or 0) != 0 and pd.notna(rn)) else None
    cir = (float(charges)/float(pnb_v)*100) if (pd.notna(pnb_v) and float(pnb_v or 0) != 0 and pd.notna(charges)) else None

    # Header
    if os.path.exists(logo_path):
        try:
            logo_img = Image(logo_path, width=2.8*cm, height=2.8*cm)
            hd = [[logo_img, [Paragraph("Rapport de Positionnement", sub_s),
                              Paragraph(sigle, title_s),
                              Paragraph(f"Groupe : {groupe}  |  Annee : {annee}  |  {datetime.now().strftime('%d/%m/%Y')}", sub_s)]]]
            ht = Table(hd, colWidths=[3.2*cm, 14*cm])
            ht.setStyle(TableStyle([("VALIGN",(0,0),(-1,-1),"MIDDLE"),("LEFTPADDING",(0,0),(-1,-1),0)]))
            story.append(ht)
        except:
            story.append(Paragraph(f"Rapport - {sigle}", title_s))
    else:
        story.append(Paragraph(f"Rapport - {sigle}", title_s))

    story.append(HRFlowable(width="100%", thickness=2, color=COPPER, spaceAfter=10))

    # KPIs
    def kpi_cell(lbl, val, col=COPPER):
        return [Paragraph(lbl, kpi_l_s),
                Paragraph(val, ParagraphStyle("kv", fontName="Helvetica-Bold", fontSize=13, textColor=col, alignment=TA_CENTER))]

    kd = [[
        kpi_cell("TOTAL ACTIF", safe_val(bilan)),
        kpi_cell("RESULTAT NET", safe_val(rn), SUCCESS if (pd.notna(rn) and float(rn or 0)>=0) else DANGER),
        kpi_cell("FONDS PROPRES", safe_val(fp), colors.HexColor("#B87BFF")),
        kpi_cell("ROA", safe_val(roa, pct=True), colors.HexColor("#5BC8F5")),
        kpi_cell("ROE", safe_val(roe, pct=True), COPPER),
        kpi_cell("CIR", safe_val(cir, pct=True), SUCCESS if (cir and cir<60) else (COPPER2 if (cir and cir<80) else DANGER)),
    ]]
    kt = Table(kd, colWidths=[2.9*cm]*6, rowHeights=[1.3*cm])
    kt.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,-1),LIGHT), ("BOX",(0,0),(-1,-1),0.5,COPPER),
        ("INNERGRID",(0,0),(-1,-1),0.3,colors.HexColor("#DDDDDD")),
        ("VALIGN",(0,0),(-1,-1),"MIDDLE"),("ALIGN",(0,0),(-1,-1),"CENTER"),
        ("TOPPADDING",(0,0),(-1,-1),5),("BOTTOMPADDING",(0,0),(-1,-1),5),
    ]))
    story.append(kt)
    story.append(Spacer(1, 0.4*cm))

    # Tableau évolution
    story.append(Paragraph("Evolution des Indicateurs Cles", section_s))
    story.append(HRFlowable(width="100%", thickness=0.5, color=NAVY2, spaceAfter=5))
    evol_h = ["Annee","Bilan (M)","Ressources (M)","Emplois (M)","Fonds Propres (M)","Resultat Net (M)","Effectif","Agences"]
    evol_d = [evol_h]
    for _, r in df_b.iterrows():
        rn2 = pd.to_numeric(r.get("resultat_net"), errors="coerce")
        evol_d.append([str(int(r["annee"])), safe_val(r.get("bilan")), safe_val(r.get("ressources")),
                        safe_val(r.get("emploi")), safe_val(r.get("fonds_propres")),
                        safe_val(rn2), safe_val(r.get("effectif"),decimals=0), safe_val(r.get("agences"),decimals=0)])
    et = Table(evol_d, colWidths=[1.5*cm,2.3*cm,2.3*cm,2.3*cm,2.5*cm,2.5*cm,1.6*cm,1.5*cm])
    es = TableStyle([("BACKGROUND",(0,0),(-1,0),NAVY),("TEXTCOLOR",(0,0),(-1,0),COPPER),
                     ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("FONTSIZE",(0,0),(-1,-1),8),
                     ("ALIGN",(0,0),(-1,-1),"CENTER"),("ROWBACKGROUNDS",(0,1),(-1,-1),[LIGHT,WHITE]),
                     ("GRID",(0,0),(-1,-1),0.3,colors.HexColor("#CCCCCC")),
                     ("TOPPADDING",(0,0),(-1,-1),5),("BOTTOMPADDING",(0,0),(-1,-1),5)])
    for i, r in enumerate(df_b.itertuples(), 1):
        rn_v = pd.to_numeric(getattr(r,"resultat_net",None), errors="coerce")
        if pd.notna(rn_v) and rn_v < 0:
            es.add("TEXTCOLOR",(5,i),(5,i),DANGER)
            es.add("FONTNAME",(5,i),(5,i),"Helvetica-Bold")
    et.setStyle(es)
    story.append(et)
    story.append(Spacer(1, 0.4*cm))

    # Ratios
    story.append(Paragraph("Ratios Financiers", section_s))
    story.append(HRFlowable(width="100%", thickness=0.5, color=NAVY2, spaceAfter=5))
    rh = ["Annee","ROA (%)","ROE (%)","CIR (%)","Solvabilite (%)"]
    rd = [rh]
    for _, r in df_b.iterrows():
        b2=pd.to_numeric(r.get("bilan"),errors="coerce"); rn3=pd.to_numeric(r.get("resultat_net"),errors="coerce")
        fp3=pd.to_numeric(r.get("fonds_propres"),errors="coerce"); pg=pd.to_numeric(r.get("pnb"),errors="coerce")
        cg=pd.to_numeric(r.get("charges_generales"),errors="coerce")
        rd.append([str(int(r["annee"])),
            safe_val((float(rn3)/float(b2)*100) if (pd.notna(b2) and float(b2 or 1)!=0 and pd.notna(rn3)) else None, pct=True),
            safe_val((float(rn3)/float(fp3)*100) if (pd.notna(fp3) and float(fp3 or 1)!=0 and pd.notna(rn3)) else None, pct=True),
            safe_val((float(cg)/float(pg)*100) if (pd.notna(pg) and float(pg or 1)!=0 and pd.notna(cg)) else None, pct=True),
            safe_val((float(fp3)/float(b2)*100) if (pd.notna(b2) and float(b2 or 1)!=0 and pd.notna(fp3)) else None, pct=True),
        ])
    rt = Table(rd, colWidths=[2*cm,3.5*cm,3.5*cm,3.5*cm,3.5*cm])
    rt.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),NAVY2),("TEXTCOLOR",(0,0),(-1,0),COPPER),
        ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("FONTSIZE",(0,0),(-1,-1),9),
        ("ALIGN",(0,0),(-1,-1),"CENTER"),("ROWBACKGROUNDS",(0,1),(-1,-1),[LIGHT,WHITE]),
        ("GRID",(0,0),(-1,-1),0.3,colors.HexColor("#CCCCCC")),
        ("TOPPADDING",(0,0),(-1,-1),6),("BOTTOMPADDING",(0,0),(-1,-1),6)]))
    story.append(rt)
    story.append(Spacer(1,0.4*cm))

    # Positionnement
    story.append(Paragraph(f"Positionnement sur le Marche - {annee}", section_s))
    story.append(HRFlowable(width="100%", thickness=0.5, color=NAVY2, spaceAfter=5))
    df_mkt = df_global[df_global["annee"]==annee].copy()
    df_mkt["bilan"] = pd.to_numeric(df_mkt["bilan"], errors="coerce")
    total_mkt = df_mkt["bilan"].sum()
    rank = df_mkt.sort_values("bilan",ascending=False)["sigle"].tolist()
    part = (float(bilan)/total_mkt*100) if (pd.notna(bilan) and total_mkt>0) else None
    rang = (rank.index(sigle)+1) if sigle in rank else "N/D"
    pd_data = [["Indicateur","Valeur","Interpretation"],
        ["Rang par Bilan", f"#{rang} / {len(rank)}", "Leader" if rang==1 else ("Top 3" if rang<=3 else ("Top 5" if rang<=5 else "Marche"))],
        ["Part de Marche", safe_val(part,pct=True), "Forte" if (part and part>10) else ("Moyenne" if (part and part>5) else "Faible")],
        ["Resultat Net", safe_val(rn), "Beneficiaire" if (pd.notna(rn) and float(rn or 0)>0) else "Deficitaire"],
    ]
    pt = Table(pd_data, colWidths=[5*cm,5*cm,6.5*cm])
    pt.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),NAVY),("TEXTCOLOR",(0,0),(-1,0),COPPER),
        ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("FONTSIZE",(0,0),(-1,-1),9),
        ("ALIGN",(1,0),(-1,-1),"CENTER"),("ROWBACKGROUNDS",(0,1),(-1,-1),[LIGHT,WHITE]),
        ("GRID",(0,0),(-1,-1),0.3,colors.HexColor("#CCCCCC")),
        ("TOPPADDING",(0,0),(-1,-1),7),("BOTTOMPADDING",(0,0),(-1,-1),7),
        ("FONTNAME",(0,1),(0,-1),"Helvetica-Bold")]))
    story.append(pt)
    story.append(Spacer(1,0.4*cm))
    story.append(Paragraph("Ce rapport a ete genere automatiquement par le Dashboard Banques Senegal a partir des donnees BCEAO 2015-2022.", body_s))

    doc.build(story, canvasmaker=NumberedCanvas)
    buf.seek(0)
    return buf.getvalue()
