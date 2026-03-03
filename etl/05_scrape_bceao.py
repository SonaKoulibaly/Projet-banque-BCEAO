# =============================================================================
# 05_scrape_bceao.py — Scraping automatique des rapports BCEAO
# Projet : Banques Sénégal | Auteur : Sona KOULIBALY
# Pages Sénégal vérifiées : 2022 → 267-319 | 2023 → 257-324
# =============================================================================

import os, re, time, requests, pdfplumber, pandas as pd
from pathlib import Path
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "")
DB_NAME   = os.getenv("DB_NAME", "banque_senegal")
RAW_DIR   = Path(__file__).parent.parent / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# URLs directes des PDFs BCEAO
BCEAO_PDFS = {
    2023: "https://www.bceao.int/sites/default/files/2025-11/Bilans%20et%20comptes%20de%20r%C3%A9sultat%20des%20banques%2C%20%C3%A9tablissements%20financiers%20et%20compagnies%20financi%C3%A8res%20de%20l%27UMOA%202023.pdf",
    2022: "https://www.bceao.int/sites/default/files/2024-05/Bilans%20et%20comptes%20de%20r%C3%A9sultat%20des%20banques%2C%20%C3%A9tablissements%20financiers%20et%20compagnies%20financi%C3%A8res%20de%20l%27UMOA%202022.pdf",
}

# Pages exactes de la section Sénégal — vérifiées sur chaque PDF (index base 0)
PAGES_SENEGAL = {
    2022: (266, 318),   # Pages 267-319
    2023: (256, 323),   # Pages 257-324
}


# ─── CONNEXION MONGODB ────────────────────────────────────────────────────────
def get_db():
    if not MONGO_URI or "<password>" in MONGO_URI:
        print("⚠️  MONGO_URI non configuré — mode CSV seulement")
        return None
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        print("✅ Connecté à MongoDB Atlas")
        return client[DB_NAME]
    except Exception as e:
        print(f"❌ MongoDB : {e}")
        return None


# ─── TÉLÉCHARGEMENT PDF ───────────────────────────────────────────────────────
def download_pdf(annee, url):
    """
    Télécharge le PDF BCEAO.
    Si un fichier local valide (> 1 MB) existe déjà, il est utilisé directement.
    """
    dest = RAW_DIR / f"BCEAO_{annee}.pdf"

    if dest.exists():
        size_mb = dest.stat().st_size / 1024 / 1024
        if size_mb > 1.0:
            print(f"  📁 PDF {annee} déjà présent ({size_mb:.1f} MB) — OK")
            return dest
        else:
            print(f"  ⚠️  PDF {annee} invalide ({size_mb:.1f} MB) — suppression")
            dest.unlink()

    print(f"  ⬇️  Téléchargement PDF {annee}...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/pdf,*/*",
        "Referer": "https://www.bceao.int/fr/publications/",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=120, stream=True)
        resp.raise_for_status()
        content = b"".join(resp.iter_content(8192))

        if not content.startswith(b"%PDF"):
            print(f"  ❌ BCEAO bloque le téléchargement automatique")
            print(f"  💡 Télécharge manuellement le PDF {annee} sur bceao.int")
            print(f"     Renomme-le BCEAO_{annee}.pdf → place dans data/raw/")
            return None

        dest.write_bytes(content)
        print(f"  ✅ PDF {annee} téléchargé : {dest.stat().st_size/1024/1024:.1f} MB")
        return dest
    except Exception as e:
        print(f"  ❌ Erreur téléchargement : {e}")
        print(f"  💡 Place manuellement BCEAO_{annee}.pdf dans data/raw/")
        return None


# ─── EXTRACTION DONNÉES ───────────────────────────────────────────────────────
def extract_senegal_data(pdf_path, annee):
    """
    Extrait les données Sénégal depuis le PDF BCEAO.
    Utilise les pages vérifiées dans PAGES_SENEGAL.
    """
    if annee in PAGES_SENEGAL:
        page_debut, page_fin = PAGES_SENEGAL[annee]
        print(f"  📌 Pages Sénégal {annee} : {page_debut+1} → {page_fin+1}")
    else:
        page_debut, page_fin = detect_senegal_pages(pdf_path)

    records = []
    print(f"  📊 Extraction en cours...")

    with pdfplumber.open(pdf_path) as pdf:
        total = len(pdf.pages)
        print(f"  📄 Total pages PDF : {total}")

        for page_num in range(page_debut, min(page_fin + 1, total)):
            page = pdf.pages[page_num]
            tables = page.extract_tables()

            if tables:
                for table in tables:
                    for row in table:
                        if row and any(c for c in row if c):
                            cleaned = [str(c).strip() if c else "" for c in row]
                            records.append({
                                "page"    : page_num + 1,
                                "annee"   : annee,
                                "source"  : f"pdf_bceao_{annee}",
                                "raw_data": str(cleaned),
                            })
            else:
                text = (page.extract_text() or "").strip()
                if text:
                    records.append({
                        "page"    : page_num + 1,
                        "annee"   : annee,
                        "source"  : f"pdf_bceao_{annee}_text",
                        "raw_data": text[:500],
                    })

    df = pd.DataFrame(records)
    print(f"  ✅ {len(df)} lignes extraites pour {annee}")
    return df


# ─── DÉTECTION AUTO (FALLBACK) ────────────────────────────────────────────────
def detect_senegal_pages(pdf_path):
    """
    Détecte automatiquement les pages Sénégal si l'année n'est pas dans PAGES_SENEGAL.
    Ignore les 50 premières pages (table des matières).
    """
    page_debut = page_fin = None

    with pdfplumber.open(pdf_path) as pdf:
        total = len(pdf.pages)
        for i, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            lines = [l.strip() for l in text.split('\n') if l.strip()]

            # Chercher début : titre SENEGAL seul + tableaux présents + après page 50
            if page_debut is None and i > 50:
                for line in lines[:3]:
                    if re.match(r'^S[EÉ]N[EÉ]GAL\s*$', line, re.IGNORECASE):
                        if page.extract_tables():
                            page_debut = i
                            print(f"  ✅ Début Sénégal auto-détecté : page {i+1}")
                            break

            # Chercher fin : titre TOGO
            elif page_debut and not page_fin:
                for line in lines[:3]:
                    if re.match(r'^TOGO\s*$', line, re.IGNORECASE):
                        page_fin = i - 1
                        print(f"  ✅ Fin Sénégal auto-détectée : page {i}")
                        break

            if page_fin:
                break

        # Fallback si auto-détection échoue
        if not page_debut:
            page_debut = 256
            print(f"  ⚠️  Fallback début → page {page_debut+1}")
        if not page_fin:
            page_fin = min(page_debut + 67, total - 1)
            print(f"  ⚠️  Fallback fin → page {page_fin+1}")

    return page_debut, page_fin


# ─── VÉRIFICATION DOUBLONS ────────────────────────────────────────────────────
def check_existing_years(db):
    """Retourne les années déjà présentes dans MongoDB."""
    if db is None:
        return []
    try:
        existing = db["banques_pdf_raw"].distinct("annee")
        print(f"  📌 Années déjà en base : {sorted(existing)}")
        return [int(y) for y in existing]
    except Exception as e:
        print(f"  ⚠️  Erreur vérification : {e}")
        return []


# ─── INSERTION MONGODB ────────────────────────────────────────────────────────
def insert_to_mongo(db, df, annee):
    """Insère les données — supprime d'abord les anciens pour éviter doublons."""
    if db is None or df.empty:
        return 0
    result_del = db["banques_pdf_raw"].delete_many({"annee": annee})
    if result_del.deleted_count:
        print(f"  🗑️  {result_del.deleted_count} anciens docs supprimés pour {annee}")
    result = db["banques_pdf_raw"].insert_many(df.to_dict("records"))
    print(f"  ✅ {len(result.inserted_ids)} docs insérés pour {annee}")
    return len(result.inserted_ids)


# ─── SAUVEGARDE CSV ───────────────────────────────────────────────────────────
def save_csv(df, annee):
    """Sauvegarde locale en CSV."""
    csv_path = RAW_DIR.parent / "processed" / f"banques_pdf_raw_{annee}.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"  💾 CSV sauvegardé : {csv_path.name}")


# ─── PIPELINE PRINCIPAL ───────────────────────────────────────────────────────
def run_scraping(annees=None):
    """Pipeline complet : téléchargement → extraction → déduplication → insertion."""
    print("\n" + "="*60)
    print("  🌐 SCRAPING BCEAO — Démarrage")
    print("="*60)

    db       = get_db()
    existing = check_existing_years(db)

    if annees is None:
        annees = list(BCEAO_PDFS.keys())

    total = 0
    for annee in sorted(annees):
        print(f"\n📅 Traitement année {annee}...")

        if annee in existing:
            print(f"  ⏭️  {annee} déjà en base — ignorée (anti-doublon)")
            continue

        url = BCEAO_PDFS.get(annee)
        if not url:
            print(f"  ❌ Pas d'URL connue pour {annee}")
            continue

        pdf_path = download_pdf(annee, url)
        if not pdf_path:
            continue

        df = extract_senegal_data(pdf_path, annee)
        if df.empty:
            print(f"  ⚠️  Aucune donnée extraite pour {annee}")
            continue

        save_csv(df, annee)
        total += insert_to_mongo(db, df, annee)
        time.sleep(1)

    print(f"\n{'='*60}")
    print(f"  ✅ SCRAPING TERMINÉ — {total} documents insérés")
    print("="*60 + "\n")
    return total


# ─── VÉRIFICATION NOUVELLES ANNÉES ───────────────────────────────────────────
def check_new_reports():
    """Vérifie si de nouveaux rapports sont disponibles sur le site BCEAO."""
    print("\n🔍 Vérification nouveaux rapports BCEAO...")
    url = "https://www.bceao.int/fr/publications/bilans-et-comptes-de-resultats-des-etablissements-de-credit-de-lumoa"
    try:
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        annees = sorted(set(
            int(a) for a in re.findall(r'UMOA[^\d]*(\d{4})', resp.text)
            if 2020 <= int(a) <= 2030
        ))
        print(f"  📋 Années trouvées sur BCEAO : {annees}")
        db       = get_db()
        existing = check_existing_years(db)
        nouvelles = [a for a in annees if a not in existing]
        if nouvelles:
            print(f"  🆕 Nouvelles années disponibles : {nouvelles}")
        else:
            print(f"  ✅ Base à jour — aucune nouvelle année")
        return nouvelles
    except Exception as e:
        print(f"  ❌ Erreur : {e}")
        return []


# ─── LANCEMENT ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys

    # Usage :
    #   python 05_scrape_bceao.py check     → vérifier nouvelles années
    #   python 05_scrape_bceao.py 2023      → scraper année spécifique
    #   python 05_scrape_bceao.py           → toutes les années manquantes

    if len(sys.argv) > 1:
        if sys.argv[1] == "check":
            n = check_new_reports()
            if n:
                print(f"\n💡 Pour scraper : python 05_scrape_bceao.py {' '.join(map(str, n))}")
        else:
            run_scraping([int(a) for a in sys.argv[1:] if a.isdigit()])
    else:
        run_scraping()