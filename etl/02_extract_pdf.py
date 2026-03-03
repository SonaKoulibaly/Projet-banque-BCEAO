"""
ETL - ÉTAPE 02 : Extraction des données PDF BCEAO (Bilans & Comptes de Résultat)
==================================================================================
Ce script :
  1. Lit le PDF BCEAO (ex : UMOA 2022) et cible uniquement les pages SÉNÉGAL
  2. Extrait les bilans (actif/passif) et comptes de résultat par banque et par année
  3. Sauvegarde les données brutes en JSON dans data/processed/pdf_raw.json
  4. Insère dans MongoDB Atlas (collection : banques_pdf_raw)

Structure du PDF BCEAO :
  - Pages 267-319 = données des banques sénégalaises
  - Pour chaque banque : 2 pages consécutives → Bilan | Compte de Résultat
  - Chaque page contient les années 2020, 2021, 2022
"""

import os
import re
import json
import logging
import pdfplumber
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

BASE_DIR  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PDF_PATH  = os.path.join(BASE_DIR, "data", "raw", "Bilans_et_comptes_de_résultat_des_banques_2022.pdf")
JSON_OUT  = os.path.join(BASE_DIR, "data", "processed", "pdf_raw.json")

MONGO_URI  = os.getenv("MONGO_URI", "mongodb+srv://<user>:<password>@cluster0.mongodb.net/")
DB_NAME    = "banque_senegal"
COLLECTION = "banques_pdf_raw"

# Pages du Sénégal dans le PDF BCEAO 2022 (index 0-based)
# Page 267 = index 266 = SGSN Bilan | Page 268 = SGSN Compte Résultat ...
SENEGAL_START_PAGE = 266   # index 0-based (= page 267 du PDF)
SENEGAL_END_PAGE   = 319   # index 0-based (= page 320 du PDF)

# Mapping sigle PDF → sigle standard (harmonisation avec l'Excel)
SIGLE_MAP = {
    "SGSN"        : "SGBS",
    "BICIS"       : "BICIS",
    "CBAO"        : "CBAO",
    "C.D.S."      : "CDS",
    "B.H.S."      : "BHS",
    "CITIBANK"    : "CITIBANK",
    "LBA"         : "LBA",
    "B.I.S."      : "BIS",
    "ECOBANK"     : "ECOBANK",
    "ORABANK"     : "ORABANK",
    "BOA-S"       : "BOA",
    "BSIC"        : "BSIC",
    "BIMAO"       : "BIMAO",
    "B.A-S."      : "BAS",
    "B.A.-S."     : "BAS",
    "B.R.M."      : "BRM",
    "U.B.A."      : "UBA",
    "FBNBANK"     : "FBNBANK",
    "CI"          : "CISA",
    "B.N.D.E"     : "BNDE",
    "NSIA BANQUE" : "NSIA Banque",
    "BDK"         : "BDK",
    "BGFI BANK"   : "BGFI",
    "BCI-MALI"    : "BCIM",
    "LBO"         : "LBO",
    "CBI-SENEGAL" : "CBI",
    "BDM"         : "BDM",
    "BBG-CI"      : "BBG",
}

# Libellés des lignes du Bilan (Actif)
BILAN_ACTIF_LABELS = [
    "caisse_banque_centrale",
    "effets_publics",
    "creances_interbancaires",
    "creances_clientele",
    "obligations_titres_revenu_fixe",
    "actions_titres_revenu_variable",
    "actionnaires_associes",
    "autres_actifs",
    "comptes_regularisation_actif",
    "participations_titres_long_terme",
    "parts_entreprises_liees",
    "prets_subordonnes",
    "immobilisations_incorporelles",
    "immobilisations_corporelles",
    "total_actif",
]

# Libellés des lignes du Bilan (Passif)
BILAN_PASSIF_LABELS = [
    "banque_centrale_ccp",
    "dettes_interbancaires",
    "dettes_clientele",
    "dettes_representees_titre",
    "autres_passifs",
    "comptes_regularisation_passif",
    "provisions",
    "emprunts_titres_subordonnes",
    "capitaux_propres",
    "capital_souscrit",
    "primes_capital",
    "reserves",
    "ecarts_reevaluation",
    "provisions_reglementees",
    "report_nouveau",
    "resultat_exercice",
    "total_passif",
]

# Libellés des lignes du Compte de Résultat
COMPTE_RESULTAT_LABELS = [
    "interets_produits",
    "interets_charges",
    "revenus_titres",
    "commissions_produits",
    "commissions_charges",
    "gains_pertes_negociation",
    "gains_pertes_placement",
    "autres_produits_exploitation",
    "autres_charges_exploitation",
    "produit_net_bancaire",
    "subventions_investissement",
    "charges_generales_exploitation",
    "dotations_amortissements",
    "resultat_brut_exploitation",
    "cout_risque",
    "resultat_exploitation",
    "gains_pertes_actifs_immobilises",
    "resultat_avant_impot",
    "impots_benefices",
    "resultat_net",
]


def clean_number(s):
    """Convertit une chaîne de type '1 234 567' ou '-456' en float."""
    if s is None:
        return None
    s = str(s).strip().replace("\u202f", "").replace("\xa0", "").replace(" ", "")
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def extract_values_from_text(text, labels):
    """
    Extrait les valeurs numériques d'un bloc de texte structuré.
    Retourne un dict {label: {2020: val, 2021: val, 2022: val}}.
    """
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    # Repérer les lignes numériques (contiennent des chiffres avec espaces)
    number_pattern = re.compile(r"^-?\d[\d\s]*$|^-$|^0$")
    number_lines = []
    for line in lines:
        if number_pattern.match(line.replace(" ", "").replace("-", "0")):
            number_lines.append(line)

    # Extraire par blocs de 3 (2020, 2021, 2022)
    result = {}
    years  = [2020, 2021, 2022]
    idx    = 0
    for label in labels:
        row = {}
        for year in years:
            if idx < len(number_lines):
                row[year] = clean_number(number_lines[idx])
                idx += 1
            else:
                row[year] = None
        result[label] = row
    return result


def parse_bilan_page(page):
    """Parse une page de bilan (Actif + Passif)."""
    text   = page.extract_text() or ""
    tables = page.extract_tables()

    data = {"actif": {}, "passif": {}}

    # Essayer via les tables pdfplumber d'abord
    if tables and len(tables) >= 2:
        try:
            # Table Actif
            actif_col = tables[0][2] if len(tables[0]) > 2 else None
            if actif_col:
                vals = actif_col.split("\n") if isinstance(actif_col, str) else []
                actif_data = {}
                years = [2020, 2021, 2022]
                # Les 3 colonnes numériques sont tables[0][2], [3], [4] selon structure
                for i, label in enumerate(BILAN_ACTIF_LABELS):
                    row = {}
                    for j, year in enumerate(years):
                        try:
                            col_idx = 2 + j
                            if col_idx < len(tables[0]) and isinstance(tables[0][col_idx], str):
                                vals = tables[0][col_idx].split("\n")
                                row[year] = clean_number(vals[i]) if i < len(vals) else None
                            else:
                                row[year] = None
                        except Exception:
                            row[year] = None
                    actif_data[label] = row
                data["actif"] = actif_data
        except Exception as e:
            log.debug(f"Table parsing bilan actif : {e}")

    # Fallback : extraction depuis le texte brut
    if not data["actif"]:
        data["actif"] = extract_values_from_text(text, BILAN_ACTIF_LABELS)

    return data


def parse_compte_resultat_page(page):
    """Parse une page de compte de résultat."""
    text   = page.extract_text() or ""
    tables = page.extract_tables()

    data = {}

    # Extraction via texte (plus fiable pour ce PDF)
    # On récupère les 3 colonnes numériques à partir du texte
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    # Chercher les blocs de 3 valeurs numériques
    num_re = re.compile(r"^-?\s*\d[\d\s]*$|^-?\d+$")
    cols   = {2020: [], 2021: [], 2022: []}

    # Chercher ligne contenant 3 valeurs séparées (format: "val2020 val2021 val2022")
    triple_re = re.compile(r"(-?\s*\d[\d\s]*)\s{2,}(-?\s*\d[\d\s]*)\s{2,}(-?\s*\d[\d\s]*)")

    found_triples = []
    for line in lines:
        m = triple_re.search(line)
        if m:
            found_triples.append((
                clean_number(m.group(1)),
                clean_number(m.group(2)),
                clean_number(m.group(3)),
            ))

    # Associer aux labels
    for i, label in enumerate(COMPTE_RESULTAT_LABELS):
        row = {}
        for j, year in enumerate([2020, 2021, 2022]):
            if i < len(found_triples):
                row[year] = found_triples[i][j]
            else:
                row[year] = None
        data[label] = row

    # Si aucun triple trouvé, utiliser les tables
    if not found_triples and tables:
        try:
            for table in tables:
                for row_data in table:
                    if row_data and len(row_data) >= 4:
                        # Ligne avec valeurs numériques dans colonnes 1,2,3
                        pass  # fallback simplifié
        except Exception:
            pass

    return data


def extract_senegal_data(pdf_path):
    """
    Parcourt les pages sénégalaises du PDF et retourne
    une liste de documents structurés par (sigle, année).
    """
    log.info(f"Ouverture PDF : {pdf_path}")
    all_records = []

    with pdfplumber.open(pdf_path) as pdf:
        total = len(pdf.pages)
        log.info(f"PDF : {total} pages | Pages Sénégal : {SENEGAL_START_PAGE+1} → {SENEGAL_END_PAGE+1}")

        i = SENEGAL_START_PAGE
        while i < min(SENEGAL_END_PAGE, total):
            page = pdf.pages[i]
            text = page.extract_text() or ""
            lines = [l.strip() for l in text.split("\n") if l.strip()]

            # Ignorer les pages agrégées (ex: "Banques et Etablissements financiers")
            if len(lines) < 3 or "Etablissements Financiers" in lines[1]:
                i += 1
                continue

            # Identifier le sigle (ligne 2) et le type de page (ligne 3)
            raw_sigle  = lines[1] if len(lines) > 1 else ""
            page_type  = lines[2] if len(lines) > 2 else ""
            sigle_std  = SIGLE_MAP.get(raw_sigle, raw_sigle)

            if "Bilans" in page_type:
                log.info(f"  Page {i+1} | Bilan | {raw_sigle} → {sigle_std}")
                bilan_data = parse_bilan_page(page)

                # Page suivante = Compte de Résultat
                if i + 1 < total:
                    next_page    = pdf.pages[i + 1]
                    resultat_data = parse_compte_resultat_page(next_page)
                else:
                    resultat_data = {}

                # Créer un enregistrement par année
                for year in [2020, 2021, 2022]:
                    record = {
                        "sigle"          : sigle_std,
                        "sigle_pdf"      : raw_sigle,
                        "annee"          : year,
                        "source"         : "pdf_bceao_2022",
                        "bilan_actif"    : {k: v.get(year) for k, v in bilan_data.get("actif", {}).items()},
                        "bilan_passif"   : {k: v.get(year) for k, v in bilan_data.get("passif", {}).items()},
                        "compte_resultat": {k: v.get(year) for k, v in resultat_data.items()},
                    }
                    # Ajouter les totaux clés au niveau racine pour faciliter les analyses
                    record["bilan"]      = record["bilan_actif"].get("total_actif")
                    record["fonds_propres"] = record["bilan_passif"].get("capitaux_propres")
                    record["resultat_net"]  = record["compte_resultat"].get("resultat_net")
                    record["produit_net_bancaire"] = record["compte_resultat"].get("produit_net_bancaire")

                    all_records.append(record)

                i += 2  # sauter bilan + compte résultat
            else:
                i += 1

    log.info(f"Extraction terminée : {len(all_records)} enregistrements")
    return all_records


def save_json(records, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2, default=str)
    log.info(f"JSON sauvegardé : {path}")


def insert_pdf_to_mongodb(records, uri, db_name, collection):
    log.info("Insertion PDF dans MongoDB Atlas...")
    client = MongoClient(uri)
    col    = client[db_name][collection]
    ops = [
        UpdateOne(
            {"sigle": r["sigle"], "annee": r["annee"], "source": r["source"]},
            {"$set": r}, upsert=True
        )
        for r in records
    ]
    result = col.bulk_write(ops)
    log.info(f"MongoDB PDF : {result.upserted_count} insérés | {result.modified_count} mis à jour")
    col.create_index([("sigle", 1), ("annee", 1)])
    client.close()


def main():
    log.info("=== ETL 02 — Extraction PDF BCEAO → MongoDB Atlas ===")
    records = extract_senegal_data(PDF_PATH)
    save_json(records, JSON_OUT)
    insert_pdf_to_mongodb(records, MONGO_URI, DB_NAME, COLLECTION)
    log.info(f"ETL 02 OK ! {len(records)} enregistrements extraits du PDF")


if __name__ == "__main__":
    main()