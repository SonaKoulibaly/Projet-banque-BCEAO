"""
ETL - ÉTAPE 03 : Normalisation et fusion des données Excel + PDF
=================================================================
Ce script :
  1. Lit les données Excel (collection banques_raw) depuis MongoDB
  2. Lit les données PDF (collection banques_pdf_raw) depuis MongoDB
  3. Harmonise les structures pour obtenir un schéma unique
  4. Fusionne les deux sources (Excel 2015-2020 + PDF 2020-2022)
  5. Insère dans la collection finale : banques_normalized
"""

import os
import logging
import pandas as pd
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://<user>:<password>@cluster0.mongodb.net/")
DB_NAME   = "banque_senegal"

COL_EXCEL      = "banques_raw"
COL_PDF        = "banques_pdf_raw"
COL_NORMALIZED = "banques_normalized"

# Schéma cible : colonnes communes dans la collection normalisée
# Les données Excel ont directement les valeurs à plat
# Les données PDF ont des sous-documents bilan_actif / compte_resultat → on les aplatit

GROUPE_BANCAIRE_MAP = {
    "SGBS"       : "Groupes Internationaux",
    "BICIS"      : "Groupes Internationaux",
    "CBAO"       : "Groupes Continentaux",
    "CDS"        : "Groupes Locaux",
    "BHS"        : "Groupes Locaux",
    "CITIBANK"   : "Groupes Internationaux",
    "LBA"        : "Groupes Locaux",
    "BIS"        : "Groupes Locaux",
    "ECOBANK"    : "Groupes Continentaux",
    "ORABANK"    : "Groupes Régionaux",
    "BOA"        : "Groupes Continentaux",
    "BSIC"       : "Groupes Régionaux",
    "BIMAO"      : "Groupes Locaux",
    "BAS"        : "Groupes Continentaux",
    "BRM"        : "Groupes Locaux",
    "UBA"        : "Groupes Continentaux",
    "FBNBANK"    : "Groupes Continentaux",
    "CISA"       : "Groupes Locaux",
    "BNDE"       : "Groupes Locaux",
    "NSIA Banque": "Groupes Continentaux",
    "BDK"        : "Groupes Locaux",
    "BGFI"       : "Groupes Continentaux",
    "BCIM"       : "Groupes Régionaux",
    "LBO"        : "Groupes Locaux",
    "CBI"        : "Groupes Locaux",
    "BDM"        : "Groupes Régionaux",
    "BBG"        : "Groupes Continentaux",
}


def flatten_pdf_record(r):
    """
    Aplatit un enregistrement PDF (qui a des sous-documents)
    vers le même schéma plat que l'Excel.
    """
    flat = {
        "sigle"         : r.get("sigle"),
        "groupe_bancaire": GROUPE_BANCAIRE_MAP.get(r.get("sigle"), "Autres"),
        "annee"         : r.get("annee"),
        "source"        : r.get("source"),
        "bilan"         : r.get("bilan"),
        "fonds_propres" : r.get("fonds_propres"),
        "resultat_net"  : r.get("resultat_net"),
        "produit_net_bancaire": r.get("produit_net_bancaire"),
    }

    # Aplatir bilan_actif
    for k, v in r.get("bilan_actif", {}).items():
        flat[f"actif_{k}"] = v

    # Aplatir bilan_passif
    for k, v in r.get("bilan_passif", {}).items():
        flat[f"passif_{k}"] = v

    # Aplatir compte_resultat
    for k, v in r.get("compte_resultat", {}).items():
        flat[k] = v

    # Ajouter colonnes absentes dans le PDF mais présentes dans Excel (mises à None)
    for col in ["emploi", "ressources", "effectif", "agences", "comptes"]:
        flat.setdefault(col, None)

    return flat


def normalize_excel_record(r):
    """Normalise un enregistrement Excel (déjà plat, juste nettoyer _id)."""
    r.pop("_id", None)
    r.setdefault("source", "excel_base")
    return r


def deduplicate(excel_df, pdf_df):
    """
    Fusionne Excel (2015-2020) et PDF (2020-2022).
    Pour 2020 : priorité à l'Excel (données déjà vérifiées).
    Pour 2021-2022 : uniquement PDF.
    """
    log.info(f"Excel : {len(excel_df)} lignes | PDF : {len(pdf_df)} lignes")

    # Garder le PDF uniquement pour les années > 2020
    pdf_new = pdf_df[pdf_df["annee"] > 2020].copy()
    log.info(f"PDF nouvelles années (2021-2022) : {len(pdf_new)} lignes")

    combined = pd.concat([excel_df, pdf_new], ignore_index=True)
    combined.sort_values(["sigle", "annee"], inplace=True)
    combined.reset_index(drop=True, inplace=True)

    log.info(f"Combiné : {len(combined)} lignes | {combined['sigle'].nunique()} banques | années {sorted(combined['annee'].dropna().unique().astype(int).tolist())}")
    return combined


def insert_normalized(df, uri, db_name, collection):
    log.info("Insertion dans banques_normalized...")
    client = MongoClient(uri)
    col    = client[db_name][collection]
    col.drop()  # Recréer proprement à chaque normalisation

    records = df.to_dict(orient="records")
    # Remplacer NaN par None
    import math
    for r in records:
        for k, v in r.items():
            if isinstance(v, float) and math.isnan(v):
                r[k] = None

    col.insert_many(records)
    col.create_index([("sigle", 1), ("annee", 1)])
    col.create_index("groupe_bancaire")
    log.info(f"Normalisée insérée : {len(records)} docs")
    client.close()


def main():
    log.info("=== ETL 03 — Normalisation & Fusion Excel + PDF ===")

    client = MongoClient(MONGO_URI)
    db     = client[DB_NAME]

    # Charger Excel
    excel_records = list(db[COL_EXCEL].find({}))
    excel_df = pd.DataFrame([normalize_excel_record(r) for r in excel_records])
    log.info(f"Excel chargé : {len(excel_df)} lignes")

    # Charger PDF
    pdf_records  = list(db[COL_PDF].find({}))
    pdf_flat     = [flatten_pdf_record(r) for r in pdf_records]
    pdf_df       = pd.DataFrame(pdf_flat)
    log.info(f"PDF chargé et aplati : {len(pdf_df)} lignes")

    client.close()

    # Fusionner
    combined = deduplicate(excel_df, pdf_df)

    # Insérer
    insert_normalized(combined, MONGO_URI, DB_NAME, COL_NORMALIZED)

    log.info("ETL 03 OK !")


if __name__ == "__main__":
    main()