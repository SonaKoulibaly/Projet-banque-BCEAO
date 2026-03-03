"""
ETL - ÉTAPE 01 : Chargement du fichier Excel BASE_SENEGAL2.xlsx vers MongoDB Atlas
===================================================================================
Ce script :
  1. Lit le fichier Excel BASE_SENEGAL2.xlsx (données 2015-2020)
  2. Renomme et normalise les colonnes
  3. Insère les données dans MongoDB Atlas (collection : banques_raw)
  4. Sauvegarde en CSV dans data/processed/
"""

import os
import pandas as pd
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
import logging

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXCEL_PATH = os.path.join(BASE_DIR, "data", "raw", "BASE_SENEGAL2.xlsx")
CSV_OUT    = os.path.join(BASE_DIR, "data", "processed", "banques_excel_clean.csv")

MONGO_URI  = os.getenv("MONGO_URI", "mongodb+srv://<user>:<password>@cluster0.mongodb.net/")
DB_NAME    = "banque_senegal"
COLLECTION = "banques_raw"

RENAME_MAP = {
    "Sigle"                  : "sigle",
    "Goupe_Bancaire"         : "groupe_bancaire",
    "ANNEE"                  : "annee",
    "EMPLOI"                 : "emploi",
    "BILAN"                  : "bilan",
    "RESSOURCES"             : "ressources",
    "FONDS.PROPRE"           : "fonds_propres",
    "EFFECTIF"               : "effectif",
    "AGENCE"                 : "agences",
    "COMPTE"                 : "comptes",
    "INTERETS.ET.PRODUITS.ASSIMILES"                   : "interets_produits",
    "NTERETS.ET.CHARGES.ASSIMILEES"                    : "interets_charges",
    "REVENUS.DES.TITRES.A.REVENU.VARIABLE"             : "revenus_titres",
    "COMMISSIONS.(PRODUITS)"                           : "commissions_produits",
    "COMMISSIONS.(CHARGES)"                            : "commissions_charges",
    "GAINS.OU.PERTES.NETS.SUR.OPERATIONS.DES.PORTEFEUILLES.DE.NEGOCIATION"           : "gains_pertes_negociation",
    "GAINS.OU.PERTES.NETS.SUR.OPERATIONS.DES.PORTEFEUILLES.DE.PLACEMENT.ET.ASSIMILES": "gains_pertes_placement",
    "AUTRES.PRODUITS.D'EXPLOITATION.BANCAIRE"          : "autres_produits_exploitation",
    "AUTRES.CHARGES.D'EXPLOITATION.BANCAIRE"           : "autres_charges_exploitation",
    "PRODUIT.NET.BANCAIRE"                             : "produit_net_bancaire",
    "SUBVENTIONS.D'INVESTISSEMENT"                     : "subventions_investissement",
    "CHARGES.GENERALES.D'EXPLOITATION"                 : "charges_generales_exploitation",
    "DOTATIONS.AUX.AMORTISSEMENTS.ET.AUX.DEPRECIATIONS.DES.IMMOBILISATIONS.INCORPORELLES.ET.CORPORELLES": "dotations_amortissements",
    "RESULTAT.BRUT.D'EXPLOITATION"                     : "resultat_brut_exploitation",
    "COÛT.DU.RISQUE"                                   : "cout_risque",
    "RESULTAT.D'EXPLOITATION"                          : "resultat_exploitation",
    "GAINS.OU.PERTES.NETS.SUR.ACTIFS.IMMOBILISES"      : "gains_pertes_actifs_immobilises",
    "RESULTAT.AVANT.IMPÔT"                             : "resultat_avant_impot",
    "IMPÔTS.SUR.LES.BENEFICES"                         : "impots_benefices",
    "RESULTAT.NET"                                     : "resultat_net",
}


def load_excel(path):
    log.info(f"Lecture Excel : {path}")
    df = pd.read_excel(path)
    df.rename(columns=RENAME_MAP, inplace=True)
    df["source"] = "excel_base"
    log.info(f"{len(df)} lignes chargées")
    return df


def clean_dataframe(df):
    log.info("Nettoyage du DataFrame...")
    numeric_cols = [c for c in df.columns if c not in ("sigle", "groupe_bancaire", "source")]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ("sigle", "groupe_bancaire"):
        df[col] = df[col].astype(str).str.strip()
    df = df.where(pd.notnull(df), None)
    log.info(f"Nettoyage OK | {df.isnull().sum().sum()} valeurs nulles")
    return df


def save_csv(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    log.info(f"CSV sauvegardé : {path}")


def insert_to_mongodb(df, uri, db_name, collection):
    log.info("Connexion MongoDB Atlas...")
    client = MongoClient(uri)
    col    = client[db_name][collection]
    records = df.to_dict(orient="records")
    ops = [
        UpdateOne(
            {"sigle": r.get("sigle"), "annee": r.get("annee"), "source": r.get("source")},
            {"$set": r}, upsert=True
        )
        for r in records
    ]
    result = col.bulk_write(ops)
    log.info(f"MongoDB : {result.upserted_count} insérés | {result.modified_count} mis à jour")
    col.create_index([("sigle", 1), ("annee", 1)])
    col.create_index("groupe_bancaire")
    client.close()


def main():
    log.info("=== ETL 01 — Chargement Excel → MongoDB Atlas ===")
    df = load_excel(EXCEL_PATH)
    df = clean_dataframe(df)
    save_csv(df, CSV_OUT)
    insert_to_mongodb(df, MONGO_URI, DB_NAME, COLLECTION)
    log.info(f"ETL 01 OK ! {len(df)} lignes | {df['sigle'].nunique()} banques | années {sorted(df['annee'].dropna().unique().astype(int).tolist())}")


if __name__ == "__main__":
    main()