"""
ETL - ÉTAPE 04 : Nettoyage & Data Engineering → Base de Production
====================================================================
Ce script :
  1. Lit la collection banques_normalized depuis MongoDB
  2. Applique des règles de nettoyage avancées (valeurs manquantes, outliers)
  3. Calcule les ratios financiers clés (KPIs)
  4. Sauvegarde la base propre dans data/processed/banques_production.csv
  5. Insère dans la collection de PRODUCTION : banques_production

Ratios calculés :
  - ROA  : Rentabilité des actifs (Résultat Net / Total Actif)
  - ROE  : Rentabilité des fonds propres (Résultat Net / Fonds Propres)
  - NIM  : Marge nette d'intérêts ((Intérêts Produits - Intérêts Charges) / Total Actif)
  - CIR  : Coefficient d'exploitation (Charges Générales / PNB)
  - LDR  : Ratio Crédits/Dépôts (Créances Clientèle / Dettes Clientèle)
  - Solvabilité : Fonds Propres / Total Actif
  - Liquidité   : (Caisse + Créances interbancaires) / Total Actif
"""

import os
import math
import logging
import numpy as np
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_OUT    = os.path.join(BASE_DIR, "data", "processed", "banques_production.csv")

MONGO_URI  = os.getenv("MONGO_URI", "mongodb+srv://<user>:<password>@cluster0.mongodb.net/")
DB_NAME    = "banque_senegal"
COL_IN     = "banques_normalized"
COL_OUT    = "banques_production"


# ─── Stratégies de remplissage des valeurs manquantes ─────────────────────────
FILL_STRATEGIES = {
    # Colonnes remplies par interpolation linéaire par banque
    "interpolate": [
        "bilan", "fonds_propres", "resultat_net", "produit_net_bancaire",
        "emploi", "ressources", "effectif", "agences", "comptes",
        "interets_produits", "interets_charges", "commissions_produits",
        "charges_generales_exploitation", "cout_risque",
        "actif_total_actif", "passif_capitaux_propres",
        "actif_creances_clientele", "passif_dettes_clientele",
    ],
    # Colonnes remplies par 0 (absences = zéro logique)
    "fill_zero": [
        "revenus_titres", "gains_pertes_negociation", "gains_pertes_placement",
        "subventions_investissement", "gains_pertes_actifs_immobilises",
        "actif_effets_publics", "actif_obligations_titres_revenu_fixe",
        "actif_actions_titres_revenu_variable",
    ],
}


def safe_ratio(numerator, denominator, scale=100):
    """Calcule un ratio en évitant la division par zéro."""
    try:
        if denominator and not math.isnan(denominator) and denominator != 0:
            val = (numerator or 0) / denominator * scale
            return round(val, 4)
    except Exception:
        pass
    return None


def fill_missing(df):
    """Remplit les valeurs manquantes par stratégie."""
    log.info("Remplissage valeurs manquantes...")

    df = df.sort_values(["sigle", "annee"])

    for col in FILL_STRATEGIES["interpolate"]:
        if col in df.columns:
            df[col] = df.groupby("sigle")[col].transform(
                lambda x: x.interpolate(method="linear", limit_direction="both")
            )

    for col in FILL_STRATEGIES["fill_zero"]:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    log.info(f"Après remplissage : {df.isnull().sum().sum()} valeurs nulles restantes")
    return df


def remove_outliers(df, columns, method="iqr", factor=3.0):
    """
    Détecte et signale les outliers (sans les supprimer, juste les marquer).
    method='iqr' : méthode IQR (Inter-Quartile Range)
    """
    log.info("Détection des outliers...")
    df["is_outlier"] = False

    for col in columns:
        if col not in df.columns:
            continue
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        lower = q1 - factor * iqr
        upper = q3 + factor * iqr
        mask = (df[col] < lower) | (df[col] > upper)
        if mask.sum() > 0:
            log.info(f"  {col} : {mask.sum()} outliers détectés")
            df.loc[mask, "is_outlier"] = True

    return df


def compute_kpis(df):
    """Calcule tous les ratios et KPIs financiers."""
    log.info("Calcul des KPIs et ratios financiers...")

    # Utiliser les colonnes disponibles (Excel ou PDF selon la source)
    def get_col(row, *candidates):
        for c in candidates:
            val = row.get(c)
            if val is not None and not (isinstance(val, float) and math.isnan(val)):
                return val
        return None

    roa_list, roe_list, nim_list, cir_list, ldr_list, solv_list, liq_list = [], [], [], [], [], [], []

    for _, row in df.iterrows():
        bilan       = get_col(row, "bilan", "actif_total_actif")
        fonds_prop  = get_col(row, "fonds_propres", "passif_capitaux_propres")
        res_net     = get_col(row, "resultat_net")
        int_prod    = get_col(row, "interets_produits")
        int_charg   = get_col(row, "interets_charges")
        pnb         = get_col(row, "produit_net_bancaire")
        charges_gen = get_col(row, "charges_generales_exploitation")
        creances    = get_col(row, "actif_creances_clientele")
        dettes_cl   = get_col(row, "passif_dettes_clientele")
        caisse      = get_col(row, "actif_caisse_banque_centrale")
        creances_ib = get_col(row, "actif_creances_interbancaires")

        roa_list.append(safe_ratio(res_net, bilan))
        roe_list.append(safe_ratio(res_net, fonds_prop))
        nim_val = ((int_prod or 0) - (int_charg or 0))
        nim_list.append(safe_ratio(nim_val, bilan))
        cir_list.append(safe_ratio(charges_gen, pnb))
        ldr_list.append(safe_ratio(creances, dettes_cl, scale=100))
        solv_list.append(safe_ratio(fonds_prop, bilan))
        liq_val = (caisse or 0) + (creances_ib or 0)
        liq_list.append(safe_ratio(liq_val, bilan))

    df["roa"]         = roa_list    # Return on Assets (%)
    df["roe"]         = roe_list    # Return on Equity (%)
    df["nim"]         = nim_list    # Net Interest Margin (%)
    df["cir"]         = cir_list    # Cost-to-Income Ratio (%)
    df["ldr"]         = ldr_list    # Loan-to-Deposit Ratio (%)
    df["solvabilite"] = solv_list   # Ratio de Solvabilité (%)
    df["liquidite"]   = liq_list    # Ratio de Liquidité (%)

    log.info("KPIs calculés : ROA, ROE, NIM, CIR, LDR, Solvabilité, Liquidité")
    return df


def add_rankings(df):
    """Ajoute le rang de chaque banque par KPI et par année."""
    log.info("Calcul des classements...")
    for kpi in ["bilan", "produit_net_bancaire", "resultat_net", "fonds_propres", "roa", "roe"]:
        if kpi in df.columns:
            df[f"rang_{kpi}"] = df.groupby("annee")[kpi].rank(ascending=False, method="min")
    return df


def insert_production(df, uri, db_name, collection):
    log.info(f"Insertion dans {collection}...")
    client = MongoClient(uri)
    col    = client[db_name][collection]
    col.drop()

    records = df.to_dict(orient="records")
    for r in records:
        for k, v in r.items():
            if isinstance(v, float) and math.isnan(v):
                r[k] = None

    col.insert_many(records)
    col.create_index([("sigle", 1), ("annee", 1)])
    col.create_index("groupe_bancaire")
    col.create_index("annee")
    log.info(f"Production insérée : {len(records)} docs")
    client.close()


def main():
    log.info("=== ETL 04 — Nettoyage & Data Engineering → Production ===")

    # Charger depuis MongoDB
    client  = MongoClient(MONGO_URI)
    records = list(client[DB_NAME][COL_IN].find({}, {"_id": 0}))
    client.close()

    df = pd.DataFrame(records)
    log.info(f"Données normalisées chargées : {len(df)} lignes")

    # 1. Remplir valeurs manquantes
    df = fill_missing(df)

    # 2. Détecter outliers
    kpi_cols = ["bilan", "fonds_propres", "resultat_net", "produit_net_bancaire"]
    df = remove_outliers(df, kpi_cols)

    # 3. Calculer KPIs
    df = compute_kpis(df)

    # 4. Ajouter classements
    df = add_rankings(df)

    # 5. Sauvegarder CSV production
    os.makedirs(os.path.dirname(CSV_OUT), exist_ok=True)
    df.to_csv(CSV_OUT, index=False, encoding="utf-8-sig")
    log.info(f"CSV production sauvegardé : {CSV_OUT}")

    # 6. Insérer en production MongoDB
    insert_production(df, MONGO_URI, DB_NAME, COL_OUT)

    log.info(f"ETL 04 OK ! Base de production prête : {len(df)} lignes | {df['sigle'].nunique()} banques | années {sorted(df['annee'].dropna().unique().astype(int).tolist())}")


if __name__ == "__main__":
    main()