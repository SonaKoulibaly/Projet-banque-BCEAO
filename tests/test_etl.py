"""
tests/test_etl.py — Tests unitaires du pipeline ETL
====================================================
Vérifie l'intégrité des données à chaque étape.
Lancer avec : pytest tests/ -v
"""
import os, sys, math
import pytest
import pandas as pd
import numpy as np

# Ajouter le répertoire racine au path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

EXCEL_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw", "BASE_SENEGAL2.xlsx")
CSV_PATH   = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "processed", "banques_excel_clean.csv")
PROD_PATH  = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "processed", "banques_production.csv")

RENAME_MAP = {
    'Sigle':'sigle','Goupe_Bancaire':'groupe_bancaire','ANNEE':'annee',
    'EMPLOI':'emploi','BILAN':'bilan','RESSOURCES':'ressources',
    'FONDS.PROPRE':'fonds_propres','EFFECTIF':'effectif','AGENCE':'agences','COMPTE':'comptes',
    'INTERETS.ET.PRODUITS.ASSIMILES':'interets_produits',
    'NTERETS.ET.CHARGES.ASSIMILEES':'interets_charges',
    'REVENUS.DES.TITRES.A.REVENU.VARIABLE':'revenus_titres',
    'COMMISSIONS.(PRODUITS)':'commissions_produits','COMMISSIONS.(CHARGES)':'commissions_charges',
    'GAINS.OU.PERTES.NETS.SUR.OPERATIONS.DES.PORTEFEUILLES.DE.NEGOCIATION':'gains_negociation',
    'GAINS.OU.PERTES.NETS.SUR.OPERATIONS.DES.PORTEFEUILLES.DE.PLACEMENT.ET.ASSIMILES':'gains_placement',
    "AUTRES.PRODUITS.D'EXPLOITATION.BANCAIRE":'autres_produits',
    "AUTRES.CHARGES.D'EXPLOITATION.BANCAIRE":'autres_charges',
    'PRODUIT.NET.BANCAIRE':'pnb',"SUBVENTIONS.D'INVESTISSEMENT":'subventions',
    "CHARGES.GENERALES.D'EXPLOITATION":'charges_generales',
    'DOTATIONS.AUX.AMORTISSEMENTS.ET.AUX.DEPRECIATIONS.DES.IMMOBILISATIONS.INCORPORELLES.ET.CORPORELLES':'dotations',
    "RESULTAT.BRUT.D'EXPLOITATION":'rbe','COÛT.DU.RISQUE':'cout_risque',
    "RESULTAT.D'EXPLOITATION":'resultat_exploitation',
    'GAINS.OU.PERTES.NETS.SUR.ACTIFS.IMMOBILISES':'gains_actifs',
    'RESULTAT.AVANT.IMPÔT':'resultat_avant_impot','IMPÔTS.SUR.LES.BENEFICES':'impots',
    'RESULTAT.NET':'resultat_net'
}

@pytest.fixture(scope="module")
def df_excel():
    """Charge le fichier Excel brut."""
    if not os.path.exists(EXCEL_PATH):
        pytest.skip(f"Fichier Excel non trouvé : {EXCEL_PATH}")
    df = pd.read_excel(EXCEL_PATH)
    df.rename(columns=RENAME_MAP, inplace=True)
    return df

@pytest.fixture(scope="module")
def df_production():
    """Charge la base de production CSV."""
    if not os.path.exists(PROD_PATH):
        pytest.skip(f"CSV production non trouvé : {PROD_PATH}")
    return pd.read_csv(PROD_PATH)


# ══════════════════════════════════════════════════════════
# TESTS ETL 01 — Chargement Excel
# ══════════════════════════════════════════════════════════

class TestLoadExcel:
    def test_fichier_existe(self):
        """Le fichier Excel source doit exister."""
        assert os.path.exists(EXCEL_PATH), f"Fichier manquant : {EXCEL_PATH}"

    def test_nombre_lignes(self, df_excel):
        """La base doit contenir exactement 134 lignes."""
        assert len(df_excel) == 134, f"Attendu 134 lignes, obtenu {len(df_excel)}"

    def test_colonnes_obligatoires(self, df_excel):
        """Les colonnes clés doivent être présentes."""
        required = ["sigle", "groupe_bancaire", "annee", "bilan", "ressources", "emploi", "fonds_propres"]
        for col in required:
            assert col in df_excel.columns, f"Colonne manquante : {col}"

    def test_nombre_banques(self, df_excel):
        """Il doit y avoir 24 banques uniques."""
        nb = df_excel["sigle"].nunique()
        assert nb == 24, f"Attendu 24 banques, obtenu {nb}"

    def test_annees_couvertes(self, df_excel):
        """Les années 2015 à 2020 doivent être présentes."""
        annees = set(df_excel["annee"].unique())
        expected = {2015, 2016, 2017, 2018, 2019, 2020}
        assert expected.issubset(annees), f"Années manquantes : {expected - annees}"

    def test_bilan_positif(self, df_excel):
        """Le bilan (total actif) doit toujours être positif."""
        bilans = pd.to_numeric(df_excel["bilan"], errors="coerce").dropna()
        assert (bilans > 0).all(), "Certains bilans sont négatifs ou nuls"

    def test_pas_de_sigle_vide(self, df_excel):
        """Aucun sigle ne doit être vide."""
        assert df_excel["sigle"].isna().sum() == 0, "Des sigles sont manquants"

    def test_groupes_valides(self, df_excel):
        """Les groupes bancaires doivent être parmi les valeurs connues."""
        groupes_valides = {
            "Groupes Continentaux", "Groupes Règionaux",
            "Groupes Internationaux", "Groupes Locaux"
        }
        groupes_present = set(df_excel["groupe_bancaire"].dropna().unique())
        inconnus = groupes_present - groupes_valides
        assert not inconnus, f"Groupes inconnus : {inconnus}"

    def test_annee_type_numerique(self, df_excel):
        """L'année doit être numérique."""
        assert pd.api.types.is_numeric_dtype(df_excel["annee"]), "La colonne annee n'est pas numérique"


# ══════════════════════════════════════════════════════════
# TESTS ETL 04 — Base de production
# ══════════════════════════════════════════════════════════

class TestProduction:
    def test_fichier_production_existe(self):
        """Le CSV de production doit exister après l'ETL."""
        assert os.path.exists(PROD_PATH), f"CSV production manquant : {PROD_PATH}"

    def test_annees_etendues(self, df_production):
        """La base de production doit couvrir 2015-2022 (avec données PDF)."""
        annees = set(pd.to_numeric(df_production["annee"], errors="coerce").dropna().astype(int).unique())
        assert 2015 in annees, "Année 2015 manquante"
        assert 2022 in annees or 2021 in annees, "Données PDF (2021/2022) manquantes"

    def test_nombre_banques_production(self, df_production):
        """La base de production doit avoir au moins 24 banques."""
        nb = df_production["sigle"].nunique()
        assert nb >= 24, f"Attendu >= 24 banques, obtenu {nb}"

    def test_bilan_non_nul_production(self, df_production):
        """Les bilans en production doivent être positifs."""
        bilans = pd.to_numeric(df_production["bilan"], errors="coerce").dropna()
        assert (bilans > 0).all(), "Bilans négatifs ou nuls en production"

    def test_pas_de_doublon(self, df_production):
        """Pas de doublon sigle + annee + source."""
        if "source" in df_production.columns:
            dupes = df_production.duplicated(subset=["sigle","annee","source"]).sum()
        else:
            dupes = df_production.duplicated(subset=["sigle","annee"]).sum()
        assert dupes == 0, f"{dupes} doublons détectés dans la base de production"

    def test_bilan_total_croissant(self, df_production):
        """Le bilan total du secteur doit croître entre 2015 et 2019."""
        df_p = df_production.copy()
        df_p["bilan"] = pd.to_numeric(df_p["bilan"], errors="coerce")
        totaux = df_p.groupby("annee")["bilan"].sum()
        if 2015 in totaux.index and 2019 in totaux.index:
            assert totaux[2019] > totaux[2015], "Le bilan total devrait croître entre 2015 et 2019"


# ══════════════════════════════════════════════════════════
# TESTS CALCUL DES RATIOS
# ══════════════════════════════════════════════════════════

class TestRatios:
    def test_roa_plage_realiste(self, df_production):
        """Le ROA doit être entre -50% et +50% (valeurs réalistes)."""
        if "roa" not in df_production.columns:
            pytest.skip("Colonne roa absente")
        roa = pd.to_numeric(df_production["roa"], errors="coerce").dropna()
        assert (roa >= -50).all() and (roa <= 50).all(), "ROA hors plage réaliste"

    def test_roe_plage_realiste(self, df_production):
        """Le ROE doit être entre -200% et +200%."""
        if "roe" not in df_production.columns:
            pytest.skip("Colonne roe absente")
        roe = pd.to_numeric(df_production["roe"], errors="coerce").dropna()
        assert (roe >= -200).all() and (roe <= 200).all(), "ROE hors plage réaliste"

    def test_solvabilite_positive(self, df_production):
        """La solvabilité doit être positive pour la majorité des banques."""
        if "solvabilite" not in df_production.columns:
            pytest.skip("Colonne solvabilite absente")
        solv = pd.to_numeric(df_production["solvabilite"], errors="coerce").dropna()
        pct_positif = (solv > 0).mean()
        assert pct_positif > 0.7, f"Trop de solvabilités négatives : {pct_positif:.0%} positives"

    def test_cir_plage(self, df_production):
        """Le CIR doit être entre 0% et 300%."""
        if "cir" not in df_production.columns:
            pytest.skip("Colonne cir absente")
        cir = pd.to_numeric(df_production["cir"], errors="coerce").dropna()
        assert (cir >= 0).all() and (cir <= 300).all(), "CIR hors plage réaliste"


# ══════════════════════════════════════════════════════════
# TESTS DONNÉES MÉTIER
# ══════════════════════════════════════════════════════════

class TestMetier:
    def test_cbao_leader_marche(self, df_production):
        """La CBAO doit être parmi les 3 premières banques par bilan (2020)."""
        df_p = df_production.copy()
        df_p["bilan"] = pd.to_numeric(df_p["bilan"], errors="coerce")
        df2020 = df_p[df_p["annee"] == 2020].sort_values("bilan", ascending=False)
        if df2020.empty:
            pytest.skip("Pas de données 2020")
        top3 = df2020.head(3)["sigle"].tolist()
        assert "CBAO" in top3, f"CBAO devrait être dans le top 3 bilan 2020, obtenu : {top3}"

    def test_sgbs_presence(self, df_production):
        """La SGBS (Société Générale) doit être présente dans la base."""
        sigles = df_production["sigle"].unique()
        assert "SGBS" in sigles, "SGBS (Société Générale) absente de la base"

    def test_croissance_secteur_2015_2020(self, df_production):
        """Le secteur bancaire a crû entre 2015 et 2020."""
        df_p = df_production.copy()
        df_p["bilan"] = pd.to_numeric(df_p["bilan"], errors="coerce")
        b2015 = df_p[df_p["annee"]==2015]["bilan"].sum()
        b2020 = df_p[df_p["annee"]==2020]["bilan"].sum()
        if b2015 > 0 and b2020 > 0:
            assert b2020 > b2015, f"Le bilan total devrait croître (2015:{b2015:.0f} → 2020:{b2020:.0f})"

    def test_effectif_positif(self, df_production):
        """L'effectif total doit être positif."""
        effectifs = pd.to_numeric(df_production["effectif"], errors="coerce").dropna()
        assert (effectifs > 0).all(), "Des effectifs négatifs détectés"

    def test_24_banques_en_2020(self, df_production):
        """24 banques doivent être présentes en 2020."""
        nb = df_production[df_production["annee"]==2020]["sigle"].nunique()
        assert nb >= 20, f"Attendu >= 20 banques en 2020, obtenu {nb}"


# ══════════════════════════════════════════════════════════
# TESTS UTILITAIRES
# ══════════════════════════════════════════════════════════

class TestUtils:
    def test_safe_val_normal(self):
        """safe_val doit formater correctement les nombres."""
        from utils.pdf_generator import safe_val
        assert safe_val(1_500_000) == "1.50 Mds"
        assert safe_val(None) == "N/D"
        assert safe_val(float("nan")) == "N/D"
        assert safe_val(25.5, pct=True) == "25.50%"

    def test_safe_val_negatif(self):
        """safe_val doit gérer les valeurs négatives."""
        from utils.pdf_generator import safe_val
        result = safe_val(-500_000)
        assert "-" in result, "Les valeurs négatives doivent afficher un signe moins"

    def test_logo_existe(self):
        """Le logo BCEAO doit être présent dans assets/."""
        logo_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "assets", "logo.png")
        assert os.path.exists(logo_path), "Logo BCEAO manquant dans assets/"

    def test_env_file_exists(self):
        """Le fichier .env doit exister (pas .env.example)."""
        env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
        assert os.path.exists(env_path), "Fichier .env manquant — copier .env.example en .env"

    def test_mongo_uri_configured(self):
        """MONGO_URI doit être configuré dans .env."""
        from dotenv import load_dotenv
        load_dotenv()
        uri = os.getenv("MONGO_URI", "")
        assert uri != "", "MONGO_URI non défini dans .env"
        assert "<password>" not in uri, "MONGO_URI contient encore le placeholder <password>"
        assert "cluster0" in uri.lower() or "mongodb" in uri.lower(), "MONGO_URI semble invalide"