# =============================================================================
# ml_predictions.py — Module Prédictif ML | Banques Sénégal
# Auteur : Sona KOULIBALY | Mastère 2 Big Data & Data Strategy
# Objectif : Prévisions 2024-2025 pour bilan, résultat net, ROA, ROE
#            par banque — modèle Linear Regression pondérée
# =============================================================================

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
import warnings
warnings.filterwarnings("ignore")


# ─── CONSTANTES ───────────────────────────────────────────────────────────────
ANNEES_PREDICTION  = [2024, 2025]
INDICATEURS_ML     = ["bilan", "resultat_net", "fonds_propres", "roa", "roe", "emploi"]
MIN_ANNEES_REQUIS  = 3   # Minimum d'années historiques pour prédire


# ─── MODÈLE DE PRÉDICTION PAR RÉGRESSION LINÉAIRE ─────────────────────────────
def predict_serie(annees: list, valeurs: list, annees_pred: list) -> dict:
    """
    Prédit les valeurs futures d'une série temporelle.
    Utilise une régression linéaire pondérée — années récentes = plus de poids.

    Args:
        annees      : Liste des années historiques  [2015, 2016, ...]
        valeurs     : Liste des valeurs correspondantes
        annees_pred : Années à prédire              [2024, 2025]

    Returns:
        dict {annee: {valeur, confiance, tendance, r2}}
    """
    # Nettoyer les données — enlever les NaN et zéros
    data = [
        (a, v) for a, v in zip(annees, valeurs)
        if v is not None
        and not (isinstance(v, float) and np.isnan(v))
        and v != 0
    ]

    if len(data) < MIN_ANNEES_REQUIS:
        return {a: None for a in annees_pred}

    X = np.array([d[0] for d in data]).reshape(-1, 1)
    y = np.array([d[1] for d in data])

    # Poids croissants — années récentes ont plus d'influence
    poids = np.linspace(0.5, 1.0, len(y))

    model = LinearRegression()
    model.fit(X, y, sample_weight=poids)

    X_pred = np.array(annees_pred).reshape(-1, 1)
    y_pred = model.predict(X_pred)

    # Score de confiance basé sur R²
    r2        = model.score(X, y, sample_weight=poids)
    confiance = max(0, min(100, r2 * 100))
    tendance  = "📈 Hausse" if model.coef_[0] > 0 else "📉 Baisse"

    return {
        annee: {
            "valeur"   : round(float(pred), 2),
            "confiance": round(confiance, 1),
            "tendance" : tendance,
            "r2"       : round(r2, 3),
        }
        for annee, pred in zip(annees_pred, y_pred)
    }


# ─── PRÉDICTIONS POUR TOUTES LES BANQUES ──────────────────────────────────────
def generate_predictions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Génère les prédictions 2024-2025 pour toutes les banques et indicateurs.

    Args:
        df : DataFrame banques_production avec colonnes annee, sigle, bilan...

    Returns:
        DataFrame avec les prédictions par banque et indicateur
    """
    # Travailler uniquement sur les 27 banques de production
    df_prod  = df[df["sigle"].notna() & (df["sigle"] != "")].copy()
    banques  = sorted(df_prod["sigle"].unique())
    records  = []

    for sigle in banques:
        df_b = df_prod[df_prod["sigle"] == sigle].sort_values("annee")

        for indicateur in INDICATEURS_ML:
            if indicateur not in df_b.columns:
                continue

            annees_vals = df_b["annee"].astype(int).tolist()
            valeurs     = pd.to_numeric(df_b[indicateur], errors="coerce").tolist()
            predictions = predict_serie(annees_vals, valeurs, ANNEES_PREDICTION)

            for annee_pred, pred_data in predictions.items():
                if pred_data is None:
                    continue
                records.append({
                    "sigle"      : sigle,
                    "annee"      : annee_pred,
                    "indicateur" : indicateur,
                    "valeur_pred": pred_data["valeur"],
                    "confiance"  : pred_data["confiance"],
                    "tendance"   : pred_data["tendance"],
                    "r2"         : pred_data["r2"],
                    "type"       : "prediction",
                })

    df_pred = pd.DataFrame(records)
    print(f"✅ ML : {len(df_pred)} prédictions générées pour {len(banques)} banques")
    return df_pred


# ─── SCORING RISQUE ────────────────────────────────────────────────────────────
def compute_risk_score(df: pd.DataFrame, annee: int = None) -> pd.DataFrame:
    """
    Calcule un score de risque composite (0-100) par banque.
    Basé sur : ROA, ROE, Solvabilité, CIR, Résultat Net.

    Score 0-30  → Risque élevé   🔴
    Score 30-60 → Risque modéré  🟡
    Score 60-100→ Risque faible  🟢

    Args:
        df    : DataFrame banques_production
        annee : Année d'analyse (défaut = dernière année disponible)
    """
    if annee is None:
        annee = int(df["annee"].max())

    # Filtrer sur l'année ET sur les banques valides uniquement
    df_a = df[
        (df["annee"] == annee) &
        df["sigle"].notna() &
        (df["sigle"] != "")
    ].copy()

    if df_a.empty:
        return pd.DataFrame()

    scores = []
    for _, row in df_a.iterrows():
        score = 50  # Base neutre

        # ROA : rentabilité des actifs (positif = bon signe)
        roa = pd.to_numeric(row.get("roa"), errors="coerce")
        if pd.notna(roa):
            if roa > 1.5:   score += 15
            elif roa > 0.5: score += 8
            elif roa < 0:   score -= 20

        # ROE : rentabilité des fonds propres
        roe = pd.to_numeric(row.get("roe"), errors="coerce")
        if pd.notna(roe):
            if roe > 10:   score += 12
            elif roe > 5:  score += 6
            elif roe < 0:  score -= 15

        # CIR : coefficient d'exploitation (bas = efficace, < 60% idéal)
        cir = pd.to_numeric(row.get("cir"), errors="coerce")
        if pd.notna(cir):
            if cir < 60:   score += 10
            elif cir < 80: score += 3
            else:          score -= 10

        # Solvabilité : ratio fonds propres / total actif
        solv = pd.to_numeric(row.get("solvabilite"), errors="coerce")
        if pd.notna(solv):
            if solv > 10:  score += 10
            elif solv > 8: score += 5
            else:          score -= 5

        # Résultat net : positif = bonne santé financière
        rn = pd.to_numeric(row.get("resultat_net"), errors="coerce")
        if pd.notna(rn):
            if rn > 0:  score += 8
            else:       score -= 15

        # Clamp entre 0 et 100
        score = max(0, min(100, score))

        # Catégorie de risque
        if score >= 60:
            categorie = "🟢 Faible"
        elif score >= 30:
            categorie = "🟡 Modéré"
        else:
            categorie = "🔴 Élevé"

        scores.append({
            "sigle"       : row["sigle"],
            "annee"       : annee,
            "score_risque": round(score, 1),
            "categorie"   : categorie,
            "groupe"      : row.get("groupe_bancaire", "N/D"),
        })

    df_scores = pd.DataFrame(scores).sort_values("score_risque", ascending=False)
    print(f"✅ Scoring risque calculé pour {len(df_scores)} banques ({annee})")
    return df_scores


# ─── CLASSEMENT PRÉDICTIF FUTUR ────────────────────────────────────────────────
def predict_ranking(df: pd.DataFrame, indicateur: str = "bilan") -> pd.DataFrame:
    """
    Prédit le classement des banques en 2024-2025 pour un indicateur donné.
    Compare rang actuel vs rang prédit pour mesurer les évolutions.

    Returns:
        DataFrame avec sigle, rang_actuel, rang_pred, evolution, evol_label
    """
    df_pred = generate_predictions(df)
    if df_pred.empty:
        return pd.DataFrame()

    annee_actuelle = int(df["annee"].max())

    # Classement actuel — nettoyer les NaN avant le rank
    df_actuel = df[df["annee"] == annee_actuelle][["sigle", indicateur]].copy()
    df_actuel.columns = ["sigle", "valeur_actuelle"]
    df_actuel["valeur_actuelle"] = pd.to_numeric(
        df_actuel["valeur_actuelle"], errors="coerce"
    )
    # Supprimer les lignes avec NaN pour éviter l'erreur astype(int)
    df_actuel = df_actuel.dropna(subset=["valeur_actuelle"])
    df_actuel["rang_actuel"] = (
        df_actuel["valeur_actuelle"].rank(ascending=False).astype(int)
    )

    records = []
    for annee_pred in ANNEES_PREDICTION:
        df_p = df_pred[
            (df_pred["annee"] == annee_pred) &
            (df_pred["indicateur"] == indicateur)
        ].copy()
        if df_p.empty:
            continue

        df_p["rang_pred"] = df_p["valeur_pred"].rank(ascending=False).astype(int)
        df_merged = df_actuel.merge(
            df_p[["sigle","valeur_pred","rang_pred","confiance","tendance"]],
            on="sigle", how="left"
        )
        df_merged["annee_pred"] = annee_pred
        df_merged["evolution"]  = df_merged["rang_actuel"] - df_merged["rang_pred"]
        df_merged["evol_label"] = df_merged["evolution"].apply(
            lambda x: f"▲ +{int(x)}" if x > 0 else (f"▼ {int(x)}" if x < 0 else "═ Stable")
        )
        records.append(df_merged)

    return pd.concat(records, ignore_index=True) if records else pd.DataFrame()


# ─── RÉSUMÉ ML POUR LE DASHBOARD ──────────────────────────────────────────────
def get_ml_summary(df: pd.DataFrame) -> dict:
    """
    Point d'entrée principal appelé par callbacks.py.
    Retourne un dict avec toutes les données ML prêtes pour l'affichage.

    Returns:
        {predictions, risk_scores, ranking_pred, annee_base, annees_pred, nb_banques}
    """
    df_pred   = generate_predictions(df)

    # Trouver la meilleure année pour le scoring
    # (celle avec le plus de ratios ROA/ROE valides — 2023 peut avoir des NaN)
    annees_dispo  = sorted(df["annee"].dropna().unique().astype(int), reverse=True)
    annee_scoring = annees_dispo[0]
    for a in annees_dispo:
        df_test     = df[(df["annee"] == a) & df["sigle"].notna()]
        roa_valides = pd.to_numeric(df_test["roa"], errors="coerce").notna().sum()
        if roa_valides >= 5:
            annee_scoring = a
            break

    df_scores = compute_risk_score(df, annee=annee_scoring)
    df_rank   = predict_ranking(df, "bilan")
    annee_max = int(df["annee"].max())

    return {
        "predictions" : df_pred,
        "risk_scores" : df_scores,
        "ranking_pred": df_rank,
        "annee_base"  : annee_max,
        "annees_pred" : ANNEES_PREDICTION,
        "nb_banques"  : df["sigle"].nunique(),
    }


# ─── TEST AUTONOME ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from pathlib import Path

    csv_path = Path(__file__).parent / "data" / "processed" / "banques_production.csv"
    if not csv_path.exists():
        print(f"❌ Fichier non trouvé : {csv_path}")
        print("   Lance d'abord : python etl/run_etl.py")
    else:
        df = pd.read_csv(csv_path)
        print(f"✅ Données chargées : {len(df)} lignes | {df['sigle'].nunique()} banques")
        print(f"   Années : {sorted(df['annee'].unique())}\n")

        summary = get_ml_summary(df)

        print(f"\n📊 Prédictions : {len(summary['predictions'])} lignes")
        print(f"\n🎯 Scoring risque (top 10) :")
        print(summary['risk_scores'][['sigle','score_risque','categorie']].head(10).to_string(index=False))

        print(f"\n🏆 Classement prédictif bilan 2024 :")
        rank_2024 = summary['ranking_pred'][summary['ranking_pred']['annee_pred'] == 2024]
        print(rank_2024[['sigle','rang_actuel','rang_pred','evol_label','confiance']].head(10).to_string(index=False))