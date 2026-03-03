"""
app.py — Cœur de l'application Dash Bancaire Sénégal
======================================================
Point d'entrée principal. Initialise Dash + Flask,
charge les données depuis MongoDB Atlas (fallback CSV),
expose le serveur pour Render (Gunicorn).
Données : 2015–2023 | 27 banques | Source BCEAO
"""

import os
import pandas as pd
from dash import Dash
import dash_bootstrap_components as dbc
from dotenv import load_dotenv

load_dotenv()


# ─── CHARGEMENT DES DONNÉES ───────────────────────────────────────────────────
def load_data():
    """
    Charge les données depuis MongoDB Atlas (banques_production).
    Fallback automatique sur CSV local si MongoDB indisponible.
    """
    mongo_uri = os.getenv("MONGO_URI", "")

    if mongo_uri and "<password>" not in mongo_uri:
        try:
            from pymongo import MongoClient
            client  = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            db      = client[os.getenv("DB_NAME", "banque_senegal")]
            records = list(db["banques_production"].find({}, {"_id": 0}))
            client.close()
            if records:
                df = pd.DataFrame(records)
                print(f"✅ MongoDB Atlas : {len(df)} enregistrements chargés")
                return df
        except Exception as e:
            print(f"⚠️  MongoDB indisponible ({e}) — fallback CSV...")

    # Fallback CSV local
    csv_path = os.path.join(
        os.path.dirname(__file__), "data", "processed", "banques_production.csv"
    )
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        print(f"✅ CSV local : {len(df)} enregistrements chargés")
        return df

    raise FileNotFoundError(
        "❌ Aucune source de données disponible. "
        "Configurez MONGO_URI ou placez banques_production.csv dans data/processed/"
    )


# ─── INITIALISATION ───────────────────────────────────────────────────────────
DF      = load_data()
ANNEES  = sorted(DF["annee"].dropna().unique().astype(int).tolist())
BANQUES = sorted(DF["sigle"].dropna().unique().tolist())
GROUPES = sorted(DF["groupe_bancaire"].dropna().unique().tolist())

app = Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        "https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=DM+Sans:wght@300;400;500&display=swap",
        "/assets/style.css",
    ],
    suppress_callback_exceptions=True,
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
    title="Banques Sénégal | Positionnement 2015–2023",
)

# Exposer le serveur Flask pour Gunicorn (Render)
server = app.server

from layout   import create_layout
from callbacks import register_callbacks

app.layout = create_layout(app, DF, ANNEES, BANQUES, GROUPES)
register_callbacks(app, DF)

if __name__ == "__main__":
    port  = int(os.getenv("PORT", 7654))
    debug = os.getenv("DEBUG", "True").lower() == "true"
    app.run(debug=debug, host="0.0.0.0", port=port)