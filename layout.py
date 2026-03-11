"""
layout.py — Interface utilisateur du Dashboard Bancaire Sénégal
================================================================
Design  : Luxe financier africain — Marine profond + Or sénégalais + Blanc cassé
Typo    : Syne (titres) + DM Sans (corps)
Mise à jour : Ajout onglet 🤖 Prévisions ML + données 2015–2023
"""

from dash import dcc, html
import dash_bootstrap_components as dbc

# ─── Palette de couleurs centralisée ─────────────────────────────────────────
COLORS = {
    "navy"   : "#0A1628",
    "navy2"  : "#0F2040",
    "gold"   : "#D4A843",
    "gold2"  : "#F0C060",
    "cream"  : "#F8F4EE",
    "white"  : "#FFFFFF",
    "success": "#2ECC71",
    "danger" : "#E74C3C",
    "muted"  : "#8899AA",
    "card_bg": "#111E35",
    "border" : "#1E3050",
}


def create_layout(app, df, annees, banques, groupes):
    """
    Construit le layout principal du dashboard.

    Args:
        app     : Instance Dash
        df      : DataFrame banques_production (utilisé pour les valeurs initiales)
        annees  : Liste des années disponibles  [2015..2023]
        banques : Liste des sigles de banques
        groupes : Liste des groupes bancaires
    """

    return html.Div([

        # ── Store partagé entre tous les callbacks ────────────────
        dcc.Store(id="store-filtered"),
        # Store pour les prédictions ML (évite recalcul à chaque clic)
        dcc.Store(id="store-ml"),

        # ══════════════════════════════════════════════════════════
        # HEADER
        # ══════════════════════════════════════════════════════════
        html.Header([
            html.Div([

                # Logo
                html.Div([
                    html.Div([
                        html.Img(src="/assets/logo.png",
                                 style={"height":"46px","width":"46px",
                                        "borderRadius":"8px","objectFit":"contain"}),
                        html.Div([
                            html.Span("BANQUE",   className="logo-top"),
                            html.Span("SÉNÉGAL",  className="logo-bottom"),
                        ], className="logo-text"),
                    ], className="logo-wrap"),
                ], className="header-logo"),

                # Titre central mis à jour avec 2023
                html.Div([
                    html.H1("Positionnement des Banques au Sénégal",
                            className="header-title"),
                    html.P("Analyse & Data Visualisation · BCEAO · 2015–2023",
                           className="header-sub"),
                ], className="header-center"),

                # Boutons téléchargement
                html.Div([
                    html.Button([html.Span("⬇"), " Excel"],
                        id="btn-download-excel", className="btn-dl btn-excel", n_clicks=0),
                    html.Button([html.Span("⬇"), " HTML"],
                        id="btn-download-html", className="btn-dl btn-html", n_clicks=0),
                    html.Button([html.Span("📄"), " Rapport PDF"],
                        id="btn-download-pdf", className="btn-dl btn-pdf", n_clicks=0),
                    dcc.Download(id="download-excel"),
                    dcc.Download(id="download-html"),
                    dcc.Download(id="download-pdf"),
                ], className="header-actions"),

            ], className="header-inner"),
        ], className="main-header"),

        # ══════════════════════════════════════════════════════════
        # BANDEAU KPI GLOBAUX — mis à jour dynamiquement
        # ══════════════════════════════════════════════════════════
        html.Section([
            dbc.Row([

                # KPI 1 : Total Actif Bancaire
                dbc.Col(html.Div([
                    html.Div([
                        html.Span("💰", style={"fontSize":"28px"}),
                        html.Div([
                            html.P("Total Actif Bancaire", className="kpi-label"),
                            html.H3(id="kpi-bilan", className="kpi-value",
                                    style={"color": COLORS["gold"]}),
                            html.P(id="kpi-bilan-sub", className="kpi-sub"),
                        ]),
                    ], className="kpi-inner"),
                ], className="kpi-card"), xs=12, sm=6, md=3),

                # KPI 2 : Banques Actives
                dbc.Col(html.Div([
                    html.Div([
                        html.Span("🏦", style={"fontSize":"28px"}),
                        html.Div([
                            html.P("Banques Actives", className="kpi-label"),
                            html.H3(id="kpi-banques", className="kpi-value",
                                    style={"color": "#5BC8F5"}),
                            html.P("Sur le marché sénégalais", className="kpi-sub"),
                        ]),
                    ], className="kpi-inner"),
                ], className="kpi-card"), xs=12, sm=6, md=3),

                # KPI 3 : Effectif Total
                dbc.Col(html.Div([
                    html.Div([
                        html.Span("👥", style={"fontSize":"28px"}),
                        html.Div([
                            html.P("Effectif Total", className="kpi-label"),
                            html.H3(id="kpi-effectif", className="kpi-value",
                                    style={"color": COLORS["success"]}),
                            html.P(id="kpi-effectif-sub", className="kpi-sub"),
                        ]),
                    ], className="kpi-inner"),
                ], className="kpi-card"), xs=12, sm=6, md=3),

                # KPI 4 : Fonds Propres Moy.
                dbc.Col(html.Div([
                    html.Div([
                        html.Span("📊", style={"fontSize":"28px"}),
                        html.Div([
                            html.P("Fonds Propres Moy.", className="kpi-label"),
                            html.H3(id="kpi-fonds", className="kpi-value",
                                    style={"color": "#B87BFF"}),
                            html.P("Moyenne par banque", className="kpi-sub"),
                        ]),
                    ], className="kpi-inner"),
                ], className="kpi-card"), xs=12, sm=6, md=3),

            ], className="g-3"),
        ], className="kpi-section"),

        # ══════════════════════════════════════════════════════════
        # PANNEAU DE FILTRES
        # ══════════════════════════════════════════════════════════
        html.Section([
            html.Div([

                # Filtre années — slider dynamique 2015-2023
                html.Div([
                    html.Label("📅 PÉRIODE", className="filter-label"),
                    dcc.RangeSlider(
                        id="filter-annee",
                        min=min(annees), max=max(annees),
                        value=[min(annees), max(annees)],
                        marks={y: {
                            "label": str(y),
                            "style": {"color": COLORS["cream"], "fontSize": "11px"}
                        } for y in annees},
                        step=1,
                        tooltip={"placement": "bottom", "always_visible": False},
                        className="slider-custom",
                    ),
                ], className="filter-block filter-slider"),

                # Filtre banques
                html.Div([
                    html.Label("🏦 BANQUES", className="filter-label"),
                    dcc.Dropdown(
                        id="filter-banque",
                        options=[{"label": b, "value": b} for b in banques],
                        value=None,
                        placeholder="Toutes les banques...",
                        multi=True,
                        className="dropdown-custom",
                        clearable=True,
                    ),
                ], className="filter-block"),

                # Filtre groupes
                html.Div([
                    html.Label("🌍 GROUPE", className="filter-label"),
                    dcc.Dropdown(
                        id="filter-groupe",
                        options=[{"label": g, "value": g} for g in groupes],
                        value=None,
                        placeholder="Tous les groupes...",
                        multi=True,
                        className="dropdown-custom",
                    ),
                ], className="filter-block"),

                # Filtre indicateur
                html.Div([
                    html.Label("📊 INDICATEUR", className="filter-label"),
                    dcc.Dropdown(
                        id="filter-indicateur",
                        options=[
                            {"label": "🏛 Bilan (Total Actif)",     "value": "bilan"},
                            {"label": "💼 Emplois (Crédits)",        "value": "emploi"},
                            {"label": "💵 Ressources (Dépôts)",      "value": "ressources"},
                            {"label": "🏗 Fonds Propres",            "value": "fonds_propres"},
                            {"label": "📈 Produit Net Bancaire",     "value": "pnb"},
                            {"label": "💰 Résultat Net",             "value": "resultat_net"},
                            {"label": "⚖️ ROA (%)",                  "value": "roa"},
                            {"label": "📉 ROE (%)",                  "value": "roe"},
                            {"label": "🔄 Coefficient Exploitation", "value": "cir"},
                        ],
                        value="bilan",
                        clearable=False,
                        className="dropdown-custom",
                    ),
                ], className="filter-block"),

                # Bouton reset
                html.Div([
                    html.Label("↺ RESET", className="filter-label"),
                    html.Button("🔄 Réinitialiser",
                        id="btn-reset-filters",
                        className="btn-reset",
                        n_clicks=0,
                    ),
                ], className="filter-block filter-reset"),

            ], className="filters-inner"),
        ], className="filters-section"),

        # ══════════════════════════════════════════════════════════
        # ONGLETS PRINCIPAUX — 7 onglets dont 1 nouveau ML
        # ══════════════════════════════════════════════════════════
        html.Section([
            dcc.Tabs(id="main-tabs", value="tab-marche",
                     className="main-tabs", children=[

                dcc.Tab(label="🏛 Vue Marché",       value="tab-marche",
                        className="tab-item", selected_className="tab-selected"),
                dcc.Tab(label="⚖️ Comparaison",      value="tab-comparaison",
                        className="tab-item", selected_className="tab-selected"),
                dcc.Tab(label="📈 Performance",      value="tab-performance",
                        className="tab-item", selected_className="tab-selected"),
                dcc.Tab(label="⚙️ Ratios Financiers",value="tab-ratios",
                        className="tab-item", selected_className="tab-selected"),
                dcc.Tab(label="🗺 Carte Sénégal",    value="tab-carte",
                        className="tab-item", selected_className="tab-selected"),
                dcc.Tab(label="🏆 Classement",       value="tab-classement",
                        className="tab-item", selected_className="tab-selected"),

                # NOUVEL ONGLET : Prévisions ML
                dcc.Tab(label="🤖 Prévisions ML",    value="tab-ml",
                        className="tab-item tab-ml", selected_className="tab-selected"),

            ]),

            # Conteneur dynamique des onglets
            html.Div(id="tabs-content", className="tabs-content"),

        ], className="main-tabs-section"),

        # ══════════════════════════════════════════════════════════
        # SECTION ANALYSE INDIVIDUELLE D'UNE BANQUE
        # ══════════════════════════════════════════════════════════
        html.Section([
            html.Div([
                html.H2("🔍 Analyse Individuelle d'une Banque",
                        className="section-title"),
                html.P("Sélectionnez une banque pour générer son profil complet",
                       className="section-sub"),
            ], className="section-header"),

            dbc.Row([
                dbc.Col([
                    dcc.Dropdown(
                        id="select-banque-profil",
                        options=[{"label": b, "value": b} for b in banques],
                        value=banques[0] if banques else None,
                        clearable=False,
                        className="dropdown-custom dropdown-lg",
                        placeholder="Choisir une banque...",
                    ),
                ], md=4),
                dbc.Col([
                    dcc.Dropdown(
                        id="select-annee-profil",
                        options=[{"label": str(a), "value": a} for a in annees],
                        value=max(annees),   # ← 2023 par défaut
                        clearable=False,
                        className="dropdown-custom dropdown-lg",
                    ),
                ], md=2),
                dbc.Col([
                    html.Button("📄 Générer & Télécharger le Rapport PDF",
                        id="btn-generer-rapport",
                        className="btn-primary-action",
                        n_clicks=0),
                    dcc.Download(id="download-rapport-individuel"),
                ], md=3),
            ], className="g-3 mb-4"),

            # KPIs banque sélectionnée
            html.Div(id="profil-banque-kpi"),

            # Graphiques profil
            dbc.Row([
                dbc.Col(dcc.Graph(id="graph-profil-evolution"), md=8),
                dbc.Col(dcc.Graph(id="graph-profil-radar"),     md=4),
            ], className="g-3"),

        ], className="individual-section"),

        # ══════════════════════════════════════════════════════════
        # FOOTER — mis à jour avec 2023
        # ══════════════════════════════════════════════════════════
        html.Footer([
            html.Div([
                html.Span("🏦 Dashboard Banques Sénégal"),
                html.Span("·"),
                html.Span("Source : BCEAO · Base Sénégal 2015–2023"),
                html.Span("·"),
                html.Span("Python · Dash · MongoDB Atlas · ML Prédictif Par Sona.K"),
            ], className="footer-inner"),
        ], className="main-footer"),


    ], className="app-wrapper")
