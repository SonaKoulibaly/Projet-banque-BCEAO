"""
callbacks.py — Logique interactive du Dashboard Bancaire Sénégal
================================================================
Callbacks Dash : filtres, graphiques, téléchargements, profil banque,
                 KPIs détaillés actif/passif, module ML prédictif.
Données : 2015–2023 | 27 banques | Source BCEAO
"""

import io
import math
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from dash import Input, Output, State, dcc, html, no_update
import dash_bootstrap_components as dbc

# ─── Palette centralisée ──────────────────────────────────────────────────────
NAVY    = "#060C0F"
NAVY2   = "#0A1510"
GOLD    = "#FF6B5B"
GOLD2   = "#FFB347"
CREAM   = "#F5F2EC"
SUCCESS = "#2ECC71"
DANGER  = "#E74C3C"
MUTED   = "#7A9A88"

# Palette de 24 couleurs distinctes pour les banques
PALETTE_BANQUES = [
    "#D4A843","#5BC8F5","#2ECC71","#E74C3C","#B87BFF",
    "#FF6B6B","#4ECDC4","#FFE66D","#A8E6CF","#FF8B94",
    "#6C5CE7","#00B894","#FDCB6E","#E17055","#74B9FF",
    "#FD79A8","#55EFC4","#636E72","#DFE6E9","#2D3436",
    "#00CEC9","#6D214F","#182C61","#F8EFBA",
]

# Thème sombre appliqué à toutes les figures Plotly
PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(11,22,40,0.6)",
    font=dict(family="DM Sans", color=CREAM, size=12),
    title_font=dict(family="Syne", color=CREAM, size=15),
    legend=dict(bgcolor="rgba(0,0,0,0.3)", bordercolor=GOLD, borderwidth=1,
                font=dict(color=CREAM, size=11)),
    margin=dict(l=40, r=20, t=50, b=40),
    xaxis=dict(gridcolor="rgba(255,255,255,0.07)", zeroline=False, color=CREAM),
    yaxis=dict(gridcolor="rgba(255,255,255,0.07)", zeroline=False, color=CREAM),
)

# Labels lisibles pour les colonnes
COL_LABELS = {
    "bilan"        : "Bilan (M FCFA)",
    "emploi"       : "Emplois (M FCFA)",
    "ressources"   : "Ressources (M FCFA)",
    "fonds_propres": "Fonds Propres (M FCFA)",
    "pnb"          : "Produit Net Bancaire (M FCFA)",
    "resultat_net" : "Résultat Net (M FCFA)",
    "roa"          : "ROA (%)",
    "roe"          : "ROE (%)",
    "cir"          : "Coefficient d'Exploitation (%)",
    "solvabilite"  : "Solvabilité (%)",
}

# Colonnes détaillées de l'actif disponibles dans banques_production
# Source : extraction PDF BCEAO — bilans individuels par banque
ACTIF_COLS = {
    "actif_effets_publics"                  : "Effets Publics",
    "actif_obligations_titres_revenu_fixe"  : "Obligations & Titres",
    "actif_actions_titres_revenu_variable"  : "Actions & Titres Variables",
    "actif_caisse_banque_centrale"          : "Caisse & Banque Centrale",
}


# ─── UTILITAIRES ──────────────────────────────────────────────────────────────
def safe_fmt(val, suffix=" M", decimals=0):
    """Formate une valeur numérique proprement, retourne N/D si invalide."""
    try:
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return "N/D"
        if abs(val) >= 1_000_000:
            return f"{val/1_000_000:.{decimals}f} Mds"
        if abs(val) >= 1_000:
            return f"{val/1_000:.{decimals}f} K"
        return f"{val:.{decimals}f}{suffix}"
    except Exception:
        return "N/D"


def apply_plotly_defaults(fig, title=""):
    """Applique le thème sombre LUXE FINANCIER AFRICAIN à une figure Plotly."""
    fig.update_layout(**PLOTLY_LAYOUT)
    if title:
        fig.update_layout(title=dict(text=title, x=0.02, xanchor="left"))
    return fig


# ─── ENREGISTREMENT DES CALLBACKS ────────────────────────────────────────────
def register_callbacks(app, df_global):
    """
    Enregistre tous les callbacks Dash.

    Args:
        app       : Instance Dash
        df_global : DataFrame banques_production chargé depuis MongoDB
    """

    # ══════════════════════════════════════════════════════════
    # CB-00 : Reset tous les filtres vers valeurs par défaut
    # ══════════════════════════════════════════════════════════
    @app.callback(
        Output("filter-annee",      "value"),
        Output("filter-banque",     "value"),
        Output("filter-groupe",     "value"),
        Output("filter-indicateur", "value"),
        Input("btn-reset-filters",  "n_clicks"),
        prevent_initial_call=True,
    )
    def reset_filters(n):
        """Réinitialise tous les filtres — plage années complète, aucune sélection."""
        annees = sorted(df_global["annee"].dropna().unique().astype(int).tolist())
        return [min(annees), max(annees)], None, None, "bilan"

    # ══════════════════════════════════════════════════════════
    # CB-00b : KPIs globaux — mis à jour selon les filtres
    # ══════════════════════════════════════════════════════════
    @app.callback(
        Output("kpi-bilan",        "children"),
        Output("kpi-bilan-sub",    "children"),
        Output("kpi-banques",      "children"),
        Output("kpi-effectif",     "children"),
        Output("kpi-effectif-sub", "children"),
        Output("kpi-fonds",        "children"),
        Input("filter-annee",      "value"),
        Input("filter-banque",     "value"),
        Input("filter-groupe",     "value"),
    )
    def update_kpis(annee_range, banques_sel, groupes_sel):
        """
        Recalcule les 4 KPIs globaux en temps réel selon les filtres.
        Affiche toujours les données de la dernière année sélectionnée.
        """
        dff = df_global.copy()

        # Appliquer les filtres actifs
        if annee_range:
            dff = dff[(dff["annee"] >= annee_range[0]) & (dff["annee"] <= annee_range[1])]
        if banques_sel:
            dff = dff[dff["sigle"].isin(banques_sel)]
        if groupes_sel:
            dff = dff[dff["groupe_bancaire"].isin(groupes_sel)]

        if dff.empty:
            return "N/D", "", "0", "N/D", "", "N/D"

        # Calcul sur la dernière année de la sélection
        last_y  = int(dff["annee"].max())
        df_last = dff[dff["annee"] == last_y]

        total_bilan = pd.to_numeric(df_last["bilan"],        errors="coerce").sum()
        nb_banques  = df_last["sigle"].nunique()
        effectif    = pd.to_numeric(df_last["effectif"],     errors="coerce").sum()
        fonds_prop  = pd.to_numeric(df_last["fonds_propres"],errors="coerce").mean()

        # Formater les valeurs
        bilan_fmt   = f"{total_bilan/1_000_000:.2f} Mds FCFA" if total_bilan > 0 else "N/D"
        bilan_sub   = f"Secteur bancaire {last_y}"
        effectif_fmt = f"{int(effectif):,}".replace(",","·") if effectif > 0 else "N/D"
        fonds_fmt   = (f"{fonds_prop/1000:.1f} Mds"
                       if (fonds_prop and not math.isnan(fonds_prop) and fonds_prop > 0)
                       else "N/D")

        return bilan_fmt, bilan_sub, str(nb_banques), effectif_fmt, f"Employés en {last_y}", fonds_fmt

    # ══════════════════════════════════════════════════════════
    # CB-01 : Filtrage → Store partagé entre tous les callbacks
    # ══════════════════════════════════════════════════════════
    @app.callback(
        Output("store-filtered", "data"),
        Input("filter-annee",    "value"),
        Input("filter-banque",   "value"),
        Input("filter-groupe",   "value"),
    )
    def update_store(annee_range, banques_sel, groupes_sel):
        """
        Filtre df_global et stocke le résultat en JSON.
        Partagé par tous les callbacks de visualisation pour éviter
        de recalculer les filtres indépendamment dans chaque callback.
        """
        dff = df_global.copy()
        if annee_range:
            dff = dff[(dff["annee"] >= annee_range[0]) & (dff["annee"] <= annee_range[1])]
        if banques_sel:
            dff = dff[dff["sigle"].isin(banques_sel)]
        if groupes_sel:
            dff = dff[dff["groupe_bancaire"].isin(groupes_sel)]
        return dff.to_json(date_format="iso", orient="split")

    # ══════════════════════════════════════════════════════════
    # CB-02 : Rendu des onglets
    # ══════════════════════════════════════════════════════════
    @app.callback(
        Output("tabs-content",     "children"),
        Input("main-tabs",         "value"),
        Input("store-filtered",    "data"),
        Input("filter-indicateur", "value"),
    )
    def render_tab(tab, store_data, indicateur):
        """
        Génère le contenu HTML/Plotly de l'onglet actif.
        Lit les données filtrées depuis le store partagé.
        """
        if not store_data:
            return html.Div("Chargement...", style={"color": CREAM})

        dff       = pd.read_json(io.StringIO(store_data), orient="split")
        ind       = indicateur or "bilan"
        ind_label = COL_LABELS.get(ind, ind)

        # ── ONGLET 1 : VUE MARCHÉ ─────────────────────────────
        if tab == "tab-marche":
            # Évolution sectorielle 2015-2023 — toutes années confondues
            df_yr = df_global.groupby("annee")[
                ["bilan","ressources","emploi","fonds_propres"]
            ].sum().reset_index()

            fig_evol = go.Figure()
            for col, name, color in [
                ("bilan",       "Total Actif",   GOLD),
                ("ressources",  "Ressources",    "#5BC8F5"),
                ("emploi",      "Emplois",       SUCCESS),
                ("fonds_propres","Fonds Propres","#B87BFF"),
            ]:
                fig_evol.add_trace(go.Scatter(
                    x=df_yr["annee"], y=df_yr[col]/1000,
                    name=name, line=dict(color=color, width=2.5),
                    mode="lines+markers",
                    hovertemplate=f"<b>{name}</b><br>%{{x}}: %{{y:,.0f}} Mds FCFA<extra></extra>",
                ))
            apply_plotly_defaults(fig_evol, "📈 Évolution du Secteur Bancaire Sénégalais 2015–2023 (Mds FCFA)")

            # Camembert répartition par groupe
            df_grp = dff.groupby("groupe_bancaire")[ind].sum().reset_index()
            df_grp = df_grp[df_grp[ind] > 0]
            fig_pie = go.Figure(go.Pie(
                labels=df_grp["groupe_bancaire"],
                values=df_grp[ind],
                hole=0.55,
                marker=dict(colors=PALETTE_BANQUES[:len(df_grp)],
                            line=dict(color=NAVY2, width=2)),
                textinfo="label+percent",
                hovertemplate="<b>%{label}</b><br>%{value:,.0f} M FCFA<br>%{percent}<extra></extra>",
            ))
            fig_pie.update_layout(
                title=dict(text=f"Répartition par Groupe · {ind_label}",
                           font=dict(family="Syne", color=CREAM, size=14)),
                annotations=[dict(text="Groupes", x=0.5, y=0.5,
                                  font_size=13, showarrow=False, font_color=CREAM)],
                **{k:v for k,v in PLOTLY_LAYOUT.items() if k not in ("xaxis","yaxis")},
            )

            # Parts de marché bilan — dernière année
            last_y = dff["annee"].max()
            df_pm  = (dff[dff["annee"] == last_y][["sigle","bilan"]]
                      .dropna().sort_values("bilan", ascending=True))
            df_pm["part"] = df_pm["bilan"] / df_pm["bilan"].sum() * 100

            fig_pm = go.Figure(go.Bar(
                x=df_pm["part"], y=df_pm["sigle"],
                orientation="h",
                marker=dict(color=df_pm["part"],
                            colorscale=[[0,NAVY2],[0.5,GOLD],[1,GOLD2]],
                            showscale=False),
                text=df_pm["part"].apply(lambda x: f"{x:.1f}%"),
                textposition="outside",
                textfont=dict(color=CREAM, size=10),
                hovertemplate="<b>%{y}</b><br>Part de marché: %{x:.2f}%<extra></extra>",
            ))
            apply_plotly_defaults(fig_pm, f"🏆 Parts de Marché — {ind_label} ({last_y})")
            fig_pm.update_layout(height=600, yaxis=dict(tickfont=dict(size=11)))

            return html.Div([
                dbc.Row([
                    dbc.Col(dcc.Graph(figure=fig_evol, config={"displayModeBar":False}), md=8),
                    dbc.Col(dcc.Graph(figure=fig_pie,  config={"displayModeBar":False}), md=4),
                ], className="g-3 mb-3"),
                dbc.Row([
                    dbc.Col(dcc.Graph(figure=fig_pm, config={"displayModeBar":False}), md=12),
                ]),
            ])

        # ── ONGLET 2 : COMPARAISON ────────────────────────────
        elif tab == "tab-comparaison":
            last_y  = dff["annee"].max()
            df_sc   = dff[dff["annee"] == last_y][
                ["sigle","bilan","resultat_net","fonds_propres","groupe_bancaire"]
            ].dropna(subset=["bilan"])
            df_sc["resultat_net"]  = pd.to_numeric(df_sc["resultat_net"],  errors="coerce").fillna(0)
            df_sc["fonds_propres"] = pd.to_numeric(df_sc["fonds_propres"], errors="coerce").abs().fillna(1)

            # Scatter bilan vs résultat net — taille = fonds propres
            fig_scatter = px.scatter(
                df_sc, x="bilan", y="resultat_net",
                size="fonds_propres", color="groupe_bancaire",
                text="sigle", size_max=60,
                color_discrete_sequence=PALETTE_BANQUES,
                labels={"bilan":"Bilan (M FCFA)","resultat_net":"Résultat Net (M FCFA)",
                        "groupe_bancaire":"Groupe"},
            )
            fig_scatter.update_traces(textposition="top center",
                                      textfont=dict(color=CREAM, size=9))
            apply_plotly_defaults(fig_scatter, f"🔵 Bilan vs Résultat Net ({last_y}) — taille = Fonds Propres")

            # Barres groupées par banque et année
            df_bar  = dff.groupby(["annee","sigle"])[ind].sum().reset_index()
            fig_bar = px.bar(
                df_bar, x="sigle", y=ind, color="annee",
                barmode="group",
                color_discrete_sequence=px.colors.sequential.YlOrBr,
                labels={"sigle":"Banque", ind:ind_label, "annee":"Année"},
            )
            apply_plotly_defaults(fig_bar, f"📊 Comparaison {ind_label} par Banque et Année")
            fig_bar.update_layout(xaxis_tickangle=-45)

            return html.Div([
                dbc.Row([
                    dbc.Col(dcc.Graph(figure=fig_scatter, config={"displayModeBar":False}), md=6),
                    dbc.Col(dcc.Graph(figure=fig_bar,     config={"displayModeBar":False}), md=6),
                ], className="g-3"),
            ])

        # ── ONGLET 3 : PERFORMANCE ────────────────────────────
        elif tab == "tab-performance":
            # Heatmap banques × années
            df_heat = dff.pivot_table(index="sigle", columns="annee",
                                      values=ind, aggfunc="sum")
            fig_heat = go.Figure(go.Heatmap(
                z=df_heat.values,
                x=[str(c) for c in df_heat.columns],
                y=df_heat.index.tolist(),
                colorscale=[[0,NAVY2],[0.5,GOLD],[1,GOLD2]],
                hovertemplate="<b>%{y}</b><br>%{x}: %{z:,.0f}<extra></extra>",
                showscale=True,
                colorbar=dict(tickfont=dict(color=CREAM)),
            ))
            apply_plotly_defaults(fig_heat, f"🌡 Heatmap {ind_label} — Banques × Années 2015–2023")
            fig_heat.update_layout(height=580)

            # Waterfall croissance
            annees_disp = sorted(dff["annee"].unique())
            if len(annees_disp) >= 2:
                y1, y2   = annees_disp[0], annees_disp[-1]
                df_wf    = (dff[dff["annee"].isin([y1,y2])]
                            .groupby(["sigle","annee"])[ind].sum().unstack())
                df_wf.columns = ["debut","fin"]
                df_wf["croissance"] = df_wf["fin"] - df_wf["debut"]
                df_wf = df_wf.sort_values("croissance", ascending=False).head(12)

                fig_wf = go.Figure(go.Bar(
                    x=df_wf.index.tolist(),
                    y=df_wf["croissance"],
                    marker_color=[SUCCESS if v >= 0 else DANGER for v in df_wf["croissance"]],
                    text=df_wf["croissance"].apply(lambda x: f"{x:+,.0f}"),
                    textposition="outside", textfont=dict(color=CREAM, size=9),
                    hovertemplate="<b>%{x}</b><br>Croissance: %{y:+,.0f}<extra></extra>",
                ))
                apply_plotly_defaults(fig_wf, f"📈 Croissance {ind_label} ({y1}→{y2})")
            else:
                fig_wf = go.Figure()

            return html.Div([
                dbc.Row([
                    dbc.Col(dcc.Graph(figure=fig_heat, config={"displayModeBar":False}), md=12),
                ], className="mb-3"),
                dbc.Row([
                    dbc.Col(dcc.Graph(figure=fig_wf, config={"displayModeBar":False}), md=12),
                ]),
            ])

        # ── ONGLET 4 : RATIOS FINANCIERS + KPIs DÉTAILLÉS ACTIF
        elif tab == "tab-ratios":
            last_y = dff["annee"].max()
            df_r   = dff[dff["annee"] == last_y].copy()

            # Recalculer les ratios si absents (sécurité)
            for col_num, col_den, col_out in [
                ("resultat_net","bilan",        "roa"),
                ("resultat_net","fonds_propres","roe"),
                ("fonds_propres","bilan",       "solvabilite"),
            ]:
                if col_out not in df_r.columns or df_r[col_out].isna().all():
                    num = pd.to_numeric(df_r[col_num], errors="coerce")
                    den = pd.to_numeric(df_r[col_den], errors="coerce")
                    df_r[col_out] = (num / den * 100).replace([np.inf, -np.inf], np.nan)

            # ── Scatter ROA vs ROE ────────────────────────────
            df_rr = df_r[["sigle","roa","roe","groupe_bancaire","bilan"]].copy()
            df_rr["roa"] = pd.to_numeric(df_rr["roa"], errors="coerce")
            df_rr["roe"] = pd.to_numeric(df_rr["roe"], errors="coerce")
            df_rr = df_rr.dropna(subset=["roa","roe"])

            fig_roa_roe = px.scatter(
                df_rr, x="roa", y="roe", text="sigle",
                color="groupe_bancaire", size="bilan", size_max=40,
                color_discrete_sequence=PALETTE_BANQUES,
                labels={"roa":"ROA (%)","roe":"ROE (%)","groupe_bancaire":"Groupe"},
            )
            fig_roa_roe.update_traces(textposition="top center",
                                      textfont=dict(color=CREAM, size=9))
            apply_plotly_defaults(fig_roa_roe, f"💡 ROA vs ROE ({last_y}) — taille = Bilan")

            # ── CIR — coefficient d'exploitation ─────────────
            df_cir = df_r[["sigle","cir"]].copy()
            df_cir["cir"] = pd.to_numeric(df_cir["cir"], errors="coerce")
            df_cir = df_cir.dropna().sort_values("cir")

            fig_cir = go.Figure(go.Bar(
                x=df_cir["sigle"], y=df_cir["cir"],
                marker_color=[SUCCESS if v < 60 else (GOLD if v < 80 else DANGER)
                              for v in df_cir["cir"]],
                text=df_cir["cir"].apply(lambda x: f"{x:.0f}%"),
                textposition="outside", textfont=dict(color=CREAM, size=9),
                hovertemplate="<b>%{x}</b><br>CIR: %{y:.1f}%<extra></extra>",
            ))
            fig_cir.add_hline(y=60, line_dash="dash", line_color=SUCCESS,
                              annotation_text="Seuil optimal 60%",
                              annotation_font_color=SUCCESS)
            fig_cir.add_hline(y=80, line_dash="dash", line_color=DANGER,
                              annotation_text="Seuil critique 80%",
                              annotation_font_color=DANGER)
            apply_plotly_defaults(fig_cir, f"⚙️ Coefficient d'Exploitation (CIR) — {last_y}")

            # ── KPIs DÉTAILLÉS ACTIF ──────────────────────────
            # Colonnes disponibles : effets publics, obligations, actions, caisse
            # Source : extraction bilans détaillés PDF BCEAO
            actif_dispo = {k: v for k, v in ACTIF_COLS.items() if k in df_r.columns}
            graphes_actif = []

            if actif_dispo:
                # Graphe 1 : Décomposition de l'actif par banque (stacked bar)
                df_actif = df_r[["sigle"] + list(actif_dispo.keys())].copy()
                for col in actif_dispo:
                    df_actif[col] = pd.to_numeric(df_actif[col], errors="coerce").fillna(0)

                fig_actif = go.Figure()
                colors_actif = [GOLD, "#5BC8F5", SUCCESS, "#B87BFF"]
                for (col, label), color in zip(actif_dispo.items(), colors_actif):
                    fig_actif.add_trace(go.Bar(
                        name=label,
                        x=df_actif["sigle"],
                        y=df_actif[col] / 1000,
                        marker_color=color,
                        hovertemplate=f"<b>%{{x}}</b><br>{label}: %{{y:,.1f}} Mds FCFA<extra></extra>",
                    ))
                fig_actif.update_layout(barmode="stack", xaxis_tickangle=-45)
                apply_plotly_defaults(fig_actif,
                    f"🏛 Décomposition de l'Actif par Banque — {last_y} (Mds FCFA)")
                graphes_actif.append(
                    dbc.Col(dcc.Graph(figure=fig_actif,
                                     config={"displayModeBar":False}), md=12)
                )

                # Graphe 2 : Structure actif moyenne sectorielle (donut)
                actif_totaux = {
                    label: df_actif[col].sum()
                    for col, label in actif_dispo.items()
                    if df_actif[col].sum() > 0
                }
                if actif_totaux:
                    fig_donut = go.Figure(go.Pie(
                        labels=list(actif_totaux.keys()),
                        values=list(actif_totaux.values()),
                        hole=0.6,
                        marker=dict(colors=colors_actif[:len(actif_totaux)],
                                    line=dict(color=NAVY2, width=2)),
                        textinfo="label+percent",
                        hovertemplate="<b>%{label}</b><br>%{value:,.0f} M FCFA<br>%{percent}<extra></extra>",
                    ))
                    fig_donut.update_layout(
                        title=dict(text=f"Structure Actif Sectorielle — {last_y}",
                                   font=dict(family="Syne", color=CREAM, size=14)),
                        annotations=[dict(text="Actif", x=0.5, y=0.5,
                                          font_size=14, showarrow=False, font_color=CREAM)],
                        **{k:v for k,v in PLOTLY_LAYOUT.items()
                           if k not in ("xaxis","yaxis")},
                    )
                    graphes_actif.append(
                        dbc.Col(dcc.Graph(figure=fig_donut,
                                         config={"displayModeBar":False}), md=6)
                    )

            return html.Div([
                dbc.Row([
                    dbc.Col(dcc.Graph(figure=fig_roa_roe, config={"displayModeBar":False}), md=6),
                    dbc.Col(dcc.Graph(figure=fig_cir,     config={"displayModeBar":False}), md=6),
                ], className="g-3 mb-3"),
                # Section KPIs détaillés actif (si données disponibles)
                html.Div([
                    html.H4("🏛 Décomposition Détaillée de l'Actif",
                            style={"color": GOLD, "fontFamily": "Syne",
                                   "marginTop": "20px", "marginBottom": "10px"}),
                    html.P("Source : bilans individuels extraits des PDF BCEAO",
                           style={"color": MUTED, "fontSize": "12px"}),
                    dbc.Row(graphes_actif, className="g-3"),
                ]) if graphes_actif else html.Div([
                    html.P("⚠️ Décomposition détaillée non disponible — données actif partielles",
                           style={"color": MUTED, "fontStyle": "italic", "marginTop": "20px"}),
                ]),
            ])

        # ── ONGLET 5 : CARTE SÉNÉGAL ──────────────────────────
        elif tab == "tab-carte":
            # Coordonnées GPS des sièges sociaux des banques à Dakar
            coords = {
                "CBAO"      : (14.6937, -17.4441, "Rue du Docteur Thèze, Dakar"),
                "SGBS"      : (14.6942, -17.4382, "19 Av. Léopold Sédar Senghor"),
                "ECOBANK"   : (14.7167, -17.4677, "Km 5 Bd du Centenaire"),
                "BOA"       : (14.6928, -17.4431, "Place de l'Indépendance"),
                "ORABANK"   : (14.6894, -17.4462, "Avenue Peytavin"),
                "BICIS"     : (14.6939, -17.4385, "2 Av. du Président Lamine Guèye"),
                "BHS"       : (14.7133, -17.4676, "Sacré-Cœur III"),
                "BIS"       : (14.7291, -17.4627, "Route de la Corniche Ouest"),
                "BAS"       : (14.6921, -17.4418, "Avenue William Ponty"),
                "NSIA Banque":(14.6931,-17.4447,  "Immeuble Fahd"),
                "LBA"       : (14.7022, -17.4489, "Route de Rufisque"),
                "CBI"       : (14.6910, -17.4430, "Place de l'Indépendance"),
                "BRM"       : (14.7054, -17.4506, "Liberté VI"),
                "BNDE"      : (14.6944, -17.4419, "6 Av. Carde"),
                "UBA"       : (14.6916, -17.4452, "Av. Léopold Sédar Senghor"),
                "FBNBANK"   : (14.6925, -17.4437, "Immeuble SDIH"),
                "CISA"      : (14.6978, -17.4510, "Liberté II"),
                "BDK"       : (14.7201, -17.4621, "Mermoz"),
                "BGFI"      : (14.6953, -17.4445, "Avenue Roume"),
                "BSIC"      : (14.6888, -17.4422, "Médina"),
                "CITIBANK"  : (14.6946, -17.4378, "Plateau"),
                "LBO"       : (14.7012, -17.4498, "Ouakam"),
                "BCIM"      : (14.6907, -17.4461, "Av. Georges Pompidou"),
                "CDS"       : (14.6933, -17.4439, "Allées Robert Delmas"),
            }

            last_y  = dff["annee"].max()
            df_map  = dff[dff["annee"] == last_y][["sigle","bilan","groupe_bancaire"]].copy()
            df_map["bilan"] = pd.to_numeric(df_map["bilan"], errors="coerce").fillna(0)

            map_data = []
            for _, row in df_map.iterrows():
                sig = row["sigle"]
                if sig in coords:
                    lat, lon, adresse = coords[sig]
                    map_data.append({
                        "sigle": sig, "lat": lat, "lon": lon,
                        "bilan": row["bilan"],
                        "groupe": row["groupe_bancaire"],
                        "adresse": adresse,
                    })

            df_map2 = pd.DataFrame(map_data)
            if not df_map2.empty:
                df_map2["taille"] = (df_map2["bilan"] / df_map2["bilan"].max() * 40 + 5).clip(5, 50)
                fig_carte = px.scatter_mapbox(
                    df_map2, lat="lat", lon="lon",
                    size="taille", size_max=50, color="groupe",
                    hover_name="sigle",
                    hover_data={"bilan":":.0f","adresse":True,"lat":False,"lon":False,"taille":False},
                    color_discrete_sequence=PALETTE_BANQUES,
                    zoom=11.5, center=dict(lat=14.6937, lon=-17.4441),
                    mapbox_style="carto-darkmatter",
                )
                fig_carte.update_layout(
                    height=580, paper_bgcolor="rgba(0,0,0,0)",
                    margin=dict(l=0,r=0,t=40,b=0),
                    title=dict(text=f"🗺 Localisation des Banques à Dakar — Bilan {last_y}",
                               font=dict(family="Syne",color=CREAM,size=14)),
                    legend=dict(bgcolor="rgba(10,22,40,0.8)", bordercolor=GOLD,
                                borderwidth=1, font=dict(color=CREAM)),
                )
            else:
                fig_carte = go.Figure()
                apply_plotly_defaults(fig_carte, "Données cartographiques indisponibles")

            return html.Div([dcc.Graph(figure=fig_carte, config={"displayModeBar":False})])

        # ── ONGLET 6 : CLASSEMENT ─────────────────────────────
        elif tab == "tab-classement":
            last_y   = dff["annee"].max()
            df_rank  = dff[dff["annee"] == last_y].copy()

            for col in ["bilan","resultat_net","fonds_propres","pnb","ressources","emploi"]:
                df_rank[col] = pd.to_numeric(df_rank.get(col, pd.Series()), errors="coerce")

            df_rank["roa_calc"]    = df_rank["resultat_net"] / df_rank["bilan"] * 100
            df_rank["roe_calc"]    = df_rank["resultat_net"] / df_rank["fonds_propres"] * 100
            df_rank["part_marche"] = df_rank["bilan"] / df_rank["bilan"].sum() * 100
            df_rank = df_rank.sort_values("bilan", ascending=False).reset_index(drop=True)
            df_rank.index += 1

            cols_show = {
                "sigle"          : "Banque",
                "groupe_bancaire": "Groupe",
                "bilan"          : "Bilan (M)",
                "resultat_net"   : "Résultat Net (M)",
                "fonds_propres"  : "Fonds Propres (M)",
                "part_marche"    : "Part Marché (%)",
                "roa_calc"       : "ROA (%)",
                "roe_calc"       : "ROE (%)",
            }

            table_rows = []
            for rank, (_, row) in enumerate(df_rank[list(cols_show.keys())].iterrows(), 1):
                medal = "🥇" if rank==1 else ("🥈" if rank==2 else ("🥉" if rank==3 else f"#{rank}"))
                cells = [html.Td(medal, className="rank-medal")]
                for col, label in cols_show.items():
                    val = row.get(col, "N/D")
                    if col in ["part_marche","roa_calc","roe_calc"]:
                        display = f"{val:.2f}%" if pd.notna(val) else "N/D"
                    elif col in ["bilan","resultat_net","fonds_propres"]:
                        display = f"{val:,.0f}" if pd.notna(val) else "N/D"
                        if col == "resultat_net" and pd.notna(val):
                            color = SUCCESS if val >= 0 else DANGER
                            cells.append(html.Td(display, style={"color": color}))
                            continue
                    else:
                        display = str(val)
                    cells.append(html.Td(display))
                table_rows.append(html.Tr(cells))

            table = html.Table([
                html.Thead(html.Tr(
                    [html.Th("#")] + [html.Th(v) for v in cols_show.values()]
                )),
                html.Tbody(table_rows),
            ], className="classement-table")

            return html.Div([
                html.Div([
                    html.H3(f"🏆 Classement des Banques — {last_y}", className="classement-title"),
                    html.P(f"Trié par Bilan (Total Actif) — {df_rank['sigle'].nunique()} banques",
                           style={"color": MUTED, "fontSize": "13px"}),
                ], className="mb-3"),
                html.Div(table, className="table-wrapper"),
            ])

        # ── ONGLET 7 : PRÉVISIONS ML ──────────────────────────
        elif tab == "tab-ml":
            return render_tab_ml(dff)

        return html.Div("Onglet non trouvé", style={"color": CREAM})

    # ══════════════════════════════════════════════════════════
    # FONCTION INTERNE : Rendu onglet ML
    # ══════════════════════════════════════════════════════════
    def render_tab_ml(dff):
        """
        Génère le contenu de l'onglet Prévisions ML.
        Affiche : prédictions 2024-2025 + scoring risque + classement futur.
        """
        try:
            from ml_predictions import get_ml_summary, compute_risk_score
            summary    = get_ml_summary(df_global)
            df_pred    = summary["predictions"]
            df_scores  = summary["risk_scores"]
            annee_base = summary["annee_base"]
        except Exception as e:
            return html.Div([
                html.P(f"⚠️ Module ML non disponible : {e}",
                       style={"color": DANGER, "padding": "20px"}),
                html.P("Vérifiez que ml_predictions.py est dans le répertoire racine.",
                       style={"color": MUTED}),
            ])

        # ── Graphe 1 : Prévisions bilan 2024-2025 ────────────
        fig_pred = go.Figure()

        # Données historiques — toutes les banques top 10 par bilan
        top10 = (df_global.groupby("sigle")["bilan"].mean()
                 .nlargest(10).index.tolist())

        for i, sigle in enumerate(top10):
            df_b = df_global[df_global["sigle"] == sigle].sort_values("annee")
            color = PALETTE_BANQUES[i % len(PALETTE_BANQUES)]

            # Historique
            fig_pred.add_trace(go.Scatter(
                x=df_b["annee"].tolist(),
                y=(pd.to_numeric(df_b["bilan"], errors="coerce") / 1000).tolist(),
                name=sigle,
                line=dict(color=color, width=2),
                mode="lines+markers",
                legendgroup=sigle,
                hovertemplate=f"<b>{sigle}</b> %{{x}}: %{{y:,.1f}} Mds<extra></extra>",
            ))

            # Prédictions 2024-2025 — ligne pointillée
            df_p_b = df_pred[(df_pred["sigle"]==sigle) & (df_pred["indicateur"]=="bilan")]
            if not df_p_b.empty:
                # Pont entre historique et prédiction
                x_pred = [annee_base] + df_p_b["annee"].tolist()
                y_hist_last = (pd.to_numeric(
                    df_b[df_b["annee"]==annee_base]["bilan"], errors="coerce"
                ).values)
                y_last = float(y_hist_last[0]) / 1000 if len(y_hist_last) > 0 else 0
                y_pred = [y_last] + (df_p_b["valeur_pred"] / 1000).tolist()

                fig_pred.add_trace(go.Scatter(
                    x=x_pred, y=y_pred,
                    name=f"{sigle} (préd.)",
                    line=dict(color=color, width=2, dash="dot"),
                    mode="lines+markers",
                    marker=dict(symbol="diamond", size=8),
                    legendgroup=sigle,
                    showlegend=False,
                    hovertemplate=f"<b>{sigle} PRÉV.</b> %{{x}}: %{{y:,.1f}} Mds<extra></extra>",
                ))

        # Zone grisée pour indiquer la période de prédiction
        fig_pred.add_vrect(
            x0=annee_base + 0.5, x1=2025.5,
            fillcolor="rgba(212,168,67,0.05)",
            layer="below", line_width=0,
            annotation_text="Zone prédictive",
            annotation_position="top left",
            annotation_font_color=MUTED,
        )
        apply_plotly_defaults(fig_pred,
            f"🤖 Prévisions Bilan Top 10 Banques — {annee_base+1}–2025 (Mds FCFA)")

        # ── Graphe 2 : Scoring risque ─────────────────────────
        df_scores = df_scores.sort_values("score_risque", ascending=True)
        fig_risk  = go.Figure(go.Bar(
            x=df_scores["score_risque"],
            y=df_scores["sigle"],
            orientation="h",
            marker_color=[
                SUCCESS if s >= 60 else (GOLD if s >= 30 else DANGER)
                for s in df_scores["score_risque"]
            ],
            text=df_scores["categorie"],
            textposition="outside",
            textfont=dict(color=CREAM, size=10),
            hovertemplate="<b>%{y}</b><br>Score: %{x:.0f}/100<br>%{text}<extra></extra>",
        ))
        fig_risk.add_vline(x=30, line_dash="dash", line_color=DANGER,
                           annotation_text="Seuil risque élevé",
                           annotation_font_color=DANGER)
        fig_risk.add_vline(x=60, line_dash="dash", line_color=SUCCESS,
                           annotation_text="Seuil risque faible",
                           annotation_font_color=SUCCESS)
        apply_plotly_defaults(fig_risk,
            f"🎯 Scoring Risque par Banque — {annee_base} (0=Risque max, 100=Risque min)")
        fig_risk.update_layout(height=520, xaxis=dict(range=[0,120]))

        # ── Tableau résumé prédictions ────────────────────────
        df_res  = df_pred[df_pred["indicateur"] == "bilan"].copy()
        df_res  = df_res.sort_values(["annee","valeur_pred"], ascending=[True,False])

        table_rows_ml = []
        for _, row in df_res.head(20).iterrows():
            conf_color = SUCCESS if row["confiance"] > 70 else (GOLD if row["confiance"] > 40 else DANGER)
            table_rows_ml.append(html.Tr([
                html.Td(row["sigle"],           style={"color": GOLD,       "fontWeight":"bold"}),
                html.Td(str(int(row["annee"])), style={"color": CREAM}),
                html.Td(f"{row['valeur_pred']/1000:.1f} Mds FCFA", style={"color": CREAM}),
                html.Td(row["tendance"],        style={"color": CREAM}),
                html.Td(f"{row['confiance']:.0f}%", style={"color": conf_color}),
            ]))

        table_pred = html.Table([
            html.Thead(html.Tr([
                html.Th("Banque"), html.Th("Année"), html.Th("Bilan Prévu"),
                html.Th("Tendance"), html.Th("Confiance"),
            ])),
            html.Tbody(table_rows_ml),
        ], className="classement-table")

        return html.Div([
            # Note méthodologique
            html.Div([
                html.P([
                    "📌 ",
                    html.Strong("Méthode : "),
                    "Régression linéaire pondérée sur données historiques 2015–2023. "
                    "Les années récentes ont plus de poids. "
                    "La confiance indique le R² du modèle (>70% = fiable)."
                ], style={"color": MUTED, "fontSize": "12px",
                          "background": "#0C1A12", "padding": "10px",
                          "borderRadius": "6px", "borderLeft": f"3px solid {GOLD}"}),
            ], className="mb-3"),

            # Graphes ML
            dbc.Row([
                dbc.Col(dcc.Graph(figure=fig_pred, config={"displayModeBar":False}), md=12),
            ], className="g-3 mb-3"),
            dbc.Row([
                dbc.Col(dcc.Graph(figure=fig_risk, config={"displayModeBar":False}), md=8),
                dbc.Col([
                    html.H4("📋 Prévisions Bilan (Top 20)",
                            style={"color": GOLD, "fontFamily": "Syne", "fontSize": "14px"}),
                    html.Div(table_pred, className="table-wrapper",
                             style={"maxHeight": "460px", "overflowY": "auto"}),
                ], md=4),
            ], className="g-3"),
        ])

    # ══════════════════════════════════════════════════════════
    # CB-03 : KPIs banque individuelle
    # ══════════════════════════════════════════════════════════
    @app.callback(
        Output("profil-banque-kpi",   "children"),
        Input("select-banque-profil", "value"),
        Input("select-annee-profil",  "value"),
    )
    def update_profil_kpi(sigle, annee):
        """Affiche les 6 KPIs clés de la banque sélectionnée pour l'année choisie."""
        if not sigle or not annee:
            return ""
        df_b = df_global[(df_global["sigle"]==sigle) & (df_global["annee"]==annee)]
        if df_b.empty:
            return html.P(f"Aucune donnée pour {sigle} en {annee}", style={"color":MUTED})

        row        = df_b.iloc[0]
        bilan      = pd.to_numeric(row.get("bilan"),       errors="coerce")
        rn         = pd.to_numeric(row.get("resultat_net"),errors="coerce")
        fp         = pd.to_numeric(row.get("fonds_propres"),errors="coerce")
        effectif   = pd.to_numeric(row.get("effectif"),    errors="coerce")
        agences    = pd.to_numeric(row.get("agences"),     errors="coerce")

        roa = (rn / bilan * 100) if (pd.notna(bilan) and bilan!=0 and pd.notna(rn)) else None
        roe = (rn / fp * 100)    if (pd.notna(fp) and fp!=0 and pd.notna(rn))    else None

        def fmt(v, pct=False, k=False):
            if v is None or (isinstance(v, float) and math.isnan(v)):
                return "N/D"
            if pct: return f"{v:.2f}%"
            if k:   return f"{int(v):,}".replace(",","·")
            return f"{v/1000:.1f} Mds"

        return dbc.Row([
            dbc.Col(html.Div([
                html.P("Total Actif (Bilan)", className="kpi-label"),
                html.H4(fmt(bilan), className="kpi-value", style={"color":GOLD}),
            ], className="kpi-card mini"), xs=6, md=2),
            dbc.Col(html.Div([
                html.P("Résultat Net", className="kpi-label"),
                html.H4(fmt(rn), className="kpi-value",
                        style={"color": SUCCESS if (pd.notna(rn) and rn>=0) else DANGER}),
            ], className="kpi-card mini"), xs=6, md=2),
            dbc.Col(html.Div([
                html.P("Fonds Propres", className="kpi-label"),
                html.H4(fmt(fp), className="kpi-value", style={"color":"#B87BFF"}),
            ], className="kpi-card mini"), xs=6, md=2),
            dbc.Col(html.Div([
                html.P("ROA", className="kpi-label"),
                html.H4(fmt(roa, pct=True), className="kpi-value", style={"color":"#5BC8F5"}),
            ], className="kpi-card mini"), xs=6, md=2),
            dbc.Col(html.Div([
                html.P("ROE", className="kpi-label"),
                html.H4(fmt(roe, pct=True), className="kpi-value", style={"color":GOLD2}),
            ], className="kpi-card mini"), xs=6, md=2),
            dbc.Col(html.Div([
                html.P("Effectif / Agences", className="kpi-label"),
                html.H4(f"{fmt(effectif,k=True)} / {fmt(agences,k=True)}",
                        className="kpi-value", style={"color":SUCCESS}),
            ], className="kpi-card mini"), xs=6, md=2),
        ], className="g-2 mb-3")

    # ══════════════════════════════════════════════════════════
    # CB-04 : Graphique évolution banque individuelle
    # ══════════════════════════════════════════════════════════
    @app.callback(
        Output("graph-profil-evolution", "figure"),
        Input("select-banque-profil",    "value"),
    )
    def graph_profil_evolution(sigle):
        """
        Courbes d'évolution temporelle pour une banque.
        Axe principal : bilan, ressources, emplois, fonds propres.
        Axe secondaire : résultat net.
        """
        if not sigle:
            return go.Figure()
        df_b = df_global[df_global["sigle"]==sigle].sort_values("annee")

        fig = make_subplots(specs=[[{"secondary_y": True}]])
        for col, name, color, sec in [
            ("bilan",       "Bilan",         GOLD,      False),
            ("ressources",  "Ressources",    "#5BC8F5", False),
            ("emploi",      "Emplois",       SUCCESS,   False),
            ("fonds_propres","Fonds Propres","#B87BFF", False),
            ("resultat_net","Résultat Net",  DANGER,    True),
        ]:
            if col in df_b.columns:
                vals = pd.to_numeric(df_b[col], errors="coerce")
                fig.add_trace(go.Scatter(
                    x=df_b["annee"], y=vals/1000,
                    name=name, line=dict(color=color, width=2),
                    mode="lines+markers",
                    hovertemplate=f"<b>{name}</b><br>%{{x}}: %{{y:,.1f}} Mds<extra></extra>",
                ), secondary_y=sec)

        apply_plotly_defaults(fig, f"📈 Évolution {sigle} — 2015–2023 (Mds FCFA)")
        return fig

    # ══════════════════════════════════════════════════════════
    # CB-05 : Radar chart banque individuelle
    # ══════════════════════════════════════════════════════════
    @app.callback(
        Output("graph-profil-radar",  "figure"),
        Input("select-banque-profil", "value"),
        Input("select-annee-profil",  "value"),
    )
    def graph_profil_radar(sigle, annee):
        """
        Radar chart normalisé 0-100 pour comparer la banque au marché.
        Dimensions : bilan, ressources, emploi, fonds propres, effectif.
        """
        if not sigle or not annee:
            return go.Figure()

        df_b   = df_global[(df_global["sigle"]==sigle) & (df_global["annee"]==annee)]
        df_all = df_global[df_global["annee"]==annee]
        if df_b.empty:
            return go.Figure()

        dims   = [d for d in ["bilan","ressources","emploi","fonds_propres","effectif"]
                  if d in df_global.columns]

        def normalize(val, col):
            mx = pd.to_numeric(df_all[col], errors="coerce").max()
            return float(pd.to_numeric(val, errors="coerce") or 0) / mx * 100 if mx else 0

        row    = df_b.iloc[0]
        values = [normalize(row.get(d), d) for d in dims]
        labels = [COL_LABELS.get(d, d).replace(" (M FCFA)","") for d in dims]

        fig = go.Figure(go.Scatterpolar(
            r=values + [values[0]],
            theta=labels + [labels[0]],
            fill="toself",
            fillcolor="rgba(212,168,67,0.2)",
            line=dict(color=GOLD, width=2),
            name=sigle,
        ))
        fig.update_layout(
            polar=dict(
                bgcolor="rgba(15,32,64,0.6)",
                radialaxis=dict(visible=True, range=[0,100],
                                gridcolor="rgba(255,255,255,0.1)",
                                tickfont=dict(color=MUTED, size=8)),
                angularaxis=dict(gridcolor="rgba(255,255,255,0.1)",
                                 tickfont=dict(color=CREAM, size=10)),
            ),
            showlegend=False,
            title=dict(text=f"Profil {sigle} ({annee})",
                       font=dict(family="Syne",color=CREAM,size=13)),
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=40,r=40,t=50,b=40),
        )
        return fig

    # ══════════════════════════════════════════════════════════
    # CB-06 : Téléchargement Excel
    # ══════════════════════════════════════════════════════════
    @app.callback(
        Output("download-excel",   "data"),
        Input("btn-download-excel","n_clicks"),
        State("store-filtered",    "data"),
        prevent_initial_call=True,
    )
    def download_excel(n, store_data):
        """Export Excel : feuille données brutes + feuille résumé par banque."""
        if not n or not store_data:
            return no_update
        dff = pd.read_json(io.StringIO(store_data), orient="split")

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            dff.to_excel(writer, sheet_name="Données Brutes", index=False)
            summary = dff.groupby("sigle").agg(
                bilan_moy=("bilan","mean"),
                resultat_net_moy=("resultat_net","mean"),
                fonds_propres_moy=("fonds_propres","mean"),
            ).round(0)
            summary.to_excel(writer, sheet_name="Résumé par Banque")
        buf.seek(0)
        return dcc.send_bytes(buf.read(), "banques_senegal_export.xlsx")

    # ══════════════════════════════════════════════════════════
    # CB-07 : Téléchargement HTML
    # ══════════════════════════════════════════════════════════
    @app.callback(
        Output("download-html",   "data"),
        Input("btn-download-html","n_clicks"),
        State("store-filtered",   "data"),
        prevent_initial_call=True,
    )
    def download_html(n, store_data):
        """Export HTML standalone avec tableau de classement et KPIs."""
        if not n or not store_data:
            return no_update
        dff    = pd.read_json(io.StringIO(store_data), orient="split")
        last_y = int(dff["annee"].max()) if not dff.empty else "N/D"
        total_bilan = (dff[dff["annee"]==last_y]["bilan"].sum() / 1_000_000
                       if not dff.empty else 0)

        html_content = f"""<!DOCTYPE html>
<html lang="fr">
<head><meta charset="UTF-8">
<title>Rapport Banques Sénégal {last_y}</title>
<style>
  body{{font-family:'Urbanist',sans-serif;background:#060C0F;color:#F5F2EC;margin:0;padding:20px}}
  h1{{color:#FF6B5B;font-size:2em;border-bottom:2px solid #FF6B5B;padding-bottom:10px}}
  h2{{color:#FFB347;margin-top:30px}}
  table{{width:100%;border-collapse:collapse;margin:15px 0}}
  th{{background:#0A1510;color:#FF6B5B;padding:10px;text-align:left;border-bottom:2px solid #FF6B5B}}
  td{{padding:8px;border-bottom:1px solid rgba(255,107,91,0.1);color:#F5F2EC}}
  tr:hover{{background:#0C1A12}}
  .kpi-box{{display:inline-block;background:#0C1A12;border:1px solid rgba(255,107,91,0.2);
            border-radius:8px;padding:15px 25px;margin:10px;text-align:center}}
  .kpi-val{{font-size:1.8em;font-weight:bold;color:#FFB347}}
  .kpi-lbl{{font-size:.85em;color:#7A9A88;margin-top:5px}}
  footer{{margin-top:40px;color:#7A9A88;font-size:.8em;
          border-top:1px solid rgba(255,107,91,0.15);padding-top:10px}}
</style></head>
<body>
<h1>🏦 Rapport — Positionnement des Banques au Sénégal</h1>
<p>Source : BCEAO · Base Sénégal 2015–2023 | Exporté depuis le Dashboard</p>
<h2>📊 Indicateurs Globaux {last_y}</h2>
<div>
  <div class="kpi-box"><div class="kpi-val">{total_bilan:.2f} Mds</div>
    <div class="kpi-lbl">Total Actif Bancaire</div></div>
  <div class="kpi-box">
    <div class="kpi-val">{dff[dff['annee']==last_y]['sigle'].nunique()}</div>
    <div class="kpi-lbl">Banques Actives</div></div>
  <div class="kpi-box">
    <div class="kpi-val">{int(dff[dff['annee']==last_y]['effectif'].sum()):,}</div>
    <div class="kpi-lbl">Effectif Total</div></div>
</div>
<h2>🏆 Classement par Bilan — {last_y}</h2>
<table>
<thead><tr><th>#</th><th>Banque</th><th>Groupe</th><th>Bilan (M)</th>
<th>Ressources</th><th>Emplois</th><th>Fonds Propres</th></tr></thead>
<tbody>"""
        df_r2 = (dff[dff["annee"]==last_y]
                 .sort_values("bilan",ascending=False)
                 .reset_index(drop=True))
        for i, row in df_r2.iterrows():
            html_content += f"<tr><td>{i+1}</td><td><b>{row.get('sigle','')}</b></td>"
            html_content += f"<td>{row.get('groupe_bancaire','')}</td>"
            for col in ["bilan","ressources","emploi","fonds_propres"]:
                val = pd.to_numeric(row.get(col), errors="coerce")
                html_content += f"<td>{val:,.0f}</td>" if pd.notna(val) else "<td>N/D</td>"
            html_content += "</tr>\n"

        html_content += """</tbody></table>
<footer>Dashboard Banques Sénégal · Python · Dash · MongoDB Atlas · Source BCEAO 2015–2023</footer>
</body></html>"""
        return dcc.send_string(html_content, "rapport_banques_senegal.html")

    # ══════════════════════════════════════════════════════════
    # CB-08 : Rapport PDF individuel (bouton section Analyse)
    # ══════════════════════════════════════════════════════════
    @app.callback(
        Output("download-rapport-individuel", "data"),
        Input("btn-generer-rapport",          "n_clicks"),
        State("select-banque-profil",         "value"),
        State("select-annee-profil",          "value"),
        prevent_initial_call=True,
    )
    def generer_rapport_individuel(n, sigle, annee):
        """
        Génère le rapport individuel d'une banque.
        Tente d'abord ReportLab (PDF), fallback HTML si indisponible.
        """
        if not n or not sigle:
            return no_update
        try:
            from utils.pdf_generator import generate_bank_pdf
            pdf_bytes = generate_bank_pdf(
                df_global, sigle, annee or int(df_global["annee"].max())
            )
            return dcc.send_bytes(pdf_bytes, f"rapport_{sigle}_{annee}.pdf")
        except Exception:
            # Fallback HTML propre
            df_b  = df_global[df_global["sigle"]==sigle].sort_values("annee")
            rows  = ""
            for _, row in df_b.iterrows():
                rn  = pd.to_numeric(row.get("resultat_net"), errors="coerce")
                col = "#2ECC71" if (pd.notna(rn) and rn>=0) else "#E74C3C"
                rows += f"<tr><td><b>{int(row['annee'])}</b></td>"
                for c in ["bilan","ressources","emploi","fonds_propres"]:
                    v = pd.to_numeric(row.get(c), errors="coerce")
                    rows += f"<td>{v:,.0f}</td>" if pd.notna(v) else "<td>N/D</td>"
                rn_d = f"{rn:,.0f}" if pd.notna(rn) else "N/D"
                rows += f"<td style='color:{col}'>{rn_d}</td></tr>\n"
            groupe = df_b.iloc[0].get("groupe_bancaire","N/D") if not df_b.empty else "N/D"
            html_f = f"""<!DOCTYPE html><html lang="fr">
<head><meta charset="UTF-8"><title>Rapport {sigle}</title>
<style>body{{font-family:'Urbanist',sans-serif;background:#060C0F;color:#F5F2EC;padding:30px}}
h1{{color:#FF6B5B}}table{{width:100%;border-collapse:collapse}}
th{{background:#0A1510;color:#FF6B5B;padding:10px;border-bottom:2px solid #FF6B5B}}
td{{padding:8px;border-bottom:1px solid rgba(255,107,91,0.1)}}
tr:hover{{background:#0C1A12}}</style></head>
<body><h1>Rapport {sigle} — {annee}</h1><p>Groupe : {groupe}</p>
<table><thead><tr><th>Année</th><th>Bilan</th><th>Ressources</th>
<th>Emplois</th><th>Fonds Propres</th><th>Résultat Net</th></tr></thead>
<tbody>{rows}</tbody></table>
<p style="color:#7A9A88;margin-top:20px">Source BCEAO 2015–2023</p>
</body></html>"""
            return dcc.send_string(html_f, f"rapport_{sigle}_{annee}.html")

    # ══════════════════════════════════════════════════════════
    # CB-09 : Bouton "Rapport PDF" dans le header
    # ══════════════════════════════════════════════════════════
    @app.callback(
        Output("download-pdf",        "data"),
        Input("btn-download-pdf",     "n_clicks"),
        State("select-banque-profil", "value"),
        State("select-annee-profil",  "value"),
        prevent_initial_call=True,
    )
    def download_rapport_pdf(n, sigle, annee):
        """Même logique que CB-08 — déclenché depuis le header."""
        if not n or not sigle:
            return no_update
        try:
            from utils.pdf_generator import generate_bank_pdf
            pdf_bytes = generate_bank_pdf(df_global, sigle,
                                          annee or df_global["annee"].max())
            return dcc.send_bytes(pdf_bytes, f"rapport_{sigle}_{annee}.pdf")
        except Exception:
            df_b  = df_global[df_global["sigle"]==sigle].sort_values("annee")
            rows  = ""
            for _, row in df_b.iterrows():
                rn  = pd.to_numeric(row.get("resultat_net"), errors="coerce")
                col = "#2ECC71" if (pd.notna(rn) and rn>=0) else "#E74C3C"
                rows += f"<tr><td><b>{int(row['annee'])}</b></td>"
                for c in ["bilan","ressources","emploi","fonds_propres"]:
                    v = pd.to_numeric(row.get(c), errors="coerce")
                    rows += f"<td>{v:,.0f}</td>" if pd.notna(v) else "<td>N/D</td>"
                rows += (f"<td style='color:{col}'>{rn:,.0f}</td></tr>\n"
                         if pd.notna(rn) else "<td>N/D</td></tr>\n")
            html_f = (f"<html><body style='font-family:Urbanist,sans-serif;background:#060C0F;"
                      f"color:#F5F2EC;padding:20px'>"
                      f"<h1 style='color:#FF6B5B'>Rapport {sigle}</h1>"
                      f"<table border='1' style='border-color:rgba(255,107,91,0.2);width:100%;border-collapse:collapse'><thead><tr>"
                      f"<th style='background:#0A1510;color:#FF6B5B;padding:8px'>Année</th>"
                      f"<th style='background:#0A1510;color:#FF6B5B;padding:8px'>Bilan</th>"
                      f"<th style='background:#0A1510;color:#FF6B5B;padding:8px'>Ressources</th>"
                      f"<th style='background:#0A1510;color:#FF6B5B;padding:8px'>Emplois</th>"
                      f"<th style='background:#0A1510;color:#FF6B5B;padding:8px'>Fonds Propres</th>"
                      f"<th style='background:#0A1510;color:#FF6B5B;padding:8px'>Résultat Net</th>"
                      f"</tr></thead><tbody>{rows}</tbody></table></body></html>")
            return dcc.send_string(html_f, f"rapport_{sigle}_{annee}.html")
