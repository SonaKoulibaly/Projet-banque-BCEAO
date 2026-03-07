---
# 🏦 Projet Banque Sénégal
> Automatisation de l'étude du positionnement des banques au Sénégal
## 🌐 Démo Live ICI👉 Voir le tableau de bord en ligne(https://projet-banque-bceao-1.onrender.com)
!
![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)
![Dash](https://img.shields.io/badge/Dash-2.17-blue?logo=plotly)
![MongoDB](https://img.shields.io/badge/MongoDB-Atlas-green?logo=mongodb)
![ML](https://img.shields.io/badge/ML-scikit--learn-orange?logo=scikit-learn)
![Tests](https://img.shields.io/badge/Tests-29%2F29%20passed-brightgreen)
![License](https://img.shields.io/badge/Licence-Académique-orange)

---

## 📋 Présentation

Ce projet a été développé dans le cadre du **Mastère 2 Big Data & Data Strategy**. Il automatise l'étude du positionnement des banques au Sénégal à travers un dashboard interactif construit avec **Dash + Plotly**, alimenté par **MongoDB Atlas** et deux sources de données complémentaires :

- **Base Sénégal 2** : données financières 2015–2020 (24 banques, 30 colonnes)
- **Rapports PDF BCEAO** : données extraites automatiquement pour 2021–2023
- **Scraping automatique** : détection et extraction des nouveaux rapports BCEAO

---

## 🎯 Objectifs

- Centraliser les données bancaires dans MongoDB Atlas
- Extraire et harmoniser les données des rapports PDF BCEAO
- Scraper automatiquement les nouveaux rapports disponibles sur bceao.int
- Visualiser les KPIs financiers clés par banque et par année
- Prédire le positionnement futur des banques via un module ML
- Permettre la comparaison interbancaire interactive
- Générer des rapports PDF téléchargeables par banque
- Cartographier les banques sur une carte interactive du Sénégal

---

## 🏗 Architecture du Projet

```
projet_banque_senegal/
│
├── app.py                          ← Point d'entrée Dash
├── layout.py                       ← Interface utilisateur (7 onglets)
├── callbacks.py                    ← Logique interactive (9 callbacks)
├── ml_predictions.py               ← Module ML prédictif (prévisions 2024-2025)
├── .env                            ← Variables d'environnement (non versionné)
├── .env.example                    ← Template variables d'environnement
├── requirements.txt                ← Dépendances Python
├── render.yaml                     ← Configuration déploiement Render
├── .gitignore
├── README.md
│
├── assets/
│   ├── logo.png                    ← Logo BCEAO
│   └── style.css                   ← Design "Luxe Financier Africain"
│
├── data/
│   ├── raw/
│   │   ├── BASE_SENEGAL2.xlsx      ← Source principale (2015–2020)
│   │   ├── BCEAO_2022.pdf          ← Rapport PDF BCEAO (369 pages)
│   │   └── BCEAO_2023.pdf          ← Rapport PDF BCEAO 2023 (365 pages)
│   └── processed/
│       ├── banques_raw.csv
│       ├── banques_pdf_raw.csv
│       ├── banques_pdf_raw_2023.csv
│       ├── banques_normalized.csv
│       └── banques_production_FINALE.csv  ← Base finale propre (215 docs, 2015–2023)
│
├── etl/
│   ├── 01_load_excel.py            ← Chargement Excel → MongoDB
│   ├── 02_extract_pdf.py           ← Extraction PDF BCEAO (pages 267–319)
│   ├── 03_normalize.py             ← Harmonisation & fusion des sources
│   ├── 04_clean.py                 ← Nettoyage & calcul des ratios
│   ├── 05_scrape_bceao.py          ← Scraping automatique site BCEAO
│   └── run_etl.py                  ← Exécution pipeline complet
│
├── utils/
│   ├── __init__.py
│   └── pdf_generator.py            ← Génération rapports PDF (ReportLab)
│
├── tests/
│   └── test_etl.py                 ← 29 tests unitaires
│
└── docs/
    └── GUIDE_UTILISATION.md        ← Guide utilisateur complet
```

---

## 📊 Sources de Données

### Base Sénégal 2 (`BASE_SENEGAL2.xlsx`)
| Élément | Détail |
|---|---|
| **Période** | 2015 – 2020 |
| **Banques** | 24 banques sénégalaises |
| **Lignes** | 134 enregistrements |
| **Colonnes** | 30 variables financières |
| **Variables clés** | Bilan, Emploi, Ressources, Fonds Propres, PNB, Résultat Net, Effectif, Agences |

### Rapports PDF BCEAO
| Élément | Détail |
|---|---|
| **Source** | Rapport annuel BCEAO — Bilans et comptes de résultat |
| **PDF 2022** | Pages 267–319 (section Sénégal) — 81 records |
| **PDF 2023** | Pages 256–315 (section Sénégal) — 27 banques · extraction corrigée mars 2026 |
| **Outil** | `pdfplumber` (extraction tableaux natifs PDF) |

> **Pourquoi pdfplumber et non OCR ?** Les PDFs BCEAO sont des documents natifs non scannés. `pdfplumber` extrait directement les tableaux sans nécessiter d'OCR. L'OCR serait utile uniquement pour des PDFs scannés (images).

> **Correction extraction 2023 (mars 2026)** : L'extraction initiale produisait 1304 documents corrompus (sigle=NaN, colonnes financières NULL). La version corrigée utilise une extraction par coordonnées X/Y avec normalisation Unicode (gestion de `COÛT`, `RÉSULTAT` avec accents) et détection des valeurs sur lignes intermédiaires. Résultat : 27 documents propres, 0 NaN sur colonnes financières. Script de mise à jour : `data/processed/maj_mongodb_2023_FINAL.py`.

### Scraping Automatique BCEAO (`05_scrape_bceao.py`)
| Élément | Détail |
|---|---|
| **Source** | bceao.int — Publications officielles |
| **Détection** | Vérification automatique des nouvelles années disponibles |
| **Anti-doublons** | Vérification MongoDB avant insertion |
| **Pages Sénégal** | Détectées automatiquement + fallback valeurs connues |

---

## 🔄 Pipeline ETL

```
BASE_SENEGAL2.xlsx  ──► 01_load_excel.py  ──► banques_raw        (134 docs)
BCEAO_2022.pdf      ──► 02_extract_pdf.py ──► banques_pdf_raw    ( 81 docs)
BCEAO_2023.pdf      ──► 05_scrape_bceao.py──► banques_pdf_raw    ( 27 docs propres)
                         03_normalize.py  ──► banques_normalized (215 docs)
                         04_clean.py      ──► banques_production (215 docs)
```

### Ratios calculés automatiquement

| Ratio | Formule | Description |
|---|---|---|
| **ROA** | Résultat Net / Bilan × 100 | Rentabilité des actifs |
| **ROE** | Résultat Net / Fonds Propres × 100 | Rentabilité des capitaux |
| **CIR** | Charges Générales / PNB × 100 | Coefficient d'exploitation |
| **NIM** | PNB / Bilan × 100 | Marge nette d'intérêt |
| **LDR** | Emplois / Ressources × 100 | Ratio crédits/dépôts |
| **Solvabilité** | Fonds Propres / Bilan × 100 | Solidité financière |

---

## 📈 Dashboard — Fonctionnalités

### KPIs Globaux (bandeau supérieur)
| KPI | Source | Description |
|---|---|---|
| **Total Actif Bancaire** | `bilan` | Somme des bilans secteur entier |
| **Banques Actives** | `sigle` | Nombre de banques sur la période |
| **Effectif Total** | `effectif` | Total employés bancaires |
| **Fonds Propres Moy.** | `fonds_propres` | Moyenne par banque |

### Les 7 Onglets

| Onglet | Contenu |
|---|---|
| **🏛 Vue Marché** | Parts de marché · Évolution sectorielle 2015–2023 · Répartition groupes |
| **⚖️ Comparaison** | Scatter Bilan vs Résultat Net · Barres groupées inter-banques |
| **📈 Performance** | Heatmap performances · Croissance · Détection anomalies |
| **⚙️ Ratios Financiers** | ROA vs ROE · CIR avec seuils · Décomposition détaillée actif |
| **🗺 Carte Sénégal** | Carte Mapbox · Localisation banques · Taille = part de marché |
| **🏆 Classement** | Top banques · Médailles 🥇🥈🥉 · Couleurs résultat net |
| **🤖 Prévisions ML** | Prévisions 2024–2025 · Scoring risque · Classement futur |

### Filtres Interactifs
- **📅 Période** : RangeSlider 2015–2023
- **🏦 Banques** : Dropdown multi-sélection (27 banques)
- **🌍 Groupe** : Dropdown multi-sélection (groupes bancaires)
- **📊 Indicateur** : Bilan · Emplois · Ressources · Fonds Propres · PNB · Résultat Net · ROA · ROE · CIR
- **🔄 Réinitialiser** : Bouton reset tous filtres

### Module ML Prédictif (`ml_predictions.py`)
| Fonctionnalité | Détail |
|---|---|
| **Prévisions** | Bilan, Résultat Net, ROA, ROE, Fonds Propres, Emplois — 2024 & 2025 |
| **Méthode** | Régression linéaire pondérée (années récentes = plus de poids) |
| **Score confiance** | R² du modèle (>70% = fiable) |
| **Scoring risque** | Score 0–100 par banque basé sur ROA, ROE, CIR, Solvabilité |
| **Classement futur** | Évolution prédite du rang par indicateur |

### Analyse Individuelle
- Sélection banque + année (2015–2023)
- 6 KPIs individuels (Bilan, Résultat Net, Fonds Propres, ROA, ROE, Effectif)
- Graphique évolution temporelle
- Radar chart multi-indicateurs
- **📄 Génération rapport PDF individuel** (ReportLab)

### Téléchargements
| Format | Contenu |
|---|---|
| **⬇ Excel** | Données filtrées — 2 onglets (Données brutes + Résumé) |
| **⬇ HTML** | Tableau interactif navigateur |
| **📄 Rapport PDF** | Rapport complet banque sélectionnée |

---

## 🗄 Collections MongoDB Atlas

| Collection | Documents | Description |
|---|---|---|
| `banques_raw` | 134 | Données brutes Excel 2015–2020 |
| `banques_pdf_raw` | 108 | Données brutes PDF BCEAO 2022–2023 |
| `banques_normalized` | 215 | Données fusionnées harmonisées |
| `banques_production` | **215** | Base finale — alimente le dashboard (nettoyée mars 2026) |

---

## 🚀 Installation et Lancement

### Prérequis
- Python 3.12+
- Compte MongoDB Atlas
- Git

### Étapes

**1. Cloner le repository**
```bash
git clone https://github.com/SonaKoulibaly/projet-banque-senegal.git
cd projet-banque-senegal
```

**2. Créer l'environnement virtuel**
```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# Mac/Linux
source .venv/bin/activate
```

**3. Installer les dépendances**
```bash
pip install -r requirements.txt
```

**4. Configurer les variables d'environnement**
```bash
cp .env.example .env
```

Éditer `.env` :
```env
MONGO_URI=mongodb+srv://username:password@cluster0.xxxxx.mongodb.net/?retryWrites=true&w=majority
DB_NAME=banque_senegal
DEBUG=True
```

**5. Exécuter le pipeline ETL**
```bash
# Pipeline complet
python etl/run_etl.py

# Avec scraping BCEAO (nouveaux rapports)
python etl/run_etl.py --avec-scraping

# Vérifier nouvelles années disponibles
python etl/05_scrape_bceao.py check
```

**6. Tester le module ML**
```bash
python ml_predictions.py
```

**7. Lancer le dashboard**
```bash
python app.py
```

**8. Ouvrir dans le navigateur**
```
http://localhost:7654
```

---

## 🧪 Tests Unitaires

```bash
pytest tests/ -v
```

**Résultat attendu : 29/29 tests passés ✅**

| Classe | Tests | Description |
|---|---|---|
| `TestLoadExcel` | 9 | Fichier, lignes, colonnes, banques, années |
| `TestProduction` | 6 | CSV production, années, doublons, croissance |
| `TestRatios` | 4 | ROA, ROE, solvabilité, CIR dans plages réalistes |
| `TestMetier` | 5 | CBAO leader, SGBS présence, 24 banques en 2020 |
| `TestUtils` | 5 | safe_val, logo, .env, MONGO_URI |

---

## ☁️ Déploiement sur Render

**1. Push GitHub**
```bash
git add .
git commit -m "Projet Banque Sénégal v3.0 — Données 2023 corrigées (215 docs)"
git push origin main
```

**2. Render.com**
- New → Web Service → connecter le repo
- Render détecte `render.yaml` automatiquement

**3. Variable d'environnement**
- Environment → ajouter `MONGO_URI`

---

## 🛠 Stack Technique

| Technologie | Usage | Version |
|---|---|---|
| Python | Langage principal | 3.12 |
| Dash | Dashboard interactif | 2.17+ |
| Plotly | Graphiques | 5.22+ |
| Flask | Serveur WSGI | 3.0+ |
| MongoDB Atlas | Base de données cloud | — |
| PyMongo | Connecteur MongoDB | 4.8+ |
| Pandas | Manipulation données | 2.2+ |
| scikit-learn | Module ML prédictif | 1.5+ |
| pdfplumber | Extraction PDF natifs | 0.11+ |
| requests | Scraping BCEAO | 2.32+ |
| ReportLab | Génération PDF | 4.2+ |
| dash-bootstrap-components | UI/UX | 1.6+ |
| Gunicorn | Serveur production | 22.0+ |
| pytest | Tests unitaires | 8.2+ |
| Render | Hébergement cloud | — |

---

## ✅ Statut des Fonctionnalités

| Fonctionnalité | Statut |
|---|---|
| Pipeline ETL complet (Excel + PDF) | ✅ Fait |
| MongoDB Atlas centralisé (215 docs) | ✅ Fait |
| Données 2015–2023 (27 banques) · extraction 2023 corrigée | ✅ Fait |
| Dashboard 7 onglets | ✅ Fait |
| KPIs globaux dynamiques | ✅ Fait |
| Filtres interactifs + Reset | ✅ Fait |
| Carte Sénégal Mapbox | ✅ Fait |
| Ratios financiers (ROA, ROE, CIR, NIM, LDR) | ✅ Fait |
| KPIs détaillés actif (effets publics, obligations) | ✅ Fait |
| Rapports PDF individuels (ReportLab) | ✅ Fait |
| Export Excel + HTML | ✅ Fait |
| Scraping automatique site BCEAO | ✅ Fait |
| Données 2023 intégrées | ✅ Fait |
| Module prédictif ML (prévisions 2024–2025) | ✅ Fait |
| Scoring risque par banque | ✅ Fait |
| Commentaires code complets | ✅ Fait |
| 29 tests unitaires (29/29) | ✅ Fait |
| Déploiement Render | ✅ Fait |
| OCR PDFs scannés | ➖ Non nécessaire (PDFs natifs) |

---

## 🔒 Sécurité

- `.env` jamais commité — protégé par `.gitignore`
- `MONGO_URI` injecté via variables d'environnement Render
- Données de production (CSV) exclues du repository
- PDFs et Excel bruts exclus du repository

---

## 👤 Auteur

**Sona KOULIBALY**
Mastère 2 Big Data & Data Strategy — 2025–2026

---

## 📄 Licence

Projet académique — Mastère 2 Big Data & Data Strategy · 2025–2026
