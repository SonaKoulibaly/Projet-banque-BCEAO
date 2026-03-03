# 📖 Guide d'Utilisation — Dashboard Banques Sénégal

## 1. Vue d'ensemble du Dashboard

Le dashboard se compose de **6 sections principales** :

### 🔝 Header
- **Logo BCEAO** à gauche
- **Titre du projet** au centre
- **3 boutons de téléchargement** à droite :
  - ⬇ **Excel** → télécharge directement sur votre PC
  - ⬇ **HTML** → fichier rapport ouvrable dans tout navigateur
  - 📄 **Rapport PDF** → rapport professionnel de la banque sélectionnée

### 📊 Bandeau KPIs Globaux
Affiche en temps réel pour l'année filtrée :
- **Total Actif Bancaire** du secteur (en Mds FCFA)
- **Nombre de banques** actives
- **Effectif total** du secteur
- **Fonds propres moyens** par banque

---

## 2. Utilisation des Filtres

Les filtres en haut sont **interconnectés** — chaque modification met à jour l'ensemble du dashboard.

| Filtre | Comment l'utiliser |
|--------|-------------------|
| 📅 **Slider Période** | Glisser les poignées pour sélectionner 2015→2022 |
| 🏦 **Banques** | Cliquer pour sélectionner une ou plusieurs banques |
| 🌍 **Groupe** | Filtrer par type de groupe bancaire |
| 📊 **Indicateur** | Choisir le KPI à afficher sur les graphiques |

---

## 3. Les 6 Onglets

### 🏛 Vue Marché
**Ce que vous voyez :**
- Courbes d'évolution du Total Actif, Ressources, Emplois, Fonds Propres (2015–2022)
- Graphique circulaire de répartition par groupe bancaire
- Classement horizontal des parts de marché

**Storytelling :** Le secteur bancaire sénégalais a progressé de **+72%** entre 2015 et 2020, passant de 5 003 Mds à 8 616 Mds FCFA de total actif.

### ⚖️ Comparaison
**Ce que vous voyez :**
- **Nuage de points** : Bilan vs Résultat Net (taille = Fonds Propres)
- **Barres groupées** : comparaison de l'indicateur sélectionné par banque et par année

**Storytelling :** La CBAO et la SGBS dominent par la taille, mais certaines banques plus petites affichent de meilleures rentabilités relatives.

### 📈 Performance
**Ce que vous voyez :**
- **Heatmap** banques × années : identifier d'un coup d'œil les leaders et les retardataires
- **Barres de croissance** : qui a le plus progressé entre la première et la dernière année ?

**Anomalies détectées :** Les cases rouges dans la heatmap signalent des valeurs nulles ou manquantes — données à compléter via les rapports PDF BCEAO.

### ⚙️ Ratios Financiers
**Ce que vous voyez :**
- **ROA vs ROE** : les banques les plus rentables en haut à droite
- **CIR** : coefficient d'exploitation — seuil optimal < 60%, critique > 80%

**Interprétation :**
- ROA > 1.5% = excellente rentabilité des actifs
- ROE > 10% = bonne rémunération des actionnaires
- CIR < 60% = banque bien gérée (vert), > 80% = inefficience (rouge)

### 🗺 Carte Sénégal
**Ce que vous voyez :**
- Localisation des sièges sociaux à Dakar
- Taille des bulles proportionnelle au bilan
- Couleurs par groupe bancaire

**Usage :** Cliquer sur une bulle pour voir les détails de la banque.

### 🏆 Classement
**Ce que vous voyez :**
- Tableau complet des 24+ banques triées par bilan
- Médailles 🥇🥈🥉 pour le podium
- Résultat net coloré en vert (bénéfice) ou rouge (perte)

---

## 4. Analyse d'une Banque Individuelle

En bas du dashboard, la section **"Analyse Individuelle"** permet de :

1. **Sélectionner une banque** dans le dropdown
2. **Choisir une année** de référence
3. Voir ses **6 KPIs** en temps réel (Bilan, Résultat Net, Fonds Propres, ROA, ROE, Effectif/Agences)
4. Consulter son **graphique d'évolution** sur toute la période
5. Voir son **radar chart** de positionnement normalisé
6. Cliquer **"📄 Rapport PDF"** pour télécharger le rapport complet

---

## 5. Téléchargements

### Excel
- Contient les données filtrées en cours
- Deux onglets : "Données Brutes" et "Résumé par Banque"
- Téléchargement direct sur votre ordinateur

### HTML
- Rapport statique avec tableau de classement complet
- Ouvrable dans tout navigateur sans connexion internet
- Partage facile par email

### PDF (Rapport Banque)
- Rapport professionnel A4 avec logo BCEAO
- Contient : KPIs, évolution historique, ratios financiers, positionnement marché, analyse synthétique
- Numéros de page et footer automatiques
- Idéal pour présentation aux décideurs

---

## 6. Lire les Données Correctement

### Unités
Toutes les valeurs financières sont en **millions de FCFA** sauf mention contraire.
- "1 000" = 1 000 millions = 1 milliard FCFA
- "Mds" = milliards de FCFA

### Valeurs N/D
"N/D" signifie que la donnée n'est pas disponible pour cette banque/année. Cela est normal car certaines colonnes (PNB, Résultat Net) ne sont disponibles que pour 2017–2019 dans la base Excel.

### Codes couleur des ratios
- 🟢 **Vert** = bonne performance (résultat positif, CIR < 60%)
- 🟡 **Or** = performance modérée (CIR 60–80%)
- 🔴 **Rouge** = vigilance requise (perte, CIR > 80%)

---

## 7. FAQ

**Q : Le dashboard ne charge pas de données ?**
R : Vérifiez que l'ETL a bien tourné (`python etl/run_etl.py`) et que votre `.env` contient le bon `MONGO_URI`.

**Q : La carte ne s'affiche pas ?**
R : La carte Mapbox nécessite une connexion internet active.

**Q : Les ratios affichent N/D ?**
R : Normal pour les années 2015–2016 et 2020 — ces colonnes n'étaient pas disponibles dans la base Excel source.

**Q : Comment ajouter une nouvelle année ?**
R : Télécharger le rapport PDF BCEAO de l'année concernée, le placer dans `data/raw/` et relancer `python etl/run_etl.py`.