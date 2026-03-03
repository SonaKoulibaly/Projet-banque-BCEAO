# =============================================================================
# run_etl.py — Pipeline ETL complet
# Projet : Banques Sénégal | Auteur : Sona KOULIBALY
# Exécute toutes les étapes dans l'ordre avec gestion des erreurs
# =============================================================================

import subprocess
import sys
import time
from pathlib import Path

ETL_DIR = Path(__file__).parent

ETAPES = [
    ("01_load_excel.py",    "Chargement Excel → MongoDB"),
    ("02_extract_pdf.py",   "Extraction PDF BCEAO existant"),
    ("03_normalize.py",     "Harmonisation & fusion sources"),
    ("04_clean.py",         "Nettoyage & calcul ratios"),
]

ETAPES_OPTIONNELLES = [
    ("05_scrape_bceao.py",  "Scraping nouveaux rapports BCEAO"),
]

def run_step(script: str, label: str) -> bool:
    """Exécute une étape ETL et retourne True si succès."""
    print(f"\n{'─'*50}")
    print(f"▶  {label}")
    print(f"   Script : {script}")
    print(f"{'─'*50}")

    result = subprocess.run(
        [sys.executable, str(ETL_DIR / script)],
        capture_output=False
    )
    success = result.returncode == 0
    status  = "✅ Succès" if success else "❌ Échec"
    print(f"\n{status} — {label}")
    return success


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pipeline ETL Banques Sénégal")
    parser.add_argument("--avec-scraping", action="store_true",
                        help="Inclure le scraping BCEAO dans le pipeline")
    parser.add_argument("--scraping-seulement", action="store_true",
                        help="Exécuter uniquement le scraping BCEAO")
    args = parser.parse_args()

    print("\n" + "="*60)
    print("  🏦 PIPELINE ETL — BANQUES SÉNÉGAL")
    print(f"  📅 Démarrage : {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    etapes = []
    if args.scraping_seulement:
        etapes = ETAPES_OPTIONNELLES
    elif args.avec_scraping:
        etapes = ETAPES_OPTIONNELLES + ETAPES
    else:
        etapes = ETAPES

    resultats = []
    for script, label in etapes:
        ok = run_step(script, label)
        resultats.append((label, ok))
        if not ok:
            print(f"\n⚠️  Étape échouée — arrêt du pipeline")
            break
        time.sleep(1)

    # Résumé
    print("\n" + "="*60)
    print("  📊 RÉSUMÉ DU PIPELINE")
    print("="*60)
    for label, ok in resultats:
        status = "✅" if ok else "❌"
        print(f"  {status}  {label}")

    total_ok  = sum(1 for _, ok in resultats if ok)
    print(f"\n  Résultat : {total_ok}/{len(resultats)} étapes réussies")
    print("="*60 + "\n")