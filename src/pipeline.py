# Contenido de src/pipeline.py (Nueva versión para Yahoo Sports)

import pandas as pd
from datetime import date
import os
import time
import requests

# Importaciones de Selenium y BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# --- LOGGING CONFIGURATION ---
import logging
# Constantes (Actualizamos la ruta del log)
LOGS_PATH = "logs/"

def setup_logging(log_path: str = LOGS_PATH):
    """Logging Configuration = Custom Logging."""
    
    if not os.path.exists(log_path):
        os.makedirs(log_path)

    log_filename = f"pipeline_run_{date.today().strftime('%Y%m%d')}.log"
    log_filepath = os.path.join(log_path, log_filename)
    
    # Configuración del logging
    logging.basicConfig(
        filename=log_filepath,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        filemode='a' # 'a' (append) para añadir al archivo si ya existe
    )
    
    # También configuramos un handler para que imprima en la consola
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    # Aseguramos que solo se añada una vez a la raíz
    if not logging.getLogger().handlers:
        logging.getLogger().addHandler(console_handler)

    logging.info("--- Logging Configuration completed ---")

# --------------------------------#

# ---------- PIPELINE ----------
# Constantes (ACTUALIZADAS A PREMIER LEAGUE / YAHOO)
URL_PREMIER_LEAGUE = "https://www.transfermarkt.com/premierleague/tabelle/wettbewerb/GB1"
DATA_RAW_PATH = "data/01_raw/"
SLEEP_TIME = 5 # Aumentamos un poco el tiempo para asegurar la carga completa de Yahoo

def extract_data_dynamic(url: str = URL_PREMIER_LEAGUE, path: str = DATA_RAW_PATH) -> pd.DataFrame:
    """
    Extract the table from the page
    """
    headers = {
    "User-Agent": "Mozilla/5.0",
    }

    # -> We do the request
    res = requests.get(url, headers=headers)
    soup = BeautifulSoup(res.text, "html.parser")

    # -> We find the tables with this specification
    table = soup.find("table", class_="items")

    
    rows = [] # - Empty list to save the columns
    for row in table.find("tbody").find_all("tr"): # - Iterate each row in the table, finding the tag tbody and tr
        
        cols = row.find_all("td")
        if len(cols) < 10:
            continue
        
        position = cols[0].get_text(strip=True)
        club = cols[2].get_text(strip=True)
        played = cols[3].get_text(strip=True)
        wins = cols[4].get_text(strip=True)
        draws = cols[5].get_text(strip=True)
        losses = cols[6].get_text(strip=True)
        goals = cols[7].get_text(strip=True)  # format "24:6"
        goal_diff = cols[8].get_text(strip=True)
        points = cols[9].get_text(strip=True)

        rows.append({
            "Pos": position,
            "Club": club,
            "P": played,
            "W": wins,
            "D": draws,
            "L": losses,
            "Goals": goals,
            "+/-": goal_diff,
            "Pts": points
        })

    df = pd.DataFrame(rows)
    logging.info("-- Dataframe Generated")
    print(df)


if __name__ == "__main__":
    print("---EXTRACTION PHASE: STARTED---")
    setup_logging()
    extract_data_dynamic()
    logging.info("---EXTRACTION COMPLETE---")