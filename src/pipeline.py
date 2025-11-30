import pandas as pd
from datetime import date
import os
import time
import requests
import logging
from typing import Optional

# bs4 Library
from bs4 import BeautifulSoup

# --- Variables ---
LOGS_PATH = "logs/"
URL_PREMIER_LEAGUE = "https://www.transfermarkt.com/premierleague/tabelle/wettbewerb/GB1"
DATA_RAW_PATH = "data/01_raw/"
DATA_PROCESSED_PATH = "data/02_processed/"
SLEEP_TIME = 5

# --- LOGGING ---
def setup_logging(log_path: str = LOGS_PATH) -> None:
    """Logging Configuration"""
    
    os.makedirs(log_path, exist_ok=True)

    log_filepath = os.path.join(log_path, f"pipeline_run_{date.today().strftime('%Y%m%d')}.log")
    
    logging.basicConfig(
        filename=log_filepath,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        filemode='a'
    )
    
    # Adding handler if doesn't exist 
    if not logging.getLogger().handlers:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logging.getLogger().addHandler(console_handler)

    logging.info("--- Logging Configuration completed ---")

# --- Extraction ---
def extract_data_dynamic(url: str = URL_PREMIER_LEAGUE) -> pd.DataFrame:
    """Extraction of the table from the website"""
    
    headers = {"User-Agent": "Mozilla/5.0"}
    res = requests.get(url, headers=headers)
    soup = BeautifulSoup(res.text, "html.parser")
    
    table = soup.find("table", class_="items")
    if not table:
        logging.error("Table not found")
        return pd.DataFrame()
    
    rows = []
    for row in table.find("tbody").find_all("tr"):
        cols = row.find_all("td")
        if len(cols) < 10:
            continue
        
        # Extracción directa sin variables temporales
        rows.append({
            "Pos": cols[0].get_text(strip=True),
            "Club": cols[2].get_text(strip=True),
            "P": cols[3].get_text(strip=True),
            "W": cols[4].get_text(strip=True),
            "D": cols[5].get_text(strip=True),
            "L": cols[6].get_text(strip=True),
            "Goals": cols[7].get_text(strip=True),
            '+/-': cols[8].get_text(strip=True),
            "Pts": cols[9].get_text(strip=True)
        })

    df = pd.DataFrame(rows)
    logging.info(f"Dataframe generated: {len(df)} rows")
    return df

# --- Transformation --- 
def transform_data_cleanup(df_raw: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Cleaning and transformation of the data"""
    
    if df_raw.empty:
        logging.warning("Empty Dataframe Received")
        return None
    
    # Rename columns
    column_mapping = {
        'Pos': 'Rank', 'P': 'MP', 'W': 'Wins', 
        'D': 'Ties', 'L': 'Loses', 'Goals': 'Goals_FA',
        '+/-': 'GD_Raw', 'Pts': 'Points'
    }
    
    df = df_raw.rename(columns=column_mapping)
    
    # Divide Goals and convert to numeric
    df[['GF', 'GA']] = df['Goals_FA'].str.split(':', expand=True)
    
    # CMasive conversion to numeric
    numeric_cols = ['Rank', 'MP', 'Wins', 'Ties', 'Loses', 'GF', 'GA', 'GD_Raw', 'Points']
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    
    # Calculate GD and eliminate temp columns
    df['GD'] = df['GF'] - df['GA']
    df = df.drop(columns=['Goals_FA', 'GD_Raw'])
    
    # Final cleaning
    df = df.dropna(subset=['Points', 'Club', 'MP']).reset_index(drop=True)
    
    # Reorder columns
    final_cols = ['Rank', 'Club', 'Points', 'MP', 'Wins', 'Ties', 'Loses', 'GF', 'GA', 'GD']
    df_cleaned = df[final_cols]
    
    # Save file
    file_path = os.path.join(DATA_PROCESSED_PATH, f"pl_team_status_{date.today().strftime('%Y%m%d')}.csv")
    os.makedirs(DATA_PROCESSED_PATH, exist_ok=True)
    df_cleaned.to_csv(file_path, index=False)
    
    logging.info(f"- - - Dataset saved on {file_path}")
    return df_cleaned

# --- MAIN FUNCTION ---
def main() -> None:
    """Main function"""
    setup_logging()
    
    logging.info("--- EXTRACTION STARTED ---")
    df_raw = extract_data_dynamic()
    
    logging.info("--- TRANSFORMATION STARTED ---")
    df_cleaned = transform_data_cleanup(df_raw)
    
    if df_cleaned is not None:
        logging.info(f"PROCESS COMPLETE. Dataset: {df_cleaned.shape}")
    else:
        logging.error("THE PROCESS FAILED - Empty Dataframe")

if __name__ == "__main__":
    main()
