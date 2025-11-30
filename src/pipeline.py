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
# log Path
LOGS_PATH = "logs/"

def setup_logging(log_path: str = LOGS_PATH):
    """Logging Configuration = Custom Logging."""
    
    #- If the route doesn't exist: create it
    if not os.path.exists(log_path):
        os.makedirs(log_path)

    # -> Log Customization
    log_filename = f"pipeline_run_{date.today().strftime('%Y%m%d')}.log"
    log_filepath = os.path.join(log_path, log_filename)
    
    # Basic Config
    logging.basicConfig(
        filename=log_filepath,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        filemode='a' # 'a' (append) para añadir al archivo si ya existe
    )
    
    # Handler to print in the console too
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    # Just add it ONE TIME
    if not logging.getLogger().handlers:
        logging.getLogger().addHandler(console_handler)

    logging.info("--- Logging Configuration completed ---")

# --------------------------------#

# ---------- PIPELINE ----------
# Constantes (ACTUALIZADAS A PREMIER LEAGUE / YAHOO)
URL_PREMIER_LEAGUE = "https://www.transfermarkt.com/premierleague/tabelle/wettbewerb/GB1"
DATA_RAW_PATH = "data/01_raw/"
DATA_PROCESSED_PATH = "data/02_processed/"
SLEEP_TIME = 5 

# Extraction
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
    return df

# Transformation
def transform_data_cleanup(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Cleaning and transformation phase"""

    df = df_raw.copy() # A copy of the Dataframe

    # Rename the columns
    try:
        df = df.rename(columns={
            'Pos': 'Rank',
            'Club': 'Club',
            'P': 'MP', # Matches Played
            'W': 'Wins',
            'D': 'Ties',
            'L': 'Loses',
            'Goals': 'Goals_FA', # Format "24.6"
            '+/-': 'GD_Raw', # Goal Difference
            'Pts': 'Points'
        })
    except KeyError as e:
        # Error management (ej. Unnamed: 0, Unnamed: 1)
        logging.error(f"Error renaming: {e}. header is different.")
        return pd.DataFrame()
    
    # Divide the Goals table ':'
    df[['GF', 'GA']] = df['Goals_FA'].str.split(':', expand=True)
    
    # Columns that MUST be numeric
    cols_to_numeric = ['Rank','MP', 'Wins', 'Ties', 'Loses', 'GF', 'GA', 'GD_Raw', 'Points']

    for col in cols_to_numeric:
        # Convert to numeric, forcing NaN errors
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Calculate GD to confirm that's the same as 'GD_Raw'
    df['GD'] = df['GF'] - df['GA']

    # Eliminate original columns
    df = df.drop(columns=['Goals_FA', 'GD_Raw'])
    
    # Make sure there are not null values on the target columns
    df = df.dropna(subset=['Points', 'Club', 'MP']).reset_index(drop=True)
    
    # Reordenate the dataframe for clarity
    df_cleaned = df[['Rank', 'Club', 'Points', 'MP', 'Wins', 'Ties', 'Loses', 'GF', 'GA', 'GD']]
    
    print(df_cleaned)

    # Export the new dataset
    file_path = os.path.join(DATA_PROCESSED_PATH, f"pl_team_status_{date.today().strftime('%Y%m%d')}.csv")
    df_cleaned.to_csv(file_path, index=False)
    print(f"New dataset saved at {file_path}")

    return df_cleaned.info()

# Loading - Load the info to a Database
def loading_table():
    return None

def main():
    print("---EXTRACTION PHASE: STARTED---")
    setup_logging()
    df_raw = extract_data_dynamic()
    logging.info("---EXTRACTION COMPLETE---")
    print("----------------------\n")
    logging.info("---TRANSFORMATION PHASE: STARTED")
    df_cleaned = transform_data_cleanup(df_raw)  
    logging.info("---TRANSFORMATION COMPLETED---")

if __name__ == "__main__":
    main()