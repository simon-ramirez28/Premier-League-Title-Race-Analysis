from pipeline import setup_logging, loading_data, transform_data_cleanup, extract_data_dynamic, logging


# --- MAIN FUNCTION ---
def main() -> None:
    """Main function"""
    setup_logging()
    
    logging.info("--- EXTRACTION STARTED ---")
    df_raw = extract_data_dynamic()
    
    logging.info("--- TRANSFORMATION STARTED ---")
    df_cleaned = transform_data_cleanup(df_raw)
    
    if df_cleaned is not None:
        logging.info(f"TRANSFORMATION COMPLETE. Dataset: {df_cleaned.shape}")
    else:
        logging.error("THE PROCESS FAILED - Empty Dataframe")
    
    logging.info("--- LOADING DATA ---")
    loading_data(df_cleaned)

if __name__ == "__main__":
    main()