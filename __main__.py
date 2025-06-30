from src.adapters.external_api.almah_data_extract import AlmahAPIExtractor
import polars as pl
if __name__ == "__main__":
    extractor = AlmahAPIExtractor()
    try:
        extractor._get_units_bills_html()
    except Exception as e:
        print(f"An error occurred during API extraction: {e}")
    finally:
        extractor.close()