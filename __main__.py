from src.adapters.external_api.almah_data_extract import AlmahAPIExtractor
import polars as pl
if __name__ == "__main__":
    extractor = AlmahAPIExtractor()
    try:
        # print(extractor.get_all_units_dataframe())
        print(extractor.get_all_non_payments_dataframe())
        
    except Exception as e:
        print(f"An error occurred during API extraction: {e}")
    finally:
        extractor.close()