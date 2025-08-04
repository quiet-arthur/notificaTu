from src.adapters.almah_data_extract import AlmahAPIExtractor
from src.services.non_paying_data_processing import DataFrameManager

import polars as pl

if __name__ == "__main__":
    extractor = AlmahAPIExtractor()
    try:
        df = extractor.get_all_non_payments_dataframe()
   
        df_final = (
            DataFrameManager(df)
            .type_handle()
            .owner_agg()
            .get_result()
        )
        print(df_final)
        
        
    except Exception as e:
        print(f"An error occurred during API extraction: {e}")
    finally:
        extractor.close()