from src.adapters.external_api.almah_data_extract import AlmahAPIExtractor
import polars as pl
if __name__ == "__main__":
    extractor = AlmahAPIExtractor()
    try:
        units_df = extractor.get_all_units_dataframe()
        if not units_df.is_empty():
            # VERIFICAR TIPO INT64 EM CPF/CNPJ O TIPO DEVE SER STR - UTF8
            print("\nFetched Units Data (first 5 rows):")
            print(units_df.head())
            print(f"\nSchema:\n{units_df.schema}")
        else:
            print("No units data to display.")
            
    except Exception as e:
        print(f"An error occurred during API extraction: {e}")
    finally:
        extractor.close()