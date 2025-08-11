from src.adapters.almah_data_extract import AlmahAPIExtractor
from src.services.non_paying_data_processing import NonPaymentManager

import polars as pl

if __name__ == "__main__":
    extractor = AlmahAPIExtractor()
    try:
        default_df = extractor.get_all_non_payments_dataframe()
        # print(default_df)
        # print(default_df.get_columns())

        units_df = extractor.get_all_units_dataframe()
        # print(units_df)
        # print(units_df.get_columns())

        df_final = (
            NonPaymentManager(default_df, units_df)
                .get_non_payment_data()
        )
        print(df_final)
        # print(df1)
        # print(df2)
        
    except Exception as e:
        print(f"An error occurred during API extraction: {e}")
    finally:
        extractor.close()