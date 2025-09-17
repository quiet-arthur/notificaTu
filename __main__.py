from src.adapters.almah_data_extract import AlmahAPIExtractor
from src.services.non_paying_data_processing import DataInteger
from src.adapters.waha_api import WppEngine

import polars as pl

if __name__ == "__main__":
    extractor = AlmahAPIExtractor()
    wpp = WppEngine()
    try:
        # # BUSCA OS DADOS NO SISTEMA API-ALMAH
        # debts_df = extractor.get_all_non_payments_dataframe()
        # units_df = extractor.get_all_units_dataframe()

        # # FORMATA DADOS DE INADIMPLENCIA
        # final_data = (
        #     DataInteger(debts_df, units_df)
        #         .get_non_payment_data()
        # )
        # print(final_data)

        # test_df = pl.DataFrame({
        #     "unit": ["A001"],
        #     "phone_list": [["556799560548"]]
        # })

        # for unit, phone_list in test_df.select(["unit", "phone_list"]).iter_rows():
        #     for phone in phone_list:
        #         wpp.send_nonpayment_notification(phone, unit, "PS Assessoria")

        print(wpp._check_health())
        
    except Exception as e:
        print(f"An error occurred during API extraction: {e}")
    finally:
        pass
        # extractor.close()