from data_processing.api_data_extractor import AlmahAPIExtractor

if __name__ == "__main__":
    extractor = AlmahAPIExtractor()
    try:
        units_df = extractor.get_all_units_dataframe()
        if not units_df.is_empty():
            print("\nFetched Units Data (first 5 rows):")
            print(units_df.head())
            print(f"\nSchema:\n{units_df.schema}")
        else:
            print("No units data to display.")
            
    except Exception as e:
        print(f"An error occurred during API extraction: {e}")
    finally:
        extractor.close()