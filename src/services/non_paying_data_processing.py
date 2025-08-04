import polars as pl 

class DataFrameManager:
    def __init__(self, df):
        self.origin_df = df
        self.clone_df = df.clone()
    
    def type_handle(self):
        self.clone_df = self.clone_df.with_columns([
            pl.col("Multa").str.replace(",", ".").cast(pl.Float64),
            pl.col("Juros").str.replace(",", ".").cast(pl.Float64),
            pl.col("Vlr Total").str.replace(",", ".").cast(pl.Float64),
        ])
        return self
    
    def owner_agg(self):
        self.clone_df = self.clone_df.group_by(
            pl.col("Unidade").alias("unidade"),
            maintain_order=True
        ).agg(
            pl.col("Vlr Total").sum().alias("vlr_inadimplente")
        )
        return self
    
    def get_result(self):
        return self.clone_df