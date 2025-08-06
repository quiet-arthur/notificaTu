import polars as pl 

def format_phone(tel_column):
    phone = (
        pl.col(tel_column)
        .cast(pl.Utf8)
        .str.replace_all(r"[^0-9]", "")
    )
    return (
        pl.when(phone.is_not_null() & ~phone.str.starts_with("67"))
        .then("+5567" + phone)
        .otherwise(
            pl.when(phone.is_not_null() & phone.str.starts_with("67"))
            .then("+55" + phone)
        )
    )

def format_cpf(cpf_column):
    '''
        Create a function for formatting cpf/cnpj and handle zeros exclusions
    '''
    ...

class NonPaymentManager:
    def __init__(self, default_df, units_df):
        '''
            Escrever algo que descreva a função
        '''
        self.origin_default_df = default_df
        self.origin_units_df = units_df
        self.clone_units_df = units_df.clone()
        self.clone_default_df = default_df.clone()

    def set_columns(self):
        '''
            Escrever algo que descreva a função
        '''
        self.clone_default_df = self.clone_default_df.select([
            pl.col("Unidade").alias("unidade"),
            pl.col("Nome do Pagador").alias("proprietario"),
            pl.col("Vlr Total").
                str.replace(",", ".")
                .cast(pl.Float64)
                .alias("vlr_total"),
        ])

        self.clone_units_df = self.clone_units_df.select([
            pl.col("Unidade").alias("unidade"),
            pl.col("CodigoBloco").cast(pl.Utf8).alias("bloco"),
            pl.col("ProprietarioCpfCnpj").cast(pl.Utf8).alias("cpf_cnpj"),
            pl.concat_list(["ProprietarioEmail1", "ProprietarioEmail2"])
                .list.drop_nulls()
                .alias("email_list"),
            pl.concat_list([
                format_phone("ProprietarioTelefone1"),
                format_phone("ProprietarioTelefone2"),
            ])
                .list.drop_nulls()
                .alias("phone_list")
        ])
        
        return self

    def default_agg(self):
        '''
            Escrever algo que descreva a função
        '''
        self.clone_default_df = self.clone_default_df.group_by(
            pl.col("unidade"),
            pl.col("proprietario"),
            maintain_order=True
        ).agg(
            pl.col("vlr_total").sum()
                .alias("vlr_inadimplente"),
        )
        return self
    
    def get_non_payment_data(self):
        self.set_columns()
        self.default_agg()

        # return self.clone_units_df, self.clone_default_df
        return self.clone_default_df.join(self.clone_units_df, on="unidade")