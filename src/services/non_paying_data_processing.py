from datetime import date 
from math import prod
import requests
import polars as pl 
import re

# TROCAR NOME CLASSE -> DataInteger
class DataInteger:
    def __init__(self, debts, units_df):
        '''
            Escrever algo que descreva a função
        '''
        self.origin_debts = debts
        self.origin_units_df = units_df
        self.clone_units_df = units_df.clone()
        self.clone_debts = debts.clone()

    def _set_debts_columns(self):
        '''
            Escrever algo que descreva a função
        '''
        self.clone_debts = self.clone_debts.select([
            pl.col("Unidade")
                .cast(pl.Utf8)
                .map_elements(format_unit, return_dtype=pl.String)
                .alias("unit"),
            pl.col("Nome do Pagador").alias("owner"),
            pl.col("Vlr Original")
                .str.replace(",", ".")
                .cast(pl.Float64)
                .alias("vl_original"),
            pl.col("Vlr Total")
                .str.replace(",", ".")
                .cast(pl.Float64)
                .alias("vl_taxes"),
            pl.col("Venc"),
            pl.col("Status")
        ])

        return self.clone_debts

    def _set_units_columns(self):
        '''
            Escrever algo que descreva a função
        '''
        self.clone_units_df = self.clone_units_df.select([
            pl.col("Unidade")
                .cast(pl.Utf8)
                .map_elements(format_unit, return_dtype=pl.String)
                .alias("unit"),
            pl.col("CodigoBloco").cast(pl.Utf8).alias("block"),
            format_cpf_pl(pl.col("ProprietarioCpfCnpj")).alias("cpf_cnpj"),
            pl.concat_list(["ProprietarioEmail1", "ProprietarioEmail2"])
                .list.drop_nulls()
                .alias("email_list"),
            pl.concat_list([
                format_phone(pl.col("ProprietarioTelefone1")),
                format_phone(pl.col("ProprietarioTelefone2")),
            ])
                .list.drop_nulls()
                .alias("phone_list")
        ])

        return self.clone_units_df

    def default_agg(self):
        '''
            Escrever algo que descreva a função
        '''
        self.clone_debts = self.clone_debts.group_by(
            pl.col("unidade"),
            pl.col("proprietario"),
            maintain_order=True
        ).agg(
            pl.col("vlr_juros_multa")
                .sum()
                .alias("vlr_inadimplente"),
        )
        return self
    
    def get_non_payment_data(self):
        self._set_debts_columns()
        self._set_units_columns()
        self.default_agg()

        return self.clone_debts.join(self.clone_units_df, on="unit")

# VERIFICAR VIABILIDADE DA CLASSE, EM VISTA DAS DIVERGENCIAS DE VALORES
class LevelPriceCalculator:
    def __init__(self, index_code=189, stop_deflection=False, include_start_month=True, include_end_month=False ) -> None:
        '''
            Docstring function
        '''
        self.index_code = index_code
        self.stop_deflection = stop_deflection
        self.include_start_month = include_start_month
        self.include_end_month = include_end_month
        self.monthly_series = self._get_igpm_bcb()

    def _get_igpm_bcb(self):
        '''Docstring this function'''
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{self.index_code}/dados?formato=json"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        series = {}

        for index in data:
            date_str = index["data"]
            value = float(index['valor'].replace(',', '.')) / 100.0
            year = int(date_str[6:])
            month = int(date_str[3:5])
            series[f"{year:04d}-{month:02d}"] = value

        return series
    
    def _format_yyyymm(self, date):
        return f"{date.year:04d}-{date.month:02d}"
    
    def _set_next_month(self, year, month):
        return (year + (month // 12), 1 if month == 12 else month + 1)
    
    def _iter_yyyymm(self, start, end):
        y, m = start.year, start.month

        if not self.include_start_month:
            y, m = self._set_next_month(y, m)
    
        end_key = self._format_yyyymm(end)
        cur_key = f"{y:04d}-{m:02d}"

        while True:
            if cur_key > end_key or (cur_key == end_key and not self.include_end_month):
                break
            yield cur_key

            y, m = self._set_next_month(y, m)
            cur_key = f"{y:04d}-{m:02d}"
    
    def calculate_igpm(self, value, ref_date, base_date):
        ''''
            Docstring this function
        '''
        factors = []

        for month in self._iter_yyyymm(ref_date, base_date):
            factor = self.monthly_series.get(month, 0.0)
            if self.stop_deflection and factor < 0:
                factor = 0.0
            factors.append(1.0 + factor)
        
        accumulated_factors = prod(factors) if factors else 1.0
        print(accumulated_factors)
        return round(value * (accumulated_factors - 1.0), 2)

# DOCUMENTAR FUNÇÕES E VERIFICAR VIABILIDADE EM SER METODO DA CLASSE: "DataInteger"
def format_phone(col: pl.Expr) -> pl.Expr:
    return (
        col
        .cast(pl.Utf8)
        .str.replace_all(r"\D", "")
        .str.strip_chars("+")
        .pipe(lambda c: pl.when(~c.str.starts_with("55"))
                          .then(pl.lit("55") + c)
                          .otherwise(c))
        .str.replace_all(r"^(55\d{2})9(\d{8})$", r"$1$2")
    )

def format_cpf_pl(col: pl.Expr) -> pl.Expr:
    return (
        col.cast(pl.Utf8)                        
           .str.replace_all(r"\D", "")           
           .str.zfill(11)                        
           .str.replace_all(r"^(\d{3})(\d{3})(\d{3})(\d{2})$", "$1.$2.$3-$4")  
    )

def format_unit(unit: str) -> str:
    if not unit:
        return ""

    unit = unit.upper().strip()
    unit = re.sub(r'\b(BLOCO|BL\.?|APTO|APT|AP)\b', '', unit)
    unit = re.sub(r'[\-\.]', ' ', unit)
    unit = re.sub(r'\s+', ' ', unit).strip()

    sections = unit.split()

    block = ""
    apt = ""

    for section in sections:
        if re.fullmatch(r'[A-Z]', section) and not block:
            block = section
        elif re.fullmatch(r'\d+', section) and not apt:
            apt = section

    return f"{block}{apt}" if block else apt

