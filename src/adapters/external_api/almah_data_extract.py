from settings import AlmahAPI, get_api_credentials # Importar a classe AlmahAPI e a função de credenciais
import httpx
import polars as pl
from bs4 import BeautifulSoup
from io import StringIO
import csv

class AlmahAPIExtractor:
    """
    Handles authentication and data extraction from the Almah API.
    Returns data as Polars DataFrames.
    """
    def __init__(self):
        self.base_url = AlmahAPI.BASE_URL
        self.login_endpoint = AlmahAPI.LOGIN_ENDPOINT
        self.access_endpoint = AlmahAPI.ACCESS_ENDPOINT
        self.units_export_endpoint = AlmahAPI.UNITS_EXPORT_ENDPOINT

        # IDs específicos do ambiente (podem ser passados no construtor ou lidos de settings/env)
        self.condominio_id = AlmahAPI.CONDOMINIO_ID
        self.usuario_id = AlmahAPI.USUARIO_ID
        self.estabelecimento_id = AlmahAPI.ESTABELECIMENTO_ID
        self.perfil_uso_id = AlmahAPI.PERFIL_USO_ID

        # Obter credenciais do .env via settings.py
        credentials = get_api_credentials()
        self.login_user = credentials.get("user")
        self.alpha_password_hash = credentials.get("alpha_password")

        self.client = httpx.Client() # O cliente HTTPX será gerenciado pela instância da classe
        self._is_authenticated = False # Flag para controlar o status de autenticação

    def _login_and_set_cookies(self) -> bool:
        """
        Performs login and sets necessary cookies for subsequent requests.
        Returns True if login is successful, False otherwise.
        """
        login_url = f'{self.base_url}{self.login_endpoint}'
        payload_login = {
            "login": self.login_user,
            "senhaCriptografada": self.alpha_password_hash
        }

        try:
            response_login = self.client.post(
                login_url,
                json=payload_login
            )
            response_login.raise_for_status() # Levanta exceção para status de erro HTTP
            print(f"Status Code (Login): {response_login.status_code}")

            login_data = response_login.json()
            if not login_data.get('d') or not login_data['d'][0].get('Codigo'):
                print("Login failed: No 'Codigo' found in response.")
                return False

            access_url = f"{self.base_url}{self.access_endpoint}"
            params = {
                "Empresa": login_data['d'][0]['Codigo'],
                "Estabelecimento": self.estabelecimento_id,
                "Condominio": self.condominio_id,
                "PerfilUso": self.perfil_uso_id,
                "Usuario": self.usuario_id,
            }
            response_access = self.client.get(
                access_url,
                params=params,
            )

            if response_access.cookies:
                print(f'Cookies captured with success!')
                self._is_authenticated = True
                return True
            else:
                print('Failed to capture cookies after access request.')
                return False
            
        except httpx.HTTPStatusError as e:
            print(f"HTTP error during login: {e.response.status_code} - {e.response.text}")
            return False
        except httpx.RequestError as e:
            print(f"Network error during login: {e}")
            return False
        except Exception as e:
            print(f"An unexpected error occurred during login: {e}")
            return False

    def _get_units_html(self) -> str:
        """
        Fetches the HTML content containing units data.
        Returns the HTML content as a string, or an empty string if failed.
        """
        if not self._is_authenticated:
            print("Not authenticated. Attempting to log in...")
            if not self._login_and_set_cookies():
                print("Failed to authenticate. Cannot get units data.")
                return ""

        units_url = f'{self.base_url}{self.units_export_endpoint}'
        payload_units = {"codigoCondominio": self.condominio_id}

        try:
            response = self.client.post(
                units_url,
                json=payload_units
            )
            response.raise_for_status()
            units_data = response.json()
            html_content = units_data.get('d', '')

            if html_content:
                print("Units HTML content fetched successfully.")
                return html_content
            else:
                print('No HTML content found. Verify the units export!')
                return ""
            
        except httpx.HTTPStatusError as e:
            print(f"HTTP error fetching units: {e.response.status_code} - {e.response.text}")
            return ""
        except httpx.RequestError as e:
            print(f"Network error fetching units: {e}")
            return ""
        except Exception as e:
            print(f"An unexpected error occurred fetching units: {e}")
            return ""

    def extract_units_data(self, html_content: str) -> pl.DataFrame:
        """
        Extracts tabular data from HTML content and returns it as a Polars DataFrame.
        """
        if not html_content:
            print("No HTML content provided for extraction. Returning empty DataFrame.")
            return pl.DataFrame()

        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            table = soup.find('table')

            if not table:
                print("No table found in the HTML content. Returning empty DataFrame.")
                return pl.DataFrame()

            output = StringIO()
            writer = csv.writer(output, delimiter=';')

            # Extract header
            header = []
            th_elements = table.thead.find_all('th') if table.thead else []
            for th in th_elements:
                header.append(th.get_text(strip=True))
            writer.writerow(header)

            # Extract rows
            tbody = table.tbody
            if tbody:
                for row in tbody.find_all('tr'):
                    data = []
                    for td in row.find_all('td'):
                        text = ' '.join(td.get_text(strip=True).split())
                        data.append(text)
                    writer.writerow(data)

            output.seek(0) # Rewind to the beginning of the StringIO object

            df = pl.read_csv(output, separator=';')
            output.close()
            print(f"Extracted {len(df)} rows of units data.")
            return df
        
        except Exception as e:
            print(f"Error extracting units data from HTML: {e}")
            return pl.DataFrame()

    def get_all_units_dataframe(self) -> pl.DataFrame:
        """
        Orchestrates the process of logging in, fetching HTML, and extracting data.
        Returns a Polars DataFrame with all units data.
        """
        html_content = self._get_units_html()
        if html_content:
            return self.extract_units_data(html_content)
        else:
            return pl.DataFrame()

    def close(self):
        """Closes the httpx client session."""
        if self.client:
            self.client.close()
            print("HTTPX client session closed.")

# Example usage (for testing within this file, or in main.py):
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