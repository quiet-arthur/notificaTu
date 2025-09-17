import os
from dotenv import load_dotenv 
import logging

# Chamada para carregar as variáveis de ambiente
load_dotenv()
def get_api_credentials():
    """
    Retrieves API credentials from environment variables.
    """
    return {
        "user": os.getenv("LOGIN_USER"),
        "alpha_password": os.getenv("ALPHA_PASSWORD_HASH")
    }

class AlmahAPI:
    '''
        Class created to set the URL's used on the Almah API and 
        headers/ID's of the API environment.
    '''
    BASE_URL = "https://alphaassessoriams.almahcondos.com.br"
    LOGIN_ENDPOINT = "/SIS/EmpresaWS.asmx/SelecionarPorLoginSenha"
    ACCESS_ENDPOINT = "/novo_acesso.aspx"
    UNITS_EXPORT_ENDPOINT = "/CND/UnidadeCondominioWS.asmx/Exportar"
    UNITS_BILLS_EXPORT_ENDPOINT = "/FIN/ContasAReceberWS.asmx/GerarRelatorioInadimplenciaCondominioXLS"

    # IDs específicos do ambiente
    CONDOMINIO_ID = 164
    USUARIO_ID = 341
    ESTABELECIMENTO_ID = 1
    PERFIL_USO_ID = 1

# Chamada de configuração do modulo de Logging
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler() # Envia logs para o console (stderr por padrão)
        # logging.FileHandler("app.log") # Descomente para enviar logs para um arquivo
    ]
)