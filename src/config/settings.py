import os
from dotenv import load_dotenv

load_dotenv()

class AlmahAPI:
    
    BASE_URL = "https://alphaassessoriams.almahcondos.com.br"
    LOGIN_ENDPOINT = "/SIS/EmpresaWS.asmx/SelecionarPorLoginSenha"
    ACCESS_ENDPOINT = "/novo_acesso.aspx"
    UNITS_EXPORT_ENDPOINT = "/CND/UnidadeCondominioWS.asmx/Exportar"
    UNITS_BILLS_EXPORT_ENDPOINT = "/FIN/ContasAReceberWS.asmx/GerarRelatorioInadimplenciaCondominioXLS"

    # IDs específicos do ambiente
    CONDOMINIO_ID = 205
    USUARIO_ID = 341
    ESTABELECIMENTO_ID = 1
    PERFIL_USO_ID = 1

def get_api_credentials():

    """
    Retrieves API credentials from environment variables.
    """
    return {
        "user": os.getenv("LOGIN_USER"),
        "alpha_password": os.getenv("ALPHA_PASSWORD_HASH")
    }