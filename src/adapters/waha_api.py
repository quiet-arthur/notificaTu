from qrcode.image.pil import PilImage
from qrcode.image.pure import PyPNGImage
import httpx
import textwrap
import time
import qrcode


class WahaClient():
    def __init__(self, port: int, session_name: str):
        '''
        
        '''
        self.base_url: str = f"http://localhost:{port}/api"
        self.client: Any = httpx.Client()
        self.port: int = port
        self.session_name: str = session_name
        self.headers: dict = {
            "Content-Type": "application/json",
            "X-Api-Key": "admin"
        }

    def _get_session_status(self) -> dict:
        url: str = f'{self.base_url}/sessions/{self.session_name}'
        response = self.client.get(url)

        return response.json()

    def _start_session(self) -> dict:
        url = f"{self.base_url}/sessions/{self.session_name}/start"
        response = self.client.post(url, headers=self.headers)

        
    def _stop_session(self) -> None:
        url: str = f"{self.base_url}/sessions/{self.session_name}/stop"
        response: dict = self.client.post(url, headers=self.headers)

    
    def _restart_session(self) -> None:
        url: str = f"{self.base_url}/sessions/{self.session_name}/restart"
        response = self.client.post(url, headers = self.headers)

    
    def _logout_session(self) -> None:
        url: str = f"{self.base_url}/sessions/{self.session_name}/logout"
        response: dict = self.client.post(url, headers=self.headers)
    
    def _get_qrcode(self) -> dict:
        url: str = f"{self.base_url}/{self.session_name}/auth/qr?format=raw"
        response = self.client.get(url, headers=self.headers)

        return response.json()
        

class SessionHandler():
    def __init__(self, client: WahaClient):
        '''
        
        '''
        self.client: WahaClient = client
        self.status: dict = client._get_session_status()

    def _start_new_session(self) -> None:
        ...
        
    def _ensure_session_is_active(self, timeout: int = 90) -> bool:
        start: int = time.time()
        
        while time.time() - start < timeout:
            self.status = self.client._get_session_status()
            status: str = self.status['status']
            
            if status == "WORKING":
                return True
            
            elif status == "STARTING":
                pass
            
            elif status == "STOPPED":
                self.client._start_session()

            elif status == "SCAN_QR_CODE":
                qrcode_value = self.client._get_qrcode()
                self._get_auth(qrcode_value)
            
            else:
                return False
            
            time.sleep(30)
    
    def _get_auth(self, qrcode_data: str):
        qr: qrcode.QRCode[PilImage | PyPNGImage] = qrcode.QRCode()
        qr.add_data(qrcode_data['value'])
        qr.print_ascii(invert=True)
    
def main():
    session = SessionHandler(WahaClient(3000, "default"))
    print(session._ensure_session_is_active())


main()


















# class WppEngine():
#     def __init__(self):
#         ...
#         "Implementar work-flow de inicialização do docker"
#         self.client = httpx.Client()

#     def _check_health(self):
#         url = 'http://localhost:3000/api/default/auth/qr'
#         response = self.client.post(url, headers=self._headers())
#         return response.json()
        
    
#     def _headers(self):
#         return {
#             "Accept": "application/json",
#             "Content-Type": "application/json"
#         }

#     def send_nonpayment_notification(self, tel, unit, condom, block=None):

#         text = textwrap.dedent(f"""
#             📢 *Lembrete – Condomínio {condom}*

#             Prezado(a) condômino(a),

#             Consta em aberto débitos referente à(s) taxa(s) condominial(is) da *unidade {unit}*.  
#             Por favor, regularize sua situação o quanto antes para evitar maiores transtornos.  

#             Caso já tenha efetuado o pagamento, *favor desconsidar essa mensagem!*.

#             Atenciosamente,  
#             Assessoria Jurídica – {condom}
#         """)
        
#         msg_config = {
#             "chatId": f"{tel}@c.us",
#             "text": text,
#             "session": "default"
#         }

#         response = self.client.post(
#             self.message_url, json=msg_config, headers=self._headers()
#         )
#         return response.json()