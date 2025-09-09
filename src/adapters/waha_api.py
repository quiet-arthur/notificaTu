import requests
import polars as pl
import textwrap

class WppEngine():
    def __init__(self):
        self.url = "http://localhost:3000/api/sendText"
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
    
    def send_nonpayment_notification(self, tel, unit, condom, block=None):

        text = textwrap.dedent(f"""
            📢 *Lembrete – Condomínio {condom}*

            Prezado(a) condômino(a),

            Consta em aberto débitos referente à(s) taxa(s) condominial(is) da *unidade {unit}*.  
            Por favor, regularize sua situação o quanto antes para evitar maiores transtornos.  

            Caso já tenha efetuado o pagamento, *favor desconsidar essa mensagem!*.

            Atenciosamente,  
            Assessoria Jurídica – {condom}
        """)
        
        msg_config = {
            "chatId": f"{tel}@c.us",
            "text": text,
            "session": "default"
        }

        response = requests.post(
            self.url, json=msg_config, headers=self.headers
        )

        return response.json()