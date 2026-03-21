# diagnostico_cvm_docs.py
import requests

# Testar URLs disponíveis na CVM
urls_teste = [
    "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/PROSPECTO/DADOS/prospecto_cia_aberta.csv",
    "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/PROSPECTO/",
    "https://dados.cvm.gov.br/dados/OFERTA/DISTRIB/DOC/",
]

for url in urls_teste:
    try:
        resp = requests.get(url, timeout=10, stream=True)
        print(f"URL: {url}")
        print(f"  Status: {resp.status_code}")
        print(f"  Content-Type: {resp.headers.get('content-type', 'N/A')}")
        print(f"  Content-Length: {resp.headers.get('content-length', 'desconhecido')} bytes")
        print()
    except Exception as e:
        print(f"URL: {url} -> ERRO: {e}")
        print()
