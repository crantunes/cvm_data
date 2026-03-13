import os
import requests
import time

# Pasta já criada pelo usuário
OUTPUT_DIR = "norma_cpcs_pt_br"

# Lista dos 42 CPCs correspondentes às IFRS/IAS vigentes em 31/12/2023
# Nota: Alguns números pulam conforme a convergência brasileira (ex: CPC 00, CPC 01)
cpcs_alvo = [
    "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", 
    "15", "16", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", 
    "29", "30", "31", "32", "33", "35", "36", "37", "38", "39", "40", "41", "46", 
    "47", "48", "50"
]

def download_cpcs():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    # Base URL do repositório de documentos do CPC
    base_url = "http://www.cpc.org.br/Documentos/Pronunciamentos/Arquivos/"
    
    print(f"--- Iniciando Carga de 42 CPCs para Data Lake ---")

    for cpc in cpcs_alvo:
        # A estratégia aqui é tentar baixar a versão estável. 
        # O padrão da CVM/CPC é 'CPC_XX_ref.pdf'
        success = False
        
        # Tentamos Versões R2, R1 e Original, parando na primeira que encontrar
        # respeitando que versões de 2024 seriam ignoradas manualmente se necessário
        versoes = ["R2", "R1", ""] 
        
        for v in versoes:
            suffix = f"({v})" if v else ""
            filename = f"CPC_{cpc}{v}.pdf"
            url = f"{base_url}{filename}"
            
            try:
                r = requests.get(url, headers=headers, timeout=15)
                if r.status_code == 200:
                    path = os.path.join(OUTPUT_DIR, f"CPC_{cpc}{v}.pdf")
                    with open(path, 'wb') as f:
                        f.write(r.content)
                    print(f"  [SUCESSO] CPC {cpc} {v} baixado.")
                    success = True
                    break
            except:
                continue
        
        if not success:
            print(f"  [AVISO] Não foi possível encontrar CPC {cpc} automaticamente.")
        
        time.sleep(1) # Delay para não sobrecarregar o servidor do CPC

if __name__ == "__main__":
    download_cpcs()