import os
import requests
import time

OUTPUT_DIR = "ifrs_originals_en"
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# Lista consolidada dos 42 conforme ifrs_ias.txt
normas = {
    "IFRS": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17],
    "IAS": [1, 2, 7, 8, 10, 12, 16, 19, 20, 21, 23, 24, 26, 27, 28, 29, 32, 33, 34, 36, 37, 38, 39, 40, 41]
}

def download_ifrs_final():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    # Base URL estável para as normas vigentes em 2023
    base_url = "https://www.ifrs.org/content/dam/ifrs/publications/pdf-standards/english/2023/issued/part-a/"

    for prefix, numeros in normas.items():
        for n in numeros:
            # Tenta o formato padrão do servidor
            filename = f"{prefix.lower()}-{n}.pdf"
            url = f"{base_url}{filename}"
            
            print(f"Buscando {prefix} {n}...")
            try:
                r = requests.get(url, headers=headers, stream=True, timeout=15)
                if r.status_code == 200:
                    with open(os.path.join(OUTPUT_DIR, f"{prefix}_{n}.pdf"), 'wb') as f:
                        f.write(r.content)
                    print(f"  [SUCESSO]")
                else:
                    print(f"  [ERRO {r.status_code}] Verifique se a norma requer login na IFRS Foundation.")
                time.sleep(2) # Pausa para evitar bloqueio por segurança
            except Exception as e:
                print(f"  [FALHA] {e}")

if __name__ == "__main__":
    download_ifrs_final()