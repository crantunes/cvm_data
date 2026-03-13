import pandas as pd
import requests
import zipfile
import io
from sqlalchemy import create_engine

# R1 Headers para evitar bloqueios
HEADERS = {'User-Agent': 'Mozilla/5.0'}

def capturar_dados_cvm_v3():
    engine = create_engine('postgresql+psycopg2://pesquisador:sua_senha@localhost:5432/cvm_data')
    
    # Período necessário para Dechow & Dichev (2002)
    for ano in range(2010, 2024):
        print(f"🚀 Localizando repositório de {ano}...")
        
        # A CVM alterna entre pastas 'DADOS' e a raiz do ano
        urls_tentativa = [
            f"https://dados.cvm.gov.br/dados/CIA_ABERTA/DRE/DADOS/dre_cia_aberta_{ano}.zip",
            f"https://dados.cvm.gov.br/dados/CIA_ABERTA/DRE/DADOS/dre_cia_aberta_con_{ano}.zip"
        ]
        
        sucesso = False
        for url in urls_tentativa:
            try:
                r = requests.get(url, headers=HEADERS, timeout=20)
                if r.status_code == 200:
                    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                        # Processamento R1: Localiza o arquivo consolidado dentro do ZIP
                        file_name = [name for name in z.namelist() if f"con_{ano}" in name.lower()][0]
                        with z.open(file_name) as f:
                            df = pd.read_csv(f, sep=';', encoding='ISO-8859-1')
                            # Filtro: Lucro Líquido (3.11 ou 3.09)
                            lucro = df[df['CD_CONTA'].isin(['3.11', '3.09'])]
                            lucro.to_sql('dados_dre_brutos', engine, if_exists='append', index=False)
                    print(f"✅ Dados de {ano} capturados via {url}")
                    sucesso = True
                    break
            except Exception as e:
                continue
        
        if not sucesso:
            print(f"❌ Falha crítica em {ano}: Arquivo não localizado no servidor.")

if __name__ == "__main__":
    capturar_dados_cvm_v3()