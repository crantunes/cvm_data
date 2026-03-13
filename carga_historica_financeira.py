import pandas as pd
import requests
import zipfile
import io
from sqlalchemy import create_engine, text

# Configuração da conexão
DB_URL = "postgresql://pesquisador:sua_senha_forte@localhost:5432/cvm_data"
engine = create_engine(DB_URL)

def obter_cnpjs_cadastrados():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT cnpj_cia FROM empresas"))
        return set(row[0] for row in result)

def carregar_historico_financeiro(anos, tipos_doc=['DFP', 'ITR']):
    grupos = ['BPA', 'BPP', 'DRE']
    cnpjs_validos = obter_cnpjs_cadastrados()
    
    for ano in anos:
        for tipo in tipos_doc:
            url = f"https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/{tipo}/DADOS/{tipo.lower()}_cia_aberta_{ano}.zip"
            print(f"\n--- Processando {tipo} {ano} ---")
            
            try:
                response = requests.get(url, timeout=60)
                response.raise_for_status()
                
                with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                    arquivos_alvo = [f for f in z.namelist() if '_con_' in f and any(g in f for g in grupos)]
                    
                    for file_name in arquivos_alvo:
                        with z.open(file_name) as f:
                            df = pd.read_csv(f, sep=';', encoding='ISO-8859-1')
                            df.columns = [c.strip().lower() for c in df.columns]
                            
                            df_db = pd.DataFrame()
                            df_db['cnpj_cia'] = df.get('cnpj_cia', df.get('cnpj_companhia'))
                            df_db['data_referencia'] = pd.to_datetime(df.get('dt_refer', df.get('data_referencia')))
                            df_db['versao_doc'] = df.get('versao', 1)
                            df_db['tipo_doc'] = tipo
                            
                            for g in grupos:
                                if g in file_name: df_db['grupo_dfp'] = g
                            
                            df_db['moeda'] = df.get('moeda', 'REAL')
                            df_db['escala_moeda'] = df.get('escala_moeda', 'UNIDADE')
                            df_db['ordem_exerc'] = df.get('ordem_exerc', 'ULTIMO')
                            df_db['cd_conta'] = df.get('cd_conta', df.get('codigo_conta'))
                            df_db['ds_conta'] = df.get('ds_conta', df.get('descricao_conta'))
                            df_db['vl_conta'] = pd.to_numeric(df.get('vl_conta', df.get('valor_conta')), errors='coerce')
                            
                            # FILTRO CRÍTICO: Mantém apenas CNPJs que existem na tabela 'empresas'
                            antes = len(df_db)
                            df_db = df_db[df_db['cnpj_cia'].isin(cnpjs_validos)]
                            depois = len(df_db)
                            
                            if antes != depois:
                                print(f"   ! Ignorados {antes - depois} registros de CNPJs não cadastrados.")

                            df_db = df_db.dropna(subset=['cnpj_cia', 'vl_conta'])
                            
                            if not df_db.empty:
                                df_db.to_sql('fatos_contabeis', engine, if_exists='append', index=False, method='multi', chunksize=10000)
                                print(f"   -> Sucesso: {len(df_db)} linhas de {file_name}")
                            
            except Exception as e:
                print(f"Erro em {tipo} {ano}: {e}")

if __name__ == "__main__":
    # Agora você pode expandir o range com segurança
    anos_para_processar = range(2023, 2026) 
    carregar_historico_financeiro(anos_para_processar)