import pandas as pd
from sqlalchemy import create_engine

# 1. Configurações de Acesso
# Substitua 'sua_senha' pela senha do usuário pesquisador
DB_URL = "postgresql://pesquisador:sua_senha_forte@localhost:5432/cvm_data"
FILE_PATH = r"D:\DATACVM\Outros Cadastros\Companhia Aberta\cad_cia_aberta.csv"

def processar_e_popular():
    try:
        print("Iniciando leitura do arquivo...")
        # Lendo com latin1 pois arquivos da CVM geralmente usam esse encoding
        df = pd.read_csv(FILE_PATH, sep=';', encoding='latin1', low_memory=False)

        # 2. Filtro: TP_MERC igual a 'BOLSA' ou vazio 
        # O campo TP_MERC armazena o tipo de mercado [cite: 9]
        df_filtrado = df[(df['TP_MERC'] == 'BOLSA') | (df['TP_MERC'].isna()) | (df['TP_MERC'] == '')].copy()

        # 3. Tratamento de Duplicatas no CNPJ_CIA (Primary Key)
        # Remove duplicatas para evitar erro de 'Violation of Primary Key' no banco
        total_antes = len(df_filtrado)
        df_filtrado = df_filtrado.drop_duplicates(subset=['CNPJ_CIA'], keep='first')
        total_depois = len(df_filtrado)
        df = df['CNPJ_CIA', 'DENOM_SOCIAL', 'DENOM_COMERC', 'DT_REG', 'DT_CONST', 
    'DT_CANCEL', 'MOTIVO_CANCEL', 'SIT', 'DT_INI_SIT', 'CD_CVM', 
    'SETOR_ATIV', 'TP_MERC', 'CATEG_REG', 'DT_INI_CATEG', 'SIT_EMISSOR', 
    'DT_INI_SIT_EMISSOR', 'CONTROLE_ACIONARIO', 'TP_ENDER', 'LOGRADOURO', 
    'COMPL', 'BAIRRO', 'MUN', 'UF', 'PAIS', 'CEP', 'DDD_TEL', 'TEL', 
    'DDD_FAX', 'FAX', 'EMAIL', 'CNPJ_AUDITOR', 'AUDITOR']

        print(f"Filtro aplicado: {total_antes - total_depois} duplicatas removidas.")

        # 4. Conexão e Carga
        engine = create_engine(DB_URL)
        
        # Inserindo no schema cvm_data na tabela cad_cia_aberta
        df_filtrado.to_sql(
            'cad_cia_aberta', 
            engine, 
            schema='cvm_data', 
            if_exists='append', 
            index=False,
            method='multi',
            chunksize=1000
        )
        
        print(f"Sucesso! {total_depois} registros inseridos na tabela cvm_data.cad_cia_aberta.")

    except Exception as e:
        print(f"Erro durante a execução: {e}")

if __name__ == "__main__":
    processar_e_popular()