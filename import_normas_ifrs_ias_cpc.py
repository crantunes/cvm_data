import os
import psycopg2
from markitdown import MarkItDown

# Inicializa o conversor da Microsoft
md = MarkItDown()

def popular_normas_gold():
    try:
        # Conexão com seu banco no Docker (via Windows)
        conn = psycopg2.connect(
            dbname="cvm_data", 
            user="pesquisador", 
            password="sua_senha_forte", 
            host="localhost", 
            port="5432"
        )
        cur = conn.cursor()

        # Caminho da sua pasta de normas (ajuste para o seu caminho no Windows)
        pasta_normas = r'D:\VSCode\cvm_data\normas_ifrs_ias_cpc_pdf'
        
        print("--- Iniciando Processamento Medalhão: Camada Prata ---")

        for arquivo in os.listdir(pasta_normas):
            if arquivo.endswith(".pdf"):
                caminho_completo = os.path.join(pasta_normas, arquivo)
                nome_norma = arquivo.replace('.pdf', '')
                
                print(f"Convertendo {nome_norma}...")
                
                # Bronze -> Prata: Conversão para Markdown
                # O MarkItDown preserva a hierarquia, vital para o Índice de Restritividade
                resultado = md.convert(caminho_completo)
                conteudo_limpo = resultado.text_content
                
                # Inserção com a flag de auditoria
                cur.execute("""
                    INSERT INTO documentos_texto 
                    (tipo_documento, codigo_norma, conteudo_markdown, conferido, idioma)
                    VALUES (%s, %s, %s, %s, %s)
                """, ('NORMA_CONTABIL', nome_norma, conteudo_limpo, True, 'PT'))
                
        conn.commit()
        print("--- Carga da Camada Prata Concluída com Sucesso! ---")

    except Exception as e:
        print(f"Erro na carga: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    popular_normas_gold()