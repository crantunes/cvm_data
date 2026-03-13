import psycopg2
import spacy
import pandas as pd

# Carregando os "cérebros" de NLP que instalamos
nlp_pt = spacy.load("pt_core_news_lg")
nlp_en = spacy.load("en_core_web_lg")

# Dicionários Deônticos (Rigor de Pesquisa)
TERMOS_PT = ['deve', 'deverá', 'obrigatório', 'exigido', 'necessário', 'impõe']
TERMOS_EN = ['shall', 'must', 'mandatory', 'required', 'necessary', 'imposes']

def calcular_metricas_gold():
    try:
        conn = psycopg2.connect(
            dbname="cvm_data", user="pesquisador", 
            password="sua_senha_forte", host="localhost"
        )
        cur = conn.cursor()

        # Seleciona apenas o que já foi conferido na Camada Prata
        cur.execute("SELECT id, codigo_norma, conteudo_markdown FROM documentos_texto WHERE conferido = TRUE")
        rows = cur.fetchall()

        for id_doc, nome, texto in rows:
            # Seleciona o modelo correto conforme a origem da norma
            is_en = "Original_EN" in nome
            doc = nlp_en(texto.lower()) if is_en else nlp_pt(texto.lower())
            termos_alvo = TERMOS_EN if is_en else TERMOS_PT
            
            # Contagem de tokens (excluindo pontuação e espaços)
            tokens = [t.text for t in doc if not t.is_punct and not t.is_space]
            total_palavras = len(tokens)
            
            # Contagem de termos deônticos
            contagem = sum(1 for t in tokens if t in termos_alvo)
            indice = (contagem / total_palavras) * 1000 if total_palavras > 0 else 0

            # Upsert na tabela de métricas (Camada Ouro)
            cur.execute("""
                INSERT INTO metricas_deonticas (id_documento, total_palavras, contagem_deontica, indice_restritividade)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id_documento) DO UPDATE SET
                total_palavras = EXCLUDED.total_palavras,
                contagem_deontica = EXCLUDED.contagem_deontica,
                indice_restritividade = EXCLUDED.indice_restritividade
            """, (id_doc, total_palavras, contagem, indice))
            
            print(f"Processado: {nome} | IR: {indice:.4f}")

        conn.commit()
        print("--- Camada Ouro: Métricas Deônticas Finalizadas! ---")

    except Exception as e:
        print(f"Erro no processamento Ouro: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    calcular_metricas_gold()