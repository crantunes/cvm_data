import psycopg2
import spacy
from sklearn.feature_extraction.text import TfidfVectorizer
import pandas as pd

# Carregando o modelo pesado para português
nlp = spacy.load("pt_core_news_lg")

# Dicionário de Li et al. (2021b) - Versão adaptada PT-BR
dicionario_cultura = {
    'innovation': ['inovação', 'tecnologia', 'pesquisa', 'desenvolvimento', 'digital'],
    'integrity': ['ética', 'integridade', 'compliance', 'transparência', 'governança'],
    'quality': ['qualidade', 'excelência', 'eficiência', 'padrão', 'certificação'],
    'respect': ['respeito', 'diversidade', 'inclusão', 'sustentabilidade', 'social'],
    'teamwork': ['equipe', 'time', 'colaboração', 'parceria', 'sinergia']
}

def gerar_culture_score_gold():
    try:
        conn = psycopg2.connect(dbname="cvm_data", user="pesquisador", password="sua_senha_forte", host="localhost")
        cur = conn.cursor()

        # 1. Extração dos Prospectos (Camada Prata)
        cur.execute("SELECT id, cnpj_cia, conteudo_markdown FROM documentos_texto WHERE tipo_documento = 'PROSPECTO'")
        docs = cur.fetchall()
        
        if not docs:
            print("⚠️ Nenhum prospecto encontrado. Verifique a carga do Grupo 1.")
            return

        # 2. Processamento TF-IDF (Rigor de PhD)
        # O TF-IDF garante que palavras comuns não 'inflem' o escore de cultura
        corpus = [d[2] for d in docs]
        vectorizer = TfidfVectorizer(stop_words=None) # Stopwords tratadas pelo spacy
        tfidf_matrix = vectorizer.fit_transform(corpus)
        feature_names = vectorizer.get_feature_names_out()

        for i, (id_doc, cnpj, _) in enumerate(docs):
            escores = {dim: 0.0 for dim in dicionario_cultura}
            
            # Calculando o peso de cada dimensão no documento atual
            for dim, palavras in dicionario_cultura.items():
                for palavra in palavras:
                    if palavra in feature_names:
                        idx = list(feature_names).index(palavra)
                        escores[dim] += tfidf_matrix[i, idx]

            # 3. Persistência na Camada Ouro
            cur.execute("""
                INSERT INTO scores_cultura (id_documento, innovation, integrity, quality, respect, teamwork)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id_documento) DO UPDATE SET
                innovation = EXCLUDED.innovation, integrity = EXCLUDED.integrity
            """, (id_doc, escores['innovation'], escores['integrity'], escores['quality'], escores['respect'], escores['teamwork']))

        conn.commit()
        print("--- 🏆 Camada Ouro: Culture Scores gerados com sucesso! ---")

    except Exception as e:
        print(f"Erro no processamento de Cultura: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    gerar_culture_score_gold()