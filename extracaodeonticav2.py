import psycopg2
import spacy

# Carregando modelos Large para precisão de PhD
nlp_pt = spacy.load("pt_core_news_lg")
nlp_en = spacy.load("en_core_web_lg")

# Dicionário Deôntico Segregado (Capítulo 3.2.2 do seu artigo)
DICIONARIO = {
    'PT': {
        'forte': ['deve', 'devem', 'deverá', 'deverão', 'obrigado', 'necessário', 'requerido'],
        'moderada': ['deveria', 'deveriam', 'esperado', 'recomendado'],
        'proibicao': ['não deve', 'não devem', 'não deverá', 'proibido', 'não pode'],
        'permissao': ['pode', 'podem', 'poderá', 'poderão', 'permitido']
    },
    'EN': {
        'forte': ['shall', 'must', 'required', 'obligated', 'necessary'],
        'moderada': ['should', 'ought', 'expected', 'recommended'],
        'proibicao': ['shall not', 'must not', 'prohibited', 'may not', 'cannot'],
        'permissao': ['may', 'can', 'permitted', 'allowed']
    }
}

def processar_deontica_v2():
    conn = psycopg2.connect(dbname="cvm_data", user="pesquisador", password="sua_senha_forte", host="localhost")
    cur = conn.cursor()

    cur.execute("SELECT id, codigo_norma, conteudo_markdown FROM documentos_texto")
    normas = cur.fetchall()

    for id_doc, nome, texto in normas:
        is_en = "Original_EN" in nome
        lang = 'EN' if is_en else 'PT'
        doc = (nlp_en if is_en else nlp_pt)(texto.lower())
        
        # 1. Limpeza Seletiva: Remove stopwords, MAS preserva os marcadores deônticos
        termos_deonticos_all = [item for sublist in DICIONARIO[lang].values() for item in sublist]
        tokens_limpos = [t.text for t in doc if (not t.is_stop or t.text in termos_deonticos_all) 
                         and not t.is_punct and not t.is_space]
        
        wi = len(tokens_limpos) # Contagem Wi conforme Etapa 3 do seu artigo
        
        # 2. Contagem Segregada (incluindo busca por n-grams básicos)
        counts = {cat: 0 for cat in DICIONARIO[lang].keys()}
        texto_limpo_unido = " ".join(tokens_limpos)
        
        for cat, termos in DICIONARIO[lang].items():
            for termo in termos:
                counts[cat] += texto_limpo_unido.count(termo)

        di = sum(counts.values())
        ir = (di / wi) * 1000 if wi > 0 else 0
        
        # 3. Identificação de Variáveis Independentes
        origem = 1 if not is_en else 0
        # Classificação de Julgamento baseada na sua lista (ex: IFRS 13/CPC 46 = 1)
        julgamento = 1 if any(x in nome for x in ['CPC_46', 'IFRS_13', 'CPC_01', 'IAS_36', 'CPC_47', 'IFRS_15']) else 0

        cur.execute("""
            INSERT INTO metricas_deonticas 
            (id_documento, total_palavras_limpas, forte, moderada, proibicao, permissao, 
             indice_restritividade, origem_cpc, julgamento_alto)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (id_doc, wi, counts['forte'], counts['moderada'], counts['proibicao'], counts['permissao'], ir, origem, julgamento))

    conn.commit()
    cur.close()
    conn.close()
    print("🚀 Camada Ouro Refinada com Sucesso!")

if __name__ == "__main__":
    processar_deontica_v2()