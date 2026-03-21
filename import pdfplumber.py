import pdfplumber
import re
import os

def extrair_faixa_preço(caminho_pdf):
    with pdfplumber.open(caminho_pdf) as pdf:
        # Geralmente a faixa está nas primeiras 5 páginas
        texto_inicial = ""
        for i in range(5):
            texto_inicial += pdf.pages[i].extract_text()
        
        # Regex para buscar padrões de valores monetários próximos a "faixa" ou "estimativa"
        # Este padrão busca algo como "R$ 15,00 e R$ 20,00"
        padrao = r"R\$\s?(\d+,\d{2})\s?e\s?R\$\s?(\d+,\d{2})"
        matches = re.findall(padrao, texto_inicial)
        
        if matches:
            return matches[0] # Retorna (min, max)
        return (None, None)

# Exemplo de loop para sua pasta de 98 prospectos
# for arquivo in os.listdir("prospectos/"):
#    print(extrair_faixa_preço(f"prospectos/{arquivo}"))