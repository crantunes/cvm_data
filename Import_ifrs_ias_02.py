import os
import requests
import time

OUTPUT_DIR = "ifrs_originals_en"
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# Lista de normas essenciais para os 42 pares (H1 e H2)
# ias_list = {
#    10:	"events-after-the-reporting-period",
#
#}

# IFRS que costumam compor a amostra de alto julgamento
ifrs_list = {
1:	"first-time-adoption-of-international-financial-reporting-standards",
2:	"share-based-payment",
4:	"insurance-contracts",
5:	"non-current-assets-held-for-sale-and-discontinued-operations",
6:	"exploration-for-and-evaluation-of-mineral-resources",
7:	"financial-instruments:-disclosures",
8:	"operating-segments",
10:	"consolidated-financial-statements",
11:	"joint-arrangements",
12:	"disclosure-of-interests-in-other-entities",
14:	"regulatory-deferral-accounts",
17:	"insurance-contracts"

}

def download_robust(prefix, num, slug):
    # Testamos a URL de 2021 (que você validou) e a de 2023
    years = ["2021", "2023"]
    headers = {"User-Agent": "Mozilla/5.0"}
    
    for year in years:
        url = f"https://www.ifrs.org/content/dam/ifrs/publications/pdf-standards/english/{year}/issued/part-a/{prefix.lower()}-{num}-{slug}.pdf"
        try:
            print(f"Tentando {prefix} {num} ({year})...")
            r = requests.get(url, headers=headers, stream=True, timeout=15)
            if r.status_code == 200:
                path = os.path.join(OUTPUT_DIR, f"{prefix}_{num}_Original_EN.pdf")
                with open(path, 'wb') as f:
                    f.write(r.content)
                print(f"  [OK] Baixado!")
                return True
        except:
            continue
    print(f"  [FALHA] {prefix} {num} não encontrada.")
    return False

if __name__ == "__main__":
   # for n, s in ias_list.items(): download_robust("IAS", n, s)
    for n, s in ifrs_list.items(): download_robust("IFRS", n, s)