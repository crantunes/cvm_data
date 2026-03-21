"""
Script : culture_score_simples.py
Método : Contagem bruta de palavras-chave por dimensão cultural (Li et al. 2021b)
         Normalizada pelo total de palavras do documento (TF puro)

Dimensões:
  innovation  → inovação / inovar / inovador / inovadora / innovation / innovative
  integrity   → integridade / ético / ética / integrity / compliance / transparência
  quality     → qualidade / excelência / quality / padrão / certificação
  respect     → respeito / diversidade / sustentabilidade / esg / respect
  teamwork    → equipe / colaboração / parceria / teamwork / sinergia

Saída:
  - INSERT em cvm_data.prospectos_ipo (texto_integral) se tabela existir
  - INSERT em cvm_data.prospecto_culture_score
  - CSV local: culture_scores.csv  (backup independente do banco)

Execução:
  python culture_score_simples.py
  python culture_score_simples.py --csv-only   # só gera CSV sem banco
  python culture_score_simples.py --audit      # mostra resultados já no banco
"""

import os, re, sys, csv, logging, argparse
from pathlib import Path
from collections import defaultdict

import pdfplumber
from pypdf import PdfReader

# dotenv com fallback de caminho
from dotenv import load_dotenv
_envs = [Path(__file__).parent / ".env", Path(r"D:\VSCode\cvm_data\.env")]
for _e in _envs:
    if _e.exists():
        load_dotenv(_e, override=True)
        break

import psycopg2

# ─── CONFIGURAÇÕES ────────────────────────────────────────────────────────────
DB_URL  = "postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}".format(**{
    k: os.getenv(k, "") for k in ["DB_USER","DB_PASSWORD","DB_HOST","DB_PORT","DB_NAME"]
})
PDF_DIR = Path(r"D:\VSCode\1004_metodos_quantitativos\prospectos")
CSV_OUT = Path(__file__).parent / "culture_scores.csv"

PAT_NOME = re.compile(r'^(\d+)_(\d{14})$', re.IGNORECASE)

# ─── DICIONÁRIO DE TERMOS (PT + EN) ──────────────────────────────────────────
# Cada dimensão: lista de termos exatos (após lowercase + remoção de acentos)
# Variantes morfológicas simples incluídas diretamente (sem lematizador)

TERMOS = {
    "innovation": [
        # Português
        "inovacao", "inovar", "inovador", "inovadora", "inovadores", "inovadoras",
        "inovacoes", "inovativo", "inovativa", "disruptivo", "disruptiva",
        "disrupcao", "criatividade", "criativo", "criativa", "pioneiro", "pioneira",
        "empreendedor", "empreendedora", "empreendedorismo", "startup",
        "pesquisa e desenvolvimento", "p&d",
        # Inglês
        "innovation", "innovative", "innovate", "innovator", "disruptive",
        "creativity", "creative", "pioneer", "entrepreneurship",
    ],
    "integrity": [
        # Português
        "integridade", "integro", "integra", "etica", "etico", "etica corporativa",
        "transparencia", "transparente", "compliance", "conformidade",
        "governanca", "governanca corporativa", "anticorrupcao", "antissuborno",
        "codigo de conduta", "boas praticas", "responsabilidade",
        "honestidade", "honesto", "honesta", "confianca", "confiavel",
        # Inglês
        "integrity", "ethics", "ethical", "transparency", "transparent",
        "governance", "compliance", "honesty", "trust", "trustworthy",
    ],
    "quality": [
        # Português
        "qualidade", "excelencia", "excelente", "padrao", "padroes",
        "certificacao", "certificado", "iso", "melhoria continua",
        "controle de qualidade", "garantia de qualidade", "rastreabilidade",
        "confiabilidade", "eficiencia", "eficiente", "satisfacao do cliente",
        # Inglês
        "quality", "excellence", "excellent", "standard", "certification",
        "continuous improvement", "quality control", "reliability", "efficiency",
    ],
    "respect": [
        # Português
        "respeito", "respeitar", "diversidade", "inclusao", "inclusivo", "inclusiva",
        "bem estar", "saude e seguranca", "seguranca do trabalho",
        "responsabilidade social", "meio ambiente", "ambiental",
        "sustentabilidade", "sustentavel", "esg", "direitos humanos",
        "comunidade", "stakeholders", "partes interessadas",
        # Inglês
        "respect", "diversity", "inclusion", "inclusive", "wellbeing",
        "health and safety", "social responsibility", "environmental",
        "sustainability", "sustainable", "human rights", "community",
    ],
    "teamwork": [
        # Português
        "trabalho em equipe", "equipe", "equipes", "colaboracao", "colaborativo",
        "colaborativa", "parceria", "parcerias", "cooperacao", "cooperar",
        "sinergia", "integracao", "engajamento", "comprometimento",
        "lideranca", "lider", "alinhamento", "cultura organizacional",
        # Inglês
        "teamwork", "team", "teams", "collaboration", "collaborative",
        "partnership", "cooperation", "synergy", "integration", "engagement",
        "leadership", "leader", "alignment", "organizational culture",
    ],
}

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("culture_score_simples.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ─── UTILITÁRIOS ──────────────────────────────────────────────────────────────

def remover_acentos(texto: str) -> str:
    """Remove acentos mantendo letras base."""
    import unicodedata
    return unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")


def preprocessar(texto: str) -> str:
    """Lowercase + remoção de acentos + normalização de espaços."""
    t = texto.lower()
    t = remover_acentos(t)
    t = re.sub(r'\s+', ' ', t)
    return t


def contar_termos(texto_prep: str) -> dict:
    """
    Conta ocorrências de cada termo por dimensão.
    Retorna dict com contagens brutas e scores normalizados.
    """
    # Total de palavras (tokens simples) para normalização
    palavras = re.findall(r'\b[a-z]{2,}\b', texto_prep)
    total_palavras = max(len(palavras), 1)

    contagens = {}
    scores    = {}

    for dimensao, termos in TERMOS.items():
        count = 0
        for termo in termos:
            # Busca exata por boundary de palavra para termos simples
            # Busca substring para termos compostos (ex: "trabalho em equipe")
            if ' ' in termo:
                count += texto_prep.count(termo)
            else:
                count += len(re.findall(rf'\b{re.escape(termo)}\b', texto_prep))
        contagens[f"count_{dimensao}"] = count
        scores[f"score_{dimensao}"]    = round(count / total_palavras, 8)

    # Culture score = média simples dos 5 scores normalizados
    vals = [scores[f"score_{d}"] for d in TERMOS]
    scores["culture_score"]           = round(sum(vals) / len(vals), 8)
    scores["total_palavras"]          = total_palavras
    scores.update(contagens)
    return scores


# ─── EXTRAÇÃO DE TEXTO ────────────────────────────────────────────────────────

def extrair_texto(path: Path) -> str:
    """Extrai texto completo do PDF. Tenta pdfplumber, fallback pypdf."""
    partes = []
    try:
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                try:
                    t = page.extract_text() or ""
                    partes.append(t)
                except Exception:
                    pass
    except Exception:
        try:
            reader = PdfReader(str(path))
            for page in reader.pages:
                try:
                    partes.append(page.extract_text() or "")
                except Exception:
                    pass
        except Exception as e:
            log.error(f"  Falha na leitura do PDF: {e}")
            return ""
    return "\n".join(partes)


def formatar_cnpj(cnpj14: str) -> str:
    c = cnpj14.strip()
    return f"{c[:2]}.{c[2:5]}.{c[5:8]}/{c[8:12]}-{c[12:]}" if len(c) == 14 else c


# ─── BANCO DE DADOS ───────────────────────────────────────────────────────────

DDL_SCORE = """
CREATE TABLE IF NOT EXISTS cvm_data.prospecto_culture_score (
    id_score            SERIAL      PRIMARY KEY,
    codigo_cvm          VARCHAR(20) NOT NULL,
    cnpj_cia_14         CHAR(14)    NOT NULL,
    cnpj_cia            VARCHAR(20),
    nome_arquivo        VARCHAR(500),
    total_palavras      INT,
    count_innovation    INT,
    count_integrity     INT,
    count_quality       INT,
    count_respect       INT,
    count_teamwork      INT,
    score_innovation    NUMERIC(12,8),
    score_integrity     NUMERIC(12,8),
    score_quality       NUMERIC(12,8),
    score_respect       NUMERIC(12,8),
    score_teamwork      NUMERIC(12,8),
    culture_score       NUMERIC(12,8),
    status              VARCHAR(20) DEFAULT 'ok',
    dt_calculo          TIMESTAMP   DEFAULT NOW(),
    CONSTRAINT uq_score_cnpj UNIQUE (cnpj_cia_14, codigo_cvm)
);
"""

SQL_UPSERT = """
INSERT INTO cvm_data.prospecto_culture_score (
    codigo_cvm, cnpj_cia_14, cnpj_cia, nome_arquivo, total_palavras,
    count_innovation, count_integrity, count_quality, count_respect, count_teamwork,
    score_innovation, score_integrity, score_quality, score_respect, score_teamwork,
    culture_score, status, dt_calculo
) VALUES (
    %(codigo_cvm)s, %(cnpj_cia_14)s, %(cnpj_cia)s, %(nome_arquivo)s, %(total_palavras)s,
    %(count_innovation)s, %(count_integrity)s, %(count_quality)s,
    %(count_respect)s, %(count_teamwork)s,
    %(score_innovation)s, %(score_integrity)s, %(score_quality)s,
    %(score_respect)s, %(score_teamwork)s,
    %(culture_score)s, %(status)s, NOW()
)
ON CONFLICT (cnpj_cia_14, codigo_cvm) DO UPDATE SET
    total_palavras   = EXCLUDED.total_palavras,
    count_innovation = EXCLUDED.count_innovation,
    count_integrity  = EXCLUDED.count_integrity,
    count_quality    = EXCLUDED.count_quality,
    count_respect    = EXCLUDED.count_respect,
    count_teamwork   = EXCLUDED.count_teamwork,
    score_innovation = EXCLUDED.score_innovation,
    score_integrity  = EXCLUDED.score_integrity,
    score_quality    = EXCLUDED.score_quality,
    score_respect    = EXCLUDED.score_respect,
    score_teamwork   = EXCLUDED.score_teamwork,
    culture_score    = EXCLUDED.culture_score,
    status           = EXCLUDED.status,
    dt_calculo       = NOW()
"""

SQL_AUDIT = """
SELECT codigo_cvm, cnpj_cia, nome_arquivo,
       total_palavras,
       count_innovation, count_integrity, count_quality, count_respect, count_teamwork,
       ROUND(culture_score::NUMERIC, 6) AS culture_score,
       status
FROM cvm_data.prospecto_culture_score
ORDER BY culture_score DESC
"""


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv-only", action="store_true",
                        help="Só gera CSV, sem gravar no banco")
    parser.add_argument("--audit",    action="store_true",
                        help="Exibe resultados já no banco")
    args = parser.parse_args()

    # ── Conexão ───────────────────────────────────────────────────────────────
    conn = None
    if not args.csv_only:
        try:
            conn = psycopg2.connect(DB_URL)
            with conn.cursor() as cur:
                cur.execute(DDL_SCORE)
            conn.commit()
            log.info("Conexão OK — tabela prospecto_culture_score pronta.")
        except Exception as e:
            log.warning(f"Banco indisponível ({e}) — modo CSV apenas.")
            conn = None

    if args.audit and conn:
        with conn.cursor() as cur:
            cur.execute(SQL_AUDIT)
            rows = cur.fetchall()
        print(f"\n{'CNPJ':<20} {'Arquivo':<35} {'Palavras':>8} "
              f"{'Inov':>6} {'Integ':>6} {'Qual':>6} {'Resp':>6} {'Team':>6} "
              f"{'Score':>10}")
        print("-" * 110)
        for r in rows:
            print(f"{str(r[1]):<20} {str(r[2]):<35} {r[3]:>8,} "
                  f"{r[4]:>6} {r[5]:>6} {r[6]:>6} {r[7]:>6} {r[8]:>6} "
                  f"{float(r[9]):>10.6f}")
        conn.close()
        return

    # ── Listar PDFs ───────────────────────────────────────────────────────────
    pdfs = sorted(PDF_DIR.glob("*.pdf")) + sorted(PDF_DIR.glob("*.PDF"))
    if not pdfs:
        log.error(f"Nenhum PDF encontrado em: {PDF_DIR}")
        sys.exit(1)

    log.info(f"PDFs encontrados: {len(pdfs)}")
    log.info("=" * 70)

    resultados = []
    erros = 0

    for i, pdf in enumerate(pdfs, 1):
        # Parse do nome
        stem = pdf.stem
        m    = PAT_NOME.match(stem)
        if not m:
            log.warning(f"[{i:3d}] {pdf.name} — nome fora do padrão, pulando")
            erros += 1
            continue

        codigo_cvm = m.group(1)
        cnpj14     = m.group(2)
        cnpj_fmt   = formatar_cnpj(cnpj14)

        log.info(f"[{i:3d}/{len(pdfs)}] {pdf.name}  ({pdf.stat().st_size/1024:.0f} KB)")

        # Extração
        texto_raw  = extrair_texto(pdf)
        texto_prep = preprocessar(texto_raw)

        if len(texto_prep.strip()) < 200:
            log.warning(f"  ⚠ Texto insuficiente — possível PDF escaneado")
            scores = {k: 0 for k in
                      ["count_innovation","count_integrity","count_quality",
                       "count_respect","count_teamwork",
                       "score_innovation","score_integrity","score_quality",
                       "score_respect","score_teamwork","culture_score","total_palavras"]}
            scores["status"] = "scaneado"
        else:
            scores = contar_termos(texto_prep)
            scores["status"] = "ok"

        log.info(
            f"  words={scores['total_palavras']:,} | "
            f"inov={scores['count_innovation']} "
            f"integ={scores['count_integrity']} "
            f"qual={scores['count_quality']} "
            f"resp={scores['count_respect']} "
            f"team={scores['count_teamwork']} | "
            f"score={scores['culture_score']:.6f}"
        )

        row = {
            "codigo_cvm"      : codigo_cvm,
            "cnpj_cia_14"     : cnpj14,
            "cnpj_cia"        : cnpj_fmt,
            "nome_arquivo"    : pdf.name,
            **scores,
        }
        resultados.append(row)

        # Gravar no banco
        if conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(SQL_UPSERT, row)
                conn.commit()
            except Exception as e:
                conn.rollback()
                log.error(f"  Erro INSERT: {e}")

    # ── CSV de saída ──────────────────────────────────────────────────────────
    if resultados:
        campos = [
            "codigo_cvm", "cnpj_cia_14", "cnpj_cia", "nome_arquivo",
            "total_palavras",
            "count_innovation", "count_integrity", "count_quality",
            "count_respect", "count_teamwork",
            "score_innovation", "score_integrity", "score_quality",
            "score_respect", "score_teamwork",
            "culture_score", "status",
        ]
        with open(CSV_OUT, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=campos, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(resultados)
        log.info(f"\n✔ CSV salvo em: {CSV_OUT}")

    if conn:
        conn.close()

    log.info("=" * 70)
    log.info(f"✔ Processados: {len(resultados)}  |  Erros/pulados: {erros}")
    log.info("=" * 70)


if __name__ == "__main__":
    main()