"""
Script : pipeline_prospectos.py
Etapa  : EXTRAÇÃO DE TEXTO — armazenamento no banco (NLP/Culture Score em etapa futura)

Fonte  : D:\\VSCode\\1004_metodos_quantitativos\\prospectos\\
Padrão : {codigo_cvm}_{cnpj14}.pdf   ex: 22500_11395624000171.pdf
Tabela : cvm_data.prospectos_ipo

Seções extraídas por heurística (títulos ICVM 400):
  texto_negocio      → "Descrição das Atividades" / "Visão Geral do Negócio" (cap 7-8)
  texto_risco        → "Fatores de Risco" (cap 4)
  texto_uso_recursos → "Uso dos Recursos" (cap 6)
  texto_integral     → documento completo (sempre extraído)

Instalação:
  pip install pdfplumber pypdf python-dotenv psycopg2-binary

Execução:
  python pipeline_prospectos.py              # todos os PDFs
  python pipeline_prospectos.py --file 22500_11395624000171.pdf  # um arquivo
  python pipeline_prospectos.py --audit      # relatório sem processar
  python pipeline_prospectos.py --reprocess  # reprocessa mesmo os já carregados
"""

import os, re, sys, logging, argparse
import psycopg2
from dotenv import load_dotenv
from pathlib import Path

import pdfplumber           # extração principal (melhor para PDFs BR)
from pypdf import PdfReader  # fallback / metadados

# ─── CONFIGURAÇÕES ────────────────────────────────────────────────────────────
# Busca .env em 3 locais (ordem de prioridade):
#   1. Pasta do próprio script
#   2. D:\VSCode\cvm_data\.env  (localização padrão do projeto)
#   3. Variáveis de ambiente já definidas no sistema
_env_candidatos = [
    Path(__file__).parent / ".env",          # 1. junto ao script
    Path(r"D:\VSCode\cvm_data\.env"),      # 2. pasta principal do projeto
]
for _env_path in _env_candidatos:
    if _env_path.exists():
        load_dotenv(_env_path, override=True)
        break

DB_URL = "postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}".format(**{
    k: os.getenv(k, "") for k in ["DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_NAME"]
})

# Validar que as variáveis foram carregadas
if not os.getenv("DB_USER"):
    print("ERRO: variáveis de banco não encontradas.")
    print("  Crie um arquivo .env na pasta do script ou em D:\\VSCode\\cvm_data\\.env")
    print("  Conteúdo esperado:")
    print("    DB_USER=pesquisador")
    print("    DB_PASSWORD=sua_senha")
    print("    DB_HOST=localhost")
    print("    DB_PORT=5432")
    print("    DB_NAME=cvm_data")
    import sys; sys.exit(1)

PDF_DIR = Path(r"D:\VSCode\1004_metodos_quantitativos\prospectos")

# Padrão do nome do arquivo: {codigo_cvm}_{cnpj14}[.pdf]
PAT_NOME = re.compile(r'^(\d+)_(\d{14})(?:\.pdf)?$', re.IGNORECASE)

# Mínimo de chars para considerar uma seção como "encontrada"
MIN_CHARS_SECAO = 300

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline_prospectos.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ─── PADRÕES DE SEÇÃO (ICVM 400 / RCVM 160) ──────────────────────────────────
# Cada tupla: (padrão_início, padrão_fim, nome_seção)
# O script varre páginas sequencialmente e captura texto entre início e fim.

SECOES = {
    "negocio": (
        re.compile(
            r'(vis[aã]o\s+geral\s+(d[ao]\s+)?neg[oó]cio|'
            r'descri[cç][aã]o\s+(d[ae]s?\s+)?atividade|'
            r'noss[ao]\s+neg[oó]cio|'
            r'sum[aá]rio\s+d[ao]\s+neg[oó]cio|'
            r'vis[aã]o\s+geral\s+d[ao]\s+emiss|'
            r'descri[cç][aã]o\s+d[ao]\s+emiss)',
            re.IGNORECASE
        ),
        re.compile(
            r'(fator(es)?\s+de\s+risco|'
            r'informa[cç][oõ]es\s+financeiras\s+selecionadas|'
            r'discuss[aã]o\s+e\s+an[aá]lise|'
            r'cap[ií]tulo\s+[45]\b)',
            re.IGNORECASE
        ),
    ),
    "risco": (
        re.compile(
            r'(fator(es)?\s+de\s+risco)',
            re.IGNORECASE
        ),
        re.compile(
            r'(uso\s+d[eo]s?\s+recursos|'
            r'destina[cç][aã]o\s+d[eo]s?\s+recursos|'
            r'cap[ií]tulo\s+[56]\b)',
            re.IGNORECASE
        ),
    ),
    "uso_recursos": (
        re.compile(
            r'(uso\s+d[eo]s?\s+recursos|'
            r'destina[cç][aã]o\s+d[eo]s?\s+recursos)',
            re.IGNORECASE
        ),
        re.compile(
            r'(capitaliz[ae][cç][aã]|'
            r'diluição|'
            r'cap[ií]tulo\s+[78]\b)',
            re.IGNORECASE
        ),
    ),
}


def formatar_cnpj(cnpj14: str) -> str:
    c = cnpj14.strip()
    if len(c) == 14:
        return f"{c[:2]}.{c[2:5]}.{c[5:8]}/{c[8:12]}-{c[12:]}"
    return c


def contar_tokens(texto: str) -> int:
    """Contagem rápida de palavras (tokens) — para estatística de cobertura."""
    if not texto:
        return 0
    return len(re.findall(r'\S+', texto))


# ─── EXTRAÇÃO DE TEXTO ────────────────────────────────────────────────────────

def extrair_pdf(path: Path) -> dict:
    """
    Extrai texto integral + seções (negocio, risco, uso_recursos) do PDF.
    Usa pdfplumber como método principal; pypdf como fallback.
    """
    res = {
        "texto_integral"   : "",
        "texto_negocio"    : "",
        "texto_risco"      : "",
        "texto_uso_recursos": "",
        "total_paginas"    : 0,
        "paginas_com_texto": 0,
        "paginas_vazias"   : 0,
        "metodo_extracao"  : "pdfplumber",
        "status"           : "ok",
        "observacao"       : "",
    }

    paginas = []

    # ── Tentativa 1: pdfplumber ───────────────────────────────────────────────
    try:
        with pdfplumber.open(path) as pdf:
            res["total_paginas"] = len(pdf.pages)
            for page in pdf.pages:
                try:
                    txt = page.extract_text() or ""
                    paginas.append(txt)
                    if len(txt.strip()) > 50:
                        res["paginas_com_texto"] += 1
                    else:
                        res["paginas_vazias"] += 1
                except Exception:
                    paginas.append("")
                    res["paginas_vazias"] += 1
    except Exception as e:
        # ── Fallback: pypdf ───────────────────────────────────────────────────
        log.warning(f"    pdfplumber falhou ({e}) — tentando pypdf")
        res["metodo_extracao"] = "pypdf"
        paginas = []
        try:
            reader = PdfReader(str(path))
            res["total_paginas"] = len(reader.pages)
            for page in reader.pages:
                try:
                    txt = page.extract_text() or ""
                    paginas.append(txt)
                    if len(txt.strip()) > 50:
                        res["paginas_com_texto"] += 1
                    else:
                        res["paginas_vazias"] += 1
                except Exception:
                    paginas.append("")
                    res["paginas_vazias"] += 1
        except Exception as e2:
            res["status"]     = "erro_leitura"
            res["observacao"] = str(e2)
            return res

    # ── Texto integral ────────────────────────────────────────────────────────
    res["texto_integral"] = "\n".join(paginas)

    # Qualidade
    if res["total_paginas"] > 0:
        res["qualidade"] = round(
            res["paginas_com_texto"] / res["total_paginas"] * 100, 1
        )
    else:
        res["qualidade"] = 0.0

    # Detectar PDF escaneado
    total_chars = len(res["texto_integral"].strip())
    if total_chars < 500 and res["total_paginas"] > 3:
        res["status"]     = "scaneado"
        res["observacao"] = (
            f"Possível PDF escaneado: {total_chars} chars "
            f"em {res['total_paginas']} páginas. Requer OCR."
        )
        log.warning(f"    ⚠ Scaneado: {total_chars} chars / {res['total_paginas']} págs")
        return res

    # ── Extração de seções ────────────────────────────────────────────────────
    for nome_secao, (pat_ini, pat_fim) in SECOES.items():
        texto_secao = _extrair_secao(paginas, pat_ini, pat_fim)

        # Fallback: se seção não encontrada, marcar na observação
        if len(texto_secao.strip()) < MIN_CHARS_SECAO:
            res["observacao"] += f" [sem_secao:{nome_secao}]"

        res[f"texto_{nome_secao}"] = texto_secao

    return res


def _extrair_secao(paginas: list[str],
                   pat_ini: re.Pattern,
                   pat_fim: re.Pattern) -> str:
    """
    Captura texto entre o padrão de início e o padrão de fim.
    Se não encontrar delimitadores, retorna string vazia.
    """
    em_secao    = False
    buf         = []
    max_paginas = 60  # limite de segurança para não capturar o documento inteiro

    for pag in paginas:
        if not em_secao:
            if pat_ini.search(pag):
                em_secao = True
                buf.append(pag)
        else:
            if pat_fim.search(pag):
                break
            buf.append(pag)
            if len(buf) >= max_paginas:
                break

    return "\n".join(buf)


# ─── BANCO DE DADOS ───────────────────────────────────────────────────────────

def buscar_meta_ipo(conn, cnpj14: str) -> dict:
    """Busca ticker, nome e data de IPO no banco para o CNPJ."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                v.codigo_negociacao,
                c.nome_empresa,
                MIN(o.data_registro_oferta)                             AS data_ipo,
                EXTRACT(YEAR FROM MIN(o.data_registro_oferta))::SMALLINT AS ano_ipo
            FROM cvm_data.cad_cia_aberta            c
            LEFT JOIN cvm_data.cad_valor_mobiliario v
                ON  v.cnpj_companhia = c.cnpj_companhia
                AND v.valor_mobiliario ILIKE '%Ações%'
            LEFT JOIN cvm_data.ipo_oferta_distribuicao o
                ON  REGEXP_REPLACE(o.cnpj_emissor,   '[^0-9]', '', 'g')
                  = REGEXP_REPLACE(c.cnpj_companhia, '[^0-9]', '', 'g')
                AND o.oferta_inicial = 'S'
            WHERE REGEXP_REPLACE(c.cnpj_companhia, '[^0-9]', '', 'g') = %s
            GROUP BY v.codigo_negociacao, c.nome_empresa
            LIMIT 1
        """, (cnpj14,))
        row = cur.fetchone()
    return {
        "ticker"  : row[0] if row else None,
        "nome"    : row[1] if row else None,
        "data_ipo": row[2] if row else None,
        "ano_ipo" : int(row[3]) if row and row[3] else None,
    }


def ja_carregado(conn, cnpj14: str, codigo_cvm: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM cvm_data.prospectos_ipo "
            "WHERE cnpj_cia_14 = %s AND codigo_cvm = %s",
            (cnpj14, codigo_cvm)
        )
        return cur.fetchone() is not None


def upsert_prospecto(conn, d: dict):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO cvm_data.prospectos_ipo (
                codigo_cvm, cnpj_cia, cnpj_cia_14,
                ticker, nome_empresa, data_ipo, ano_ipo,
                nome_arquivo, caminho_arquivo, tamanho_bytes, total_paginas,
                texto_integral, texto_negocio, texto_risco, texto_uso_recursos,
                metodo_extracao, qualidade_extracao,
                paginas_com_texto, paginas_vazias,
                tokens_integral, tokens_negocio, tokens_risco, tokens_uso_recursos,
                status_extracao, observacao, dt_atualizacao
            ) VALUES (
                %(codigo_cvm)s, %(cnpj_cia)s, %(cnpj_cia_14)s,
                %(ticker)s, %(nome_empresa)s, %(data_ipo)s, %(ano_ipo)s,
                %(nome_arquivo)s, %(caminho_arquivo)s, %(tamanho_bytes)s, %(total_paginas)s,
                %(texto_integral)s, %(texto_negocio)s, %(texto_risco)s, %(texto_uso_recursos)s,
                %(metodo_extracao)s, %(qualidade_extracao)s,
                %(paginas_com_texto)s, %(paginas_vazias)s,
                %(tokens_integral)s, %(tokens_negocio)s, %(tokens_risco)s, %(tokens_uso_recursos)s,
                %(status_extracao)s, %(observacao)s, NOW()
            )
            ON CONFLICT (cnpj_cia_14, codigo_cvm) DO UPDATE SET
                ticker              = EXCLUDED.ticker,
                nome_empresa        = EXCLUDED.nome_empresa,
                data_ipo            = EXCLUDED.data_ipo,
                ano_ipo             = EXCLUDED.ano_ipo,
                tamanho_bytes       = EXCLUDED.tamanho_bytes,
                total_paginas       = EXCLUDED.total_paginas,
                texto_integral      = EXCLUDED.texto_integral,
                texto_negocio       = EXCLUDED.texto_negocio,
                texto_risco         = EXCLUDED.texto_risco,
                texto_uso_recursos  = EXCLUDED.texto_uso_recursos,
                metodo_extracao     = EXCLUDED.metodo_extracao,
                qualidade_extracao  = EXCLUDED.qualidade_extracao,
                paginas_com_texto   = EXCLUDED.paginas_com_texto,
                paginas_vazias      = EXCLUDED.paginas_vazias,
                tokens_integral     = EXCLUDED.tokens_integral,
                tokens_negocio      = EXCLUDED.tokens_negocio,
                tokens_risco        = EXCLUDED.tokens_risco,
                tokens_uso_recursos = EXCLUDED.tokens_uso_recursos,
                status_extracao     = EXCLUDED.status_extracao,
                observacao          = EXCLUDED.observacao,
                dt_atualizacao      = NOW()
        """, d)
        conn.commit()


# ─── AUDITORIA ────────────────────────────────────────────────────────────────

def imprimir_audit(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                status_extracao,
                COUNT(*)                           AS n,
                AVG(qualidade_extracao)::NUMERIC(4,1) AS qual_media,
                AVG(tokens_negocio)::INT           AS tok_neg_medio,
                SUM(CASE WHEN tokens_negocio >= 500 THEN 1 ELSE 0 END) AS neg_ok
            FROM cvm_data.prospectos_ipo
            GROUP BY status_extracao ORDER BY n DESC
        """)
        rows = cur.fetchall()

    log.info("\n" + "═" * 68)
    log.info("AUDITORIA — prospectos_ipo")
    log.info(f"  {'Status':<18} {'N':>4} {'Qual%':>7} {'Tok.Neg':>9} {'Neg>=500':>9}")
    log.info("  " + "─" * 52)
    total = 0
    for r in rows:
        log.info(f"  {str(r[0]):<18} {r[1]:>4} {str(r[2] or '-'):>7} "
                 f"{str(r[3] or 0):>9} {str(r[4] or 0):>9}")
        total += r[1]
    log.info(f"\n  Total prospectos no banco: {total}")

    with conn.cursor() as cur:
        cur.execute("""
            SELECT nome_arquivo, status_extracao, tokens_negocio, observacao
            FROM cvm_data.prospectos_ipo
            WHERE status_extracao != 'ok'
               OR tokens_negocio < 500
            ORDER BY status_extracao, tokens_negocio
        """)
        problemas = cur.fetchall()

    if problemas:
        log.info(f"\n  Prospectos com atenção ({len(problemas)}):")
        for p in problemas:
            log.info(f"    {p[0]}  [{p[1]}]  tok_neg={p[2]}  {p[3] or ''}")
    log.info("═" * 68)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Extração de texto dos prospectos IPO")
    parser.add_argument("--file",      help="Processar só este arquivo PDF (nome sem path)")
    parser.add_argument("--audit",     action="store_true", help="Relatório de status")
    parser.add_argument("--reprocess", action="store_true",
                        help="Reprocessar mesmo os já carregados")
    args = parser.parse_args()

    log.info("═" * 70)
    log.info("Pipeline Prospectos IPO — Extração de Texto")
    log.info(f"  Pasta : {PDF_DIR}")
    log.info("═" * 70)

    try:
        conn = psycopg2.connect(DB_URL)
    except Exception as e:
        log.error(f"Falha na conexão: {e}")
        sys.exit(1)

    if args.audit:
        imprimir_audit(conn)
        conn.close()
        return

    # ── Listar PDFs ───────────────────────────────────────────────────────────
    if args.file:
        pdfs = [PDF_DIR / args.file]
    else:
        pdfs = sorted(PDF_DIR.glob("*.pdf")) + sorted(PDF_DIR.glob("*.PDF"))

    if not pdfs:
        log.error(f"Nenhum PDF encontrado em: {PDF_DIR}")
        conn.close()
        sys.exit(1)

    log.info(f"PDFs encontrados: {len(pdfs)}\n")

    stats = {"ok": 0, "scaneado": 0, "erro": 0, "pulado": 0, "sem_match": 0}

    for i, pdf in enumerate(pdfs, 1):
        log.info(f"[{i:3d}/{len(pdfs):3d}] {pdf.name}")

        # ── Parse do nome do arquivo ──────────────────────────────────────────
        m = PAT_NOME.match(pdf.stem if pdf.suffix.lower() == '.pdf' else pdf.name)
        if not m:
            log.warning(f"  ⚠ Nome não corresponde ao padrão "
                        f"{{codigo_cvm}}_{{cnpj14}} — pulando")
            stats["sem_match"] += 1
            continue

        codigo_cvm = m.group(1)
        cnpj14     = m.group(2)
        cnpj_fmt   = formatar_cnpj(cnpj14)

        # ── Verificar se já foi carregado ─────────────────────────────────────
        if not args.reprocess and ja_carregado(conn, cnpj14, codigo_cvm):
            log.info(f"  [PULADO] Já carregado. Use --reprocess para forçar.")
            stats["pulado"] += 1
            continue

        # ── Buscar metadados do IPO ───────────────────────────────────────────
        meta = buscar_meta_ipo(conn, cnpj14)
        log.info(f"  CNPJ: {cnpj_fmt} | Ticker: {meta['ticker']} | "
                 f"Empresa: {(meta['nome'] or 'N/A')[:40]} | IPO: {meta['ano_ipo']}")

        # ── Extrair texto ─────────────────────────────────────────────────────
        try:
            ext = extrair_pdf(pdf)
        except Exception as e:
            log.error(f"  Erro na extração: {e}")
            stats["erro"] += 1
            continue

        tok_int = contar_tokens(ext["texto_integral"])
        tok_neg = contar_tokens(ext["texto_negocio"])
        tok_ris = contar_tokens(ext["texto_risco"])
        tok_uso = contar_tokens(ext["texto_uso_recursos"])

        log.info(f"  Páginas: {ext['total_paginas']} "
                 f"(c/texto={ext['paginas_com_texto']}, "
                 f"vazias={ext['paginas_vazias']}, "
                 f"qual={ext.get('qualidade', 0):.0f}%)")
        log.info(f"  Tokens: integral={tok_int:,} | negócio={tok_neg:,} | "
                 f"risco={tok_ris:,} | uso_rec={tok_uso:,}")
        log.info(f"  Status: {ext['status']}")
        if ext["observacao"]:
            log.info(f"  Obs   : {ext['observacao'][:120]}")

        # ── Salvar no banco ───────────────────────────────────────────────────
        dados = {
            "codigo_cvm"        : codigo_cvm,
            "cnpj_cia"          : cnpj_fmt,
            "cnpj_cia_14"       : cnpj14,
            "ticker"            : meta["ticker"],
            "nome_empresa"      : meta["nome"],
            "data_ipo"          : meta["data_ipo"],
            "ano_ipo"           : meta["ano_ipo"],
            "nome_arquivo"      : pdf.name,
            "caminho_arquivo"   : str(pdf),
            "tamanho_bytes"     : pdf.stat().st_size,
            "total_paginas"     : ext["total_paginas"],
            "texto_integral"    : ext["texto_integral"],
            "texto_negocio"     : ext["texto_negocio"],
            "texto_risco"       : ext["texto_risco"],
            "texto_uso_recursos": ext["texto_uso_recursos"],
            "metodo_extracao"   : ext["metodo_extracao"],
            "qualidade_extracao": ext.get("qualidade", 0.0),
            "paginas_com_texto" : ext["paginas_com_texto"],
            "paginas_vazias"    : ext["paginas_vazias"],
            "tokens_integral"   : tok_int,
            "tokens_negocio"    : tok_neg,
            "tokens_risco"      : tok_ris,
            "tokens_uso_recursos": tok_uso,
            "status_extracao"   : ext["status"],
            "observacao"        : ext["observacao"],
        }

        try:
            upsert_prospecto(conn, dados)
            log.info(f"  ✔ Salvo no banco.")
            if ext["status"] == "scaneado":
                stats["scaneado"] += 1
            else:
                stats["ok"] += 1
        except Exception as e:
            conn.rollback()
            log.error(f"  Erro no INSERT: {e}")
            stats["erro"] += 1

    conn.close()

    log.info("\n" + "═" * 70)
    log.info(f"✔ OK              : {stats['ok']}")
    log.info(f"⚠ Scaneados (OCR) : {stats['scaneado']}")
    log.info(f"⚠ Sem match nome  : {stats['sem_match']}")
    log.info(f"  Pulados (já OK)  : {stats['pulado']}")
    log.info(f"✗ Erros           : {stats['erro']}")
    log.info("═" * 70)
    log.info("\nPróximos passos:")
    log.info("  python pipeline_prospectos.py --audit")
    log.info("  SELECT * FROM cvm_data.vw_prospectos_status ORDER BY flag_negocio DESC;")


if __name__ == "__main__":
    main()