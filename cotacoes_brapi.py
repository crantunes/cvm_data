"""
Script : load_cotacoes_brapi.py
Tabela : cvm_data.cotacoes_diarias
Fonte  : brapi.dev — GET /api/quote/{ticker}?range=max&interval=1d
         Documentação: https://brapi.dev/docs

Variáveis do paper:
  First-Day Return  → view vw_first_day_return  (DDL nefin_brapi.sql)
  Post-IPO Return   → view vw_post_ipo_return
  Avg. First-Day Return → view vw_avg_first_day_return

Escopo:
  Apenas tickers das empresas com IPO na amostra (oferta_inicial = 'S',
  tipo_ativo ACOES), obtidos via query no banco antes da carga.

Estratégia:
  • range=max → série histórica completa do ticker (desde listagem na B3)
  • INSERT ... ON CONFLICT (ticker, data) DO NOTHING
    (histórico não muda; adj_close só muda em splits futuros)
  • Retry automático em erros 429 (rate limit) e 5xx
  • Salva cada ticker em cache local JSON para reuso sem re-download

Configuração no .env:
  BRAPI_TOKEN=seu_token_aqui

Execução:
  python load_cotacoes_brapi.py                    # todos os tickers da amostra
  python load_cotacoes_brapi.py --ticker WEGE3     # um ticker específico
  python load_cotacoes_brapi.py --force            # re-baixa mesmo com cache
  python load_cotacoes_brapi.py --dry-run          # mostra tickers sem inserir

Plano brapi.dev necessário:
  • Histórico completo (range=max) requer plano pago (Startup ou Pro)
  • Plano gratuito: últimos 12 meses (range=1y) — suficiente para IPOs recentes
  • IPOs históricos (2004–2020): plano Pro (série 10+ anos)
"""

import os
import sys
import json
import time
import logging
import argparse
import datetime
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from pathlib import Path

# ─── CONFIGURAÇÕES ────────────────────────────────────────────────────────────
load_dotenv()
DB_URL = "postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}".format(**{
    k: os.getenv(k, "") for k in ["DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_NAME"]
})

BRAPI_TOKEN   = os.getenv("BRAPI_TOKEN", "")
BRAPI_BASE    = "https://brapi.dev/api/quote"
CACHE_DIR     = Path(r"D:\DATACVM\brapi_cache")  # cache local JSON por ticker
BATCH_SIZE    = 1000
SLEEP_BETWEEN = 0.5   # segundos entre requests (respeitar rate limit)
MAX_RETRIES   = 3
RETRY_WAIT    = 10    # segundos de espera em 429/5xx

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("load_cotacoes_brapi.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ─── QUERY: tickers de IPO da amostra ─────────────────────────────────────────

QUERY_TICKERS_IPO = """
    SELECT DISTINCT
        v.codigo_negociacao                         AS ticker,
        o.cnpj_emissor,
        MIN(o.data_registro_oferta)                 AS data_ipo
    FROM cvm_data.ipo_oferta_distribuicao   o
    JOIN cvm_data.cad_valor_mobiliario      v
        -- Normaliza CNPJ removendo pontuacao antes de comparar
        ON REGEXP_REPLACE(v.cnpj_companhia, '[^0-9]', '', 'g')
         = REGEXP_REPLACE(o.cnpj_emissor,   '[^0-9]', '', 'g')
        AND v.valor_mobiliario ILIKE '%AÇÃO%'
    WHERE o.oferta_inicial = 'S'
      AND v.codigo_negociacao IS NOT NULL
      AND TRIM(v.codigo_negociacao) <> ''
    GROUP BY v.codigo_negociacao, o.cnpj_emissor
    ORDER BY data_ipo
"""


def get_tickers_ipo(conn) -> pd.DataFrame:
    df = pd.read_sql(QUERY_TICKERS_IPO, conn)
    log.info(f"  Tickers IPO da amostra: {len(df):,}")
    return df


# ─── BRAPI API ────────────────────────────────────────────────────────────────

def build_headers() -> dict:
    h = {"Content-Type": "application/json"}
    if BRAPI_TOKEN:
        h["Authorization"] = f"Bearer {BRAPI_TOKEN}"
    return h


def fetch_ticker(ticker: str, force: bool = False) -> list[dict] | None:
    """
    Retorna lista de registros históricos para o ticker.
    Cada registro: {date(unix_s), open, high, low, close, volume, adjustedClose}
    Usa cache local JSON se disponível e force=False.
    """
    cache_path = CACHE_DIR / f"{ticker}.json"

    if not force and cache_path.exists():
        age_hours = (time.time() - cache_path.stat().st_mtime) / 3600
        if age_hours < 24:
            log.info(f"    [CACHE] {ticker} ({cache_path.stat().st_size / 1024:.1f} KB, {age_hours:.0f}h atrás)")
            with open(cache_path, encoding="utf-8") as f:
                return json.load(f)
        else:
            log.info(f"    [CACHE EXPIRADO] {ticker} ({age_hours:.0f}h) — rebaixando")

    url = f"{BRAPI_BASE}/{ticker}"
    params = {
        "range"   : "max",
        "interval": "1d",
    }
    if BRAPI_TOKEN:
        params["token"] = BRAPI_TOKEN

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, headers=build_headers(), timeout=30)

            if r.status_code == 429:
                wait = RETRY_WAIT * attempt
                log.warning(f"    Rate limit (429). Aguardando {wait}s...")
                time.sleep(wait)
                continue

            if r.status_code == 404:
                log.warning(f"    Ticker não encontrado na brapi.dev: {ticker}")
                return None

            if r.status_code == 402:
                log.error(f"    Plano insuficiente (402) para {ticker} — requer upgrade")
                return None

            r.raise_for_status()
            data = r.json()

            results = data.get("results", [])
            if not results:
                log.warning(f"    Sem dados para {ticker}")
                return None

            historical = results[0].get("historicalDataPrice", [])
            if not historical:
                log.warning(f"    Sem histórico para {ticker}")
                return None

            # Salvar cache
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(historical, f)

            log.info(f"    OK {ticker}: {len(historical):,} dias | último: {results[0].get('regularMarketTime','?')[:10]}")
            return historical

        except requests.RequestException as e:
            if attempt < MAX_RETRIES:
                log.warning(f"    Erro request (tentativa {attempt}/{MAX_RETRIES}): {e}. Aguardando {RETRY_WAIT}s...")
                time.sleep(RETRY_WAIT)
            else:
                log.error(f"    Falha após {MAX_RETRIES} tentativas para {ticker}: {e}")
                return None

    return None


def parse_historical(ticker: str, records: list[dict]) -> pd.DataFrame:
    """
    Converte lista de dicts da brapi.dev em DataFrame pronto para INSERT.
    Campo date é Unix timestamp em segundos.
    """
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # Converter Unix timestamp → date
    df["data"] = pd.to_datetime(df["date"], unit="s", utc=True).dt.tz_convert("America/Sao_Paulo").dt.date

    # Renomear campos
    rename = {
        "open"         : "open",
        "high"         : "high",
        "low"          : "low",
        "close"        : "close",
        "volume"       : "volume",
        "adjustedClose": "adj_close",
    }
    df = df.rename(columns=rename)

    # Garantir colunas necessárias
    for col in ["open", "high", "low", "close", "volume", "adj_close"]:
        if col not in df.columns:
            df[col] = None

    df["ticker"] = ticker
    df["fonte"]  = "brapi"

    # Converter types
    for col in ["open", "high", "low", "close", "adj_close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")

    # Filtrar linhas sem data
    df = df.dropna(subset=["data"])

    return df[["ticker", "data", "open", "high", "low", "close", "volume", "adj_close", "fonte"]]


def insert_cotacoes(conn, df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    query = """
        INSERT INTO cvm_data.cotacoes_diarias
            (ticker, data, open, high, low, close, volume, adj_close, fonte)
        VALUES %s
        ON CONFLICT (ticker, data) DO NOTHING
    """
    cols = ["ticker", "data", "open", "high", "low", "close", "volume", "adj_close", "fonte"]
    total    = len(df)
    inserted = 0

    with conn.cursor() as cur:
        for start in range(0, total, BATCH_SIZE):
            batch = df.iloc[start: start + BATCH_SIZE][cols]
            records = []
            for row in batch.itertuples(index=False, name=None):
                clean = []
                for v in row:
                    if v is pd.NaT or v is None:
                        clean.append(None)
                    elif isinstance(v, float) and pd.isna(v):
                        clean.append(None)
                    else:
                        clean.append(v)
                records.append(tuple(clean))
            execute_values(cur, query, records)
            conn.commit()
            inserted += len(records)

    return inserted


# ─── AUDITORIA ────────────────────────────────────────────────────────────────

QUERY_AUDIT = """
    SELECT
        ticker,
        COUNT(*)        AS dias_carregados,
        MIN(data)       AS data_inicio,
        MAX(data)       AS data_fim,
        MAX(dt_carga)   AS ultima_carga
    FROM cvm_data.cotacoes_diarias
    WHERE ticker = ANY(%s)
    GROUP BY ticker
    ORDER BY ticker
"""


def print_audit(conn, tickers: list[str]):
    with conn.cursor() as cur:
        cur.execute(QUERY_AUDIT, (tickers,))
        rows = cur.fetchall()
    log.info("\n  Auditoria da carga:")
    log.info(f"  {'TICKER':<10} {'DIAS':>6} {'INÍCIO':<12} {'FIM':<12}")
    log.info(f"  {'-'*46}")
    for r in rows:
        log.info(f"  {r[0]:<10} {r[1]:>6,} {str(r[2]):<12} {str(r[3]):<12}")
    log.info(f"  Total tickers com dados: {len(rows)}")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Carrega cotações diárias via brapi.dev")
    parser.add_argument("--ticker", help="Processar apenas este ticker (ex: WEGE3)")
    parser.add_argument("--force",   action="store_true", help="Ignorar cache local")
    parser.add_argument("--dry-run", action="store_true", help="Mostrar tickers sem inserir")
    args = parser.parse_args()

    if not BRAPI_TOKEN:
        log.warning("BRAPI_TOKEN não definido no .env — usando modo sem token (limitado)")
        log.warning("Sem token: range=max pode falhar. Configure BRAPI_TOKEN=... no .env")

    log.info("=" * 70)
    log.info("Início da carga: cotacoes_diarias (brapi.dev)")
    log.info("=" * 70)

    try:
        conn = psycopg2.connect(DB_URL)
    except psycopg2.OperationalError as e:
        log.error(f"Falha na conexão: {e}")
        sys.exit(1)

    # Obter lista de tickers
    if args.ticker:
        tickers_df = pd.DataFrame([{
            "ticker": args.ticker.upper(),
            "cnpj_emissor": None,
            "data_ipo": None
        }])
    else:
        tickers_df = get_tickers_ipo(conn)

    if args.dry_run:
        log.info("DRY RUN — tickers que seriam processados:")
        for _, row in tickers_df.iterrows():
            log.info(f"  {row['ticker']:>8}  IPO: {row.get('data_ipo', '?')}")
        conn.close()
        return

    # Processar cada ticker
    total_inseridos = 0
    erros = []
    tickers_ok = []

    log.info(f"\nProcessando {len(tickers_df):,} tickers...")

    for idx, row in tickers_df.iterrows():
        ticker = str(row["ticker"]).strip().upper()
        if not ticker:
            continue

        log.info(f"\n[{idx+1:3d}/{len(tickers_df):3d}] {ticker}  (IPO: {row.get('data_ipo','?')})")

        historical = fetch_ticker(ticker, force=args.force)

        if historical is None:
            erros.append(ticker)
            time.sleep(SLEEP_BETWEEN)
            continue

        df_hist = parse_historical(ticker, historical)

        if df_hist.empty:
            log.warning(f"  DataFrame vazio para {ticker}")
            erros.append(ticker)
            continue

        try:
            n = insert_cotacoes(conn, df_hist)
            total_inseridos += n
            tickers_ok.append(ticker)
            log.info(f"  ✔ {n:,} linhas inseridas (total série: {len(df_hist):,} dias)")
        except Exception as e:
            conn.rollback()
            log.error(f"  Erro INSERT {ticker}: {e}")
            erros.append(ticker)

        time.sleep(SLEEP_BETWEEN)

    # Auditoria final
    if tickers_ok:
        try:
            print_audit(conn, tickers_ok)
        except Exception as e:
            log.warning(f"Auditoria falhou: {e}")

    conn.close()
    log.info("\n" + "=" * 70)
    log.info(f"✔ Total inserido : {total_inseridos:,} linhas")
    log.info(f"✔ Tickers OK     : {len(tickers_ok):,}")
    log.info(f"✗ Tickers com erro: {len(erros):,} — {erros[:20]}")
    log.info("=" * 70)

    if erros:
        log.info("\nPara retentar os erros:")
        for t in erros:
            log.info(f"  python load_cotacoes_brapi.py --ticker {t} --force")


if __name__ == "__main__":
    main()