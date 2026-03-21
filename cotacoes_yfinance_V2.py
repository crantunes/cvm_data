"""
Script : load_CL.py
Tabela : cvm_data.cotacoes_diarias
Fonte  : Yahoo Finance via yfinance (histórico completo, gratuito)

Diferenças em relação à brapi:
  • Histórico completo desde a listagem na B3 (sem limitação de plano)
  • Tickers B3 têm sufixo .SA  →  PETR4 → PETR4.SA
  • yfinance retorna OHLCV + adj_close + dividendos + splits em um único download
  • adj_close já vem ajustado por splits E dividendos

Execução:
  pip install yfinance psycopg2-binary python-dotenv pandas

  python load_cotacoes_yfinance.py                    # todos os tickers da amostra
  python load_cotacoes_yfinance.py --ticker WEGE3     # um ticker específico
  python load_cotacoes_yfinance.py --dry-run          # lista tickers sem inserir
  python load_cotacoes_yfinance.py --force            # re-baixa mesmo com cache
  python load_cotacoes_yfinance.py --desde 2010-01-01 # limitar período inicial (padrão)
"""

import os
import sys
import time
import logging
import argparse
import warnings
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from pathlib import Path
from datetime import date

warnings.filterwarnings("ignore", category=FutureWarning)

try:
    import yfinance as yf
except ImportError:
    print("yfinance não instalado. Execute: pip install yfinance")
    sys.exit(1)

# ─── CONFIGURAÇÕES ────────────────────────────────────────────────────────────
load_dotenv()
DB_URL = "postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}".format(**{
    k: os.getenv(k, "") for k in ["DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_NAME"]
})

CACHE_DIR     = Path(r"D:\DATACVM\yfinance_cache")
BATCH_SIZE    = 1000
SLEEP_BETWEEN = 0.3   # yfinance não tem rate limit agressivo, mas vamos respeitar
DATA_INICIO   = "2026-03-12"  # alinhado com disponibilidade CVM (dados desde 2010)

# Tickers com código diferente no Yahoo Finance (B3 → Yahoo)
TICKER_REMAP = {
    # Formato: CODIGO_NO_BANCO → CODIGO_NO_YAHOO (sem .SA)
    "GOLL4"  : "GOLL54",    # GOL em recuperação judicial
    "SANB11" : "SANB11",    # OK
    # Adicione outros conforme necessário
}

# Tickers que não existem no Yahoo Finance (skip)
TICKERS_SKIP = {
    "CTNM3",   # não existe (só CTNM4)
    "00000",
    "BSLI03",
    "LOAR11",
}

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("load_cotacoes_yfinance.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# Silenciar logs verbosos do yfinance
logging.getLogger("yfinance").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


# ─── QUERY: tickers de IPO da amostra ─────────────────────────────────────────

QUERY_TICKERS_IPO = """
    SELECT DISTINCT
        v.codigo_negociacao                         AS ticker,
        o.cnpj_emissor,
        MIN(o.data_registro_oferta)                 AS data_ipo
    FROM cvm_data.ipo_oferta_distribuicao   o
    JOIN cvm_data.cad_valor_mobiliario      v
        ON REGEXP_REPLACE(v.cnpj_companhia, '[^0-9]', '', 'g')
         = REGEXP_REPLACE(o.cnpj_emissor,   '[^0-9]', '', 'g')
    WHERE o.oferta_inicial      = 'S'
      AND o.cnpj_emissor        IS NOT NULL
      AND v.codigo_negociacao   IS NOT NULL
      AND TRIM(v.codigo_negociacao) <> ''
      AND LENGTH(TRIM(v.codigo_negociacao)) BETWEEN 4 AND 7
      AND v.valor_mobiliario    ILIKE ANY(ARRAY['%Ações Ordinárias%','%Ações Preferenciais%','%Units%'])
    GROUP BY v.codigo_negociacao, o.cnpj_emissor
    ORDER BY data_ipo
"""


def get_tickers_ipo(conn) -> pd.DataFrame:
    log.info("  Buscando tickers IPO no banco...")
    df = pd.read_sql(QUERY_TICKERS_IPO, conn)
    antes = len(df)
    df = df[~df["ticker"].isin(TICKERS_SKIP)]
    if antes > len(df):
        log.info(f"  {antes - len(df)} ticker(s) removidos (lista de skip)")
    log.info(f"  Tickers IPO encontrados: {len(df):,}")
    return df


# ─── YFINANCE: download e parse ───────────────────────────────────────────────

def resolver_ticker_yahoo(ticker: str) -> str:
    """Converte código B3 → símbolo Yahoo Finance (adiciona .SA)."""
    base = TICKER_REMAP.get(ticker, ticker)
    return f"{base}.SA"


def fetch_ticker_yfinance(ticker: str, desde: str, force: bool = False) -> pd.DataFrame | None:
    """
    Baixa histórico completo via yfinance.
    Retorna DataFrame com colunas: data, open, high, low, close, volume,
    adj_close, dividendo, fator_split, moeda.
    """
    ticker_yahoo = resolver_ticker_yahoo(ticker)
    cache_path   = CACHE_DIR / f"{ticker}.parquet"

    # Cache local (evita re-download desnecessário)
    if not force and cache_path.exists():
        age_hours = (time.time() - cache_path.stat().st_mtime) / 3600
        if age_hours < 24:
            log.info(f"    [CACHE] {ticker} ({age_hours:.0f}h atrás)")
            return pd.read_parquet(cache_path)
        else:
            log.info(f"    [CACHE EXPIRADO] {ticker} ({age_hours:.0f}h) — rebaixando")

    try:
        yf_ticker = yf.Ticker(ticker_yahoo)

        # Download OHLCV + adj_close — auto_adjust=False para manter ambos os closes
        hist = yf_ticker.history(
            start=desde,
            end=str(date.today()),
            interval="1d",
            auto_adjust=False,   # mantém Close (nominal) e Adj Close separados
            actions=True,        # inclui Dividends e Stock Splits
        )

        if hist.empty:
            log.warning(f"    Sem dados para {ticker_yahoo}")
            return None

        # Normalizar índice
        hist.index = pd.to_datetime(hist.index).tz_localize(None).normalize()
        hist.index.name = "data"
        hist = hist.reset_index()

        # Renomear colunas padrão yfinance
        hist = hist.rename(columns={
            "Date"        : "data",
            "Open"        : "open",
            "High"        : "high",
            "Low"         : "low",
            "Close"       : "close",
            "Volume"      : "volume",
            "Adj Close"   : "adj_close",
            "Dividends"   : "dividendo",
            "Stock Splits": "fator_split",
        })

        # Converter data para date (sem timezone)
        hist["data"] = pd.to_datetime(hist["data"]).dt.date

        # Moeda (pega do info do ticker)
        try:
            moeda = yf_ticker.info.get("currency", "BRL")[:3]
        except Exception:
            moeda = "BRL"
        hist["moeda"] = moeda

        # Garantir colunas necessárias
        for col in ["open", "high", "low", "close", "adj_close"]:
            hist[col] = pd.to_numeric(hist[col], errors="coerce")
        hist["volume"]      = pd.to_numeric(hist["volume"],      errors="coerce").astype("Int64")
        hist["dividendo"]   = pd.to_numeric(hist["dividendo"],   errors="coerce").fillna(0)
        hist["fator_split"] = pd.to_numeric(hist["fator_split"], errors="coerce").fillna(1.0)

        # fator_split=0 no yfinance significa "sem split" — corrigir para 1.0
        hist.loc[hist["fator_split"] == 0, "fator_split"] = 1.0

        hist["ticker"] = ticker
        hist["fonte"]  = "yfinance"

        # Filtrar linhas sem data ou close nulo
        hist = hist.dropna(subset=["data", "close"])

        cols_finais = [
            "ticker", "data", "open", "high", "low", "close",
            "volume", "adj_close", "moeda", "fator_split", "dividendo",
            "fonte",
        ]
        hist = hist[cols_finais]

        log.info(
            f"    ✔ {ticker_yahoo}: {len(hist):,} dias | "
            f"{hist['data'].min()} → {hist['data'].max()} | moeda={moeda}"
        )

        # Salvar cache
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        hist.to_parquet(cache_path, index=False)

        return hist

    except Exception as e:
        log.error(f"    Erro ao baixar {ticker_yahoo}: {e}")
        return None


# ─── INSERT ───────────────────────────────────────────────────────────────────

INSERT_SQL = """
    INSERT INTO cvm_data.cotacoes_diarias
        (ticker, data, open, high, low, close, volume, adj_close,
         moeda, fator_split, dividendo, fonte)
    VALUES %s
    ON CONFLICT (ticker, data) DO UPDATE SET
        open        = EXCLUDED.open,
        high        = EXCLUDED.high,
        low         = EXCLUDED.low,
        close       = EXCLUDED.close,
        volume      = EXCLUDED.volume,
        adj_close   = EXCLUDED.adj_close,
        moeda       = EXCLUDED.moeda,
        fator_split = EXCLUDED.fator_split,
        dividendo   = EXCLUDED.dividendo,
        fonte       = EXCLUDED.fonte,
        dt_carga    = NOW()
"""

COLS = [
    "ticker", "data", "open", "high", "low", "close",
    "volume", "adj_close", "moeda", "fator_split", "dividendo", "fonte",
]


def insert_cotacoes(conn, df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    inserted = 0
    with conn.cursor() as cur:
        for start in range(0, len(df), BATCH_SIZE):
            batch = df.iloc[start: start + BATCH_SIZE][COLS]
            records = []
            for row in batch.itertuples(index=False, name=None):
                clean = []
                for v in row:
                    if v is pd.NaT or v is None:
                        clean.append(None)
                    elif hasattr(v, "item"):          # numpy scalar → python nativo
                        clean.append(v.item())
                    elif isinstance(v, float) and pd.isna(v):
                        clean.append(None)
                    else:
                        clean.append(v)
                records.append(tuple(clean))
            execute_values(cur, INSERT_SQL, records)
            conn.commit()
            inserted += len(records)

    return inserted


# ─── AUDITORIA ────────────────────────────────────────────────────────────────

def print_audit(conn, tickers: list[str]):
    sql = """
        SELECT ticker,
               COUNT(*)        AS dias,
               MIN(data)       AS inicio,
               MAX(data)       AS fim,
               SUM(CASE WHEN fator_split <> 1 THEN 1 ELSE 0 END) AS splits,
               SUM(CASE WHEN dividendo   >  0 THEN 1 ELSE 0 END) AS div_pagos
        FROM cvm_data.cotacoes_diarias
        WHERE ticker = ANY(%s)
        GROUP BY ticker
        ORDER BY ticker
    """
    with conn.cursor() as cur:
        cur.execute(sql, (tickers,))
        rows = cur.fetchall()

    log.info("\n  Auditoria da carga:")
    log.info(f"  {'TICKER':<10} {'DIAS':>6} {'INÍCIO':<12} {'FIM':<12} {'SPLITS':>6} {'DIVID':>6}")
    log.info(f"  {'-'*56}")
    for r in rows:
        log.info(f"  {r[0]:<10} {r[1]:>6,} {str(r[2]):<12} {str(r[3]):<12} {r[4]:>6} {r[5]:>6}")
    log.info(f"\n  Total tickers com dados: {len(rows)}")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Carrega cotações via yfinance (histórico completo)")
    parser.add_argument("--ticker",  help="Processar apenas este ticker (ex: WEGE3)")
    parser.add_argument("--force",   action="store_true", help="Ignorar cache local")
    parser.add_argument("--dry-run", action="store_true", help="Listar tickers sem inserir")
    parser.add_argument("--desde",   default=DATA_INICIO, help=f"Data início (default: {DATA_INICIO})")
    args = parser.parse_args()

    log.info("=" * 70)
    log.info("Início da carga: cotacoes_diarias (yfinance)")
    log.info(f"Período: {args.desde} → hoje")
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
            "data_ipo": None,
        }])
    else:
        tickers_df = get_tickers_ipo(conn)

    if tickers_df.empty:
        log.error("Nenhum ticker retornado. Verifique a query e os dados no banco.")
        conn.close()
        sys.exit(1)

    if args.dry_run:
        log.info(f"\nDRY RUN — {len(tickers_df)} tickers que seriam processados:")
        for _, row in tickers_df.iterrows():
            t = str(row["ticker"]).strip().upper()
            yahoo = resolver_ticker_yahoo(t)
            log.info(f"  {t:<10} → {yahoo:<14}  IPO: {row.get('data_ipo','?')}")
        conn.close()
        return

    # Processar
    total_inseridos = 0
    tickers_ok      = []
    erros           = []
    skipped         = []

    log.info(f"\nProcessando {len(tickers_df):,} tickers...\n")

    for idx, row in tickers_df.iterrows():
        ticker = str(row["ticker"]).strip().upper()
        if not ticker or ticker in TICKERS_SKIP:
            skipped.append(ticker)
            continue

        log.info(f"[{idx+1:3d}/{len(tickers_df):3d}] {ticker}  (IPO: {row.get('data_ipo','?')})")

        df_hist = fetch_ticker_yfinance(ticker, desde=args.desde, force=args.force)

        if df_hist is None or df_hist.empty:
            erros.append(ticker)
            time.sleep(SLEEP_BETWEEN)
            continue

        try:
            n = insert_cotacoes(conn, df_hist)
            total_inseridos += n
            tickers_ok.append(ticker)
            log.info(f"  ✔ {n:,} linhas inseridas/atualizadas")
        except Exception as e:
            conn.rollback()
            log.error(f"  Erro INSERT {ticker}: {e}")
            erros.append(ticker)

        time.sleep(SLEEP_BETWEEN)

    # Auditoria
    if tickers_ok:
        try:
            print_audit(conn, tickers_ok)
        except Exception as e:
            log.warning(f"Auditoria falhou: {e}")

    conn.close()

    log.info("\n" + "=" * 70)
    log.info(f"  Total inserido/atualizado : {total_inseridos:,} linhas")
    log.info(f"  Tickers OK               : {len(tickers_ok):,}")
    log.info(f"  Tickers pulados (skip)   : {len(skipped):,}")
    log.info(f"  Tickers com erro         : {len(erros):,}")
    if erros:
        log.info(f"  Erros                    : {erros[:20]}")
    log.info("=" * 70)

    if erros:
        log.info("\nPara retentar erros individualmente:")
        for t in erros[:10]:
            log.info(f"  python load_cotacoes_yfinance.py --ticker {t} --force")


if __name__ == "__main__":
    main()