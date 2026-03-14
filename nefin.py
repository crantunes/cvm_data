"""
Script : load_nefin.py
Tabelas: cvm_data.nefin_risk_factors
         cvm_data.nefin_ivol_br
Fontes :
  Risk Factors: https://nefin.com.br/resources/risk_factors/nefin_factors.csv
  IVol-BR     : https://nefin.com.br/resources/volatility_index/IVol-BR.xls

Variáveis do paper:
  Market Return   → vw_market_conditions.market_return_30d
  Market Volatility → vw_market_conditions.market_vol_30d_annualized
  (views criadas no DDL nefin_brapi.sql)

Estratégia:
  • Download direto das URLs do NEFIN (CSV e XLS públicos, sem autenticação)
  • INSERT ... ON CONFLICT (data) DO UPDATE SET ... (upsert por data)
    → seguro para re-execuções; atualiza valores se NEFIN revisar série
  • Salva cópias locais em DATA_DIR para auditoria e reuso offline

Execução:
  python load_nefin.py
  python load_nefin.py --only risk_factors
  python load_nefin.py --only ivol
  python load_nefin.py --from-file risk_factors nefin_factors.csv
"""

import os
import sys
import io
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

NEFIN_RF_URL  = "https://nefin.com.br/resources/risk_factors/nefin_factors.csv"
NEFIN_VOL_URL = "https://nefin.com.br/resources/volatility_index/IVol-BR.xls"

DATA_DIR   = Path(r"D:\DATACVM\NEFIN")          # pasta local para salvar cópias
BATCH_SIZE = 2000
TIMEOUT    = 30   # segundos por request

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("load_nefin.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ─── DOWNLOAD ─────────────────────────────────────────────────────────────────

def download_bytes(url: str, label: str) -> bytes:
    log.info(f"  Baixando {label}...")
    log.info(f"  URL: {url}")
    headers = {"User-Agent": "Mozilla/5.0 (compatible; research-script/1.0)"}
    r = requests.get(url, headers=headers, timeout=TIMEOUT)
    r.raise_for_status()
    log.info(f"  Download OK — {len(r.content):,} bytes")
    return r.content


def save_local(data: bytes, filename: str):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / filename
    path.write_bytes(data)
    log.info(f"  Salvo em: {path}")


# ─── RISK FACTORS ─────────────────────────────────────────────────────────────

def parse_risk_factors(raw: bytes) -> pd.DataFrame:
    """
    Estrutura REAL do CSV NEFIN (verificada em 13/03/2026):
    "","Date","Rm_minus_Rf","SMB","HML","WML","IML","Risk_Free"

    Nota: NÃO existe coluna "RM" separada no CSV.
    rm (retorno bruto de mercado) é derivado: rm = Rm_minus_Rf + Risk_Free
    """
    df = pd.read_csv(
        io.BytesIO(raw),
        index_col=0,        # primeira coluna é índice numérico — ignorar
    )
    df.columns = [c.strip() for c in df.columns]

    log.info(f"  Colunas do CSV: {list(df.columns)}")

    # Renomear para snake_case da tabela
    df = df.rename(columns={
        "Date"        : "data",
        "Rm_minus_Rf" : "rm_minus_rf",
        "SMB"         : "smb",
        "HML"         : "hml",
        "WML"         : "wml",
        "IML"         : "iml",
        "Risk_Free"   : "risk_free",
    })

    df["data"] = pd.to_datetime(df["data"], errors="coerce").dt.date
    df = df.dropna(subset=["data"])

    num_cols = ["rm_minus_rf", "risk_free", "smb", "hml", "wml", "iml"]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # rm não existe no CSV — derivar: Rm = (Rm - Rf) + Rf
    df["rm"] = df["rm_minus_rf"] + df["risk_free"]

    log.info(f"  Risk Factors: {len(df):,} linhas | {df['data'].min()} → {df['data'].max()}")
    return df


def upsert_risk_factors(conn, df: pd.DataFrame) -> int:
    cols = ["data", "rm", "risk_free", "rm_minus_rf", "smb", "hml", "wml", "iml"]
    query = """
        INSERT INTO cvm_data.nefin_risk_factors
            (data, rm, risk_free, rm_minus_rf, smb, hml, wml, iml)
        VALUES %s
        ON CONFLICT (data) DO UPDATE SET
            rm          = EXCLUDED.rm,
            risk_free   = EXCLUDED.risk_free,
            rm_minus_rf = EXCLUDED.rm_minus_rf,
            smb         = EXCLUDED.smb,
            hml         = EXCLUDED.hml,
            wml         = EXCLUDED.wml,
            iml         = EXCLUDED.iml,
            dt_carga    = NOW()
    """
    total = len(df)
    upserted = 0
    with conn.cursor() as cur:
        for start in range(0, total, BATCH_SIZE):
            batch = df.iloc[start: start + BATCH_SIZE][cols]
            records = [
                tuple(None if pd.isna(v) else v for v in row)
                for row in batch.itertuples(index=False, name=None)
            ]
            execute_values(cur, query, records)
            conn.commit()
            upserted += len(records)
    return upserted


# ─── IVOL-BR ─────────────────────────────────────────────────────────────────

def parse_ivol_br(raw: bytes) -> pd.DataFrame:
    """
    O IVol-BR.xls tem 3 abas / pode ter 1 aba.
    Requer: pip install xlrd>=2.0.1   (formato .xls legado)
    """
    try:
        xls = pd.ExcelFile(io.BytesIO(raw), engine="xlrd")
    except ImportError:
        raise ImportError(
            "Pacote xlrd nao encontrado. Instale com:\n"
            "  pip install xlrd>=2.0.1"
        )
    log.info(f"  Abas no XLS: {xls.sheet_names}")

    frames = {}
    for sheet in xls.sheet_names:
        df_s = xls.parse(sheet)
        df_s.columns = [str(c).strip() for c in df_s.columns]

        # Identificar coluna de data
        date_col = next((c for c in df_s.columns if "date" in c.lower()), None)
        if date_col is None:
            log.warning(f"  Aba '{sheet}' sem coluna Date — ignorada")
            continue

        df_s = df_s.rename(columns={date_col: "data"})
        df_s["data"] = pd.to_datetime(df_s["data"], errors="coerce").dt.date
        df_s = df_s.dropna(subset=["data"])
        frames[sheet] = df_s.set_index("data")

    if not frames:
        raise ValueError("Nenhuma aba válida encontrada no IVol-BR.xls")

    # Merge de todas as abas por data
    result = pd.concat(frames.values(), axis=1)

    # Mapear colunas para os nomes da tabela
    col_map = {}
    for col in result.columns:
        low = col.lower()
        if "ivol" in low or "volatility" in low:
            col_map[col] = "ivol_br"
        elif "variance" in low and "premium" in low:
            col_map[col] = "variance_premium"
        elif "aversion" in low or "risk av" in low:
            col_map[col] = "risk_aversion"
    result = result.rename(columns=col_map)

    # Garantir colunas mínimas
    for c in ["ivol_br", "variance_premium", "risk_aversion"]:
        if c not in result.columns:
            result[c] = None

    result = result[["ivol_br", "variance_premium", "risk_aversion"]].reset_index()
    result.columns = ["data"] + list(result.columns[1:])

    for col in ["ivol_br", "variance_premium", "risk_aversion"]:
        result[col] = pd.to_numeric(result[col], errors="coerce")

    result = result.dropna(subset=["data"])
    log.info(f"  IVol-BR: {len(result):,} linhas | {result['data'].min()} → {result['data'].max()}")
    return result


def upsert_ivol_br(conn, df: pd.DataFrame) -> int:
    cols = ["data", "ivol_br", "variance_premium", "risk_aversion"]
    query = """
        INSERT INTO cvm_data.nefin_ivol_br
            (data, ivol_br, variance_premium, risk_aversion)
        VALUES %s
        ON CONFLICT (data) DO UPDATE SET
            ivol_br          = EXCLUDED.ivol_br,
            variance_premium = EXCLUDED.variance_premium,
            risk_aversion    = EXCLUDED.risk_aversion,
            dt_carga         = NOW()
    """
    total = len(df)
    upserted = 0
    with conn.cursor() as cur:
        for start in range(0, total, BATCH_SIZE):
            batch = df.iloc[start: start + BATCH_SIZE][cols]
            records = [
                tuple(None if (v is None or (isinstance(v, float) and pd.isna(v))) else v
                      for v in row)
                for row in batch.itertuples(index=False, name=None)
            ]
            execute_values(cur, query, records)
            conn.commit()
            upserted += len(records)
    return upserted


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Carrega dados NEFIN no cvm_data")
    parser.add_argument("--only", choices=["risk_factors", "ivol"],
                        help="Carregar apenas uma das tabelas")
    parser.add_argument("--from-file", nargs=2, metavar=("TYPE", "PATH"),
                        help="Usar arquivo local em vez de download. TYPE: risk_factors|ivol")
    args = parser.parse_args()

    log.info("=" * 70)
    log.info("Início da carga NEFIN")
    log.info("=" * 70)

    try:
        conn = psycopg2.connect(DB_URL)
    except psycopg2.OperationalError as e:
        log.error(f"Falha na conexão: {e}")
        sys.exit(1)

    try:
        # ── Risk Factors ─────────────────────────────────────────────────────
        if args.only in (None, "risk_factors"):
            log.info("\n[1/2] nefin_risk_factors")
            if args.from_file and args.from_file[0] == "risk_factors":
                raw_rf = Path(args.from_file[1]).read_bytes()
                log.info(f"  Usando arquivo local: {args.from_file[1]}")
            else:
                raw_rf = download_bytes(NEFIN_RF_URL, "Risk Factors CSV")
                ts = datetime.date.today().strftime("%Y%m%d")
                save_local(raw_rf, f"nefin_factors_{ts}.csv")

            df_rf = parse_risk_factors(raw_rf)
            n_rf  = upsert_risk_factors(conn, df_rf)
            log.info(f"  ✔ {n_rf:,} linhas upsert em nefin_risk_factors")

        # ── IVol-BR ──────────────────────────────────────────────────────────
        if args.only in (None, "ivol"):
            log.info("\n[2/2] nefin_ivol_br")
            if args.from_file and args.from_file[0] == "ivol":
                raw_vol = Path(args.from_file[1]).read_bytes()
                log.info(f"  Usando arquivo local: {args.from_file[1]}")
            else:
                raw_vol = download_bytes(NEFIN_VOL_URL, "IVol-BR XLS")
                ts = datetime.date.today().strftime("%Y%m%d")
                save_local(raw_vol, f"IVol-BR_{ts}.xls")

            df_vol = parse_ivol_br(raw_vol)
            n_vol  = upsert_ivol_br(conn, df_vol)
            log.info(f"  ✔ {n_vol:,} linhas upsert em nefin_ivol_br")

    except Exception as e:
        conn.rollback()
        log.error(f"Erro: {e}", exc_info=True)
        raise
    finally:
        conn.close()
        log.info("\nConexão encerrada.")

    log.info("=" * 70)
    log.info("✔ Carga NEFIN concluída.")
    log.info("=" * 70)
    log.info("")
    log.info("Próximo passo: verificar as views Gold criadas no DDL")
    log.info("  SELECT * FROM cvm_data.vw_market_conditions ORDER BY data DESC LIMIT 5;")
    log.info("  SELECT * FROM cvm_data.vw_first_day_return   LIMIT 5;")


if __name__ == "__main__":
    main()