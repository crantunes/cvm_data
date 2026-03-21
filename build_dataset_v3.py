"""
Script : build_dataset.py  (v3)
Âncora : cvm_data.prospecto_culture_score — exatamente os 98 CNPJs da amostra
Saída  : dataset_ipo.csv  — uma linha por empresa, pronto para Tabela 1 + modelos
"""

import os, sys, re, logging
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path

for _e in [Path(__file__).parent / ".env", Path(r"D:\VSCode\cvm_data\.env")]:
    if _e.exists():
        load_dotenv(_e, override=True)
        break

DB_URL  = "postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}".format(**{
    k: os.getenv(k, "") for k in ["DB_USER","DB_PASSWORD","DB_HOST","DB_PORT","DB_NAME"]
})
CSV_OUT = Path(__file__).parent / "dataset_ipo.csv"

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout),
              logging.FileHandler("build_dataset.log", encoding="utf-8")])
log = logging.getLogger(__name__)

# ════════════════════════════════════════════════════════════════════════════
# QUERIES
# ════════════════════════════════════════════════════════════════════════════

Q_ANCHOR = """
SELECT
    cs.cnpj_cia_14                  AS cnpj14,
    cs.cnpj_cia,
    cs.culture_score,
    cs.score_innovation,
    cs.score_integrity,
    cs.score_quality,
    cs.score_respect,
    cs.score_teamwork,
    cs.total_palavras
FROM cvm_data.prospecto_culture_score cs
"""

Q_IPO = """
SELECT
    REGEXP_REPLACE(o.cnpj_emissor,'[^0-9]','','g')  AS cnpj14,
    o.nome_emissor,
    o.data_registro_oferta                           AS data_ipo,
    EXTRACT(YEAR FROM o.data_registro_oferta)::INT   AS ano_ipo,
    o.preco_unitario                                 AS offer_price,
    v.codigo_negociacao                              AS ticker
FROM cvm_data.ipo_oferta_distribuicao o
LEFT JOIN cvm_data.cad_valor_mobiliario v
    ON  REGEXP_REPLACE(v.cnpj_companhia,'[^0-9]','','g')
      = REGEXP_REPLACE(o.cnpj_emissor,  '[^0-9]','','g')
    AND v.valor_mobiliario ILIKE '%A%es%'
WHERE o.oferta_inicial = 'S'
  AND o.cnpj_emissor IS NOT NULL
ORDER BY REGEXP_REPLACE(o.cnpj_emissor,'[^0-9]','','g'), o.data_registro_oferta
"""

Q_FDR = """
SELECT
    REGEXP_REPLACE(cnpj_emissor,'[^0-9]','','g')  AS cnpj14,
    first_day_return
FROM cvm_data.vw_first_day_return
"""

Q_MARKET = """
SELECT
    data,
    market_return_30d,
    market_vol_30d_annualized  AS market_vol_30d
FROM cvm_data.vw_market_conditions
WHERE obs_janela_30d >= 15
"""

Q_DFP = """
SELECT
    REGEXP_REPLACE(f.cnpj_cia,'[^0-9]','','g')                AS cnpj14,
    EXTRACT(YEAR FROM f.dt_refer)::INT                         AS ano_dfp,
    SUM(CASE WHEN f.cd_conta = '1'       AND f.ordem_exerc = 'ÚLTIMO'
             THEN f.vl_conta END)
        * MAX(CASE WHEN f.escala_moeda='MIL' THEN 1000.0 ELSE 1.0 END) AS ativo_total,
    SUM(CASE WHEN f.cd_conta = '3.01'    AND f.ordem_exerc = 'ÚLTIMO'
             THEN ABS(f.vl_conta) END)
        * MAX(CASE WHEN f.escala_moeda='MIL' THEN 1000.0 ELSE 1.0 END) AS receita_liquida,
    SUM(CASE WHEN f.cd_conta IN ('2.01','2.02') AND f.ordem_exerc = 'ÚLTIMO'
             THEN f.vl_conta END)
        * MAX(CASE WHEN f.escala_moeda='MIL' THEN 1000.0 ELSE 1.0 END) AS divida_total,
    SUM(CASE WHEN f.cd_conta = '3.11'    AND f.ordem_exerc = 'ÚLTIMO'
             THEN f.vl_conta END)
        * MAX(CASE WHEN f.escala_moeda='MIL' THEN 1000.0 ELSE 1.0 END) AS lucro_liquido,
    SUM(CASE WHEN f.cd_conta = '6.01'    AND f.ordem_exerc = 'ÚLTIMO'
             THEN ABS(f.vl_conta) END)
        * MAX(CASE WHEN f.escala_moeda='MIL' THEN 1000.0 ELSE 1.0 END) AS capex,
    SUM(CASE WHEN f.cd_conta = '1.02.04' AND f.ordem_exerc = 'ÚLTIMO'
             THEN f.vl_conta END)
        * MAX(CASE WHEN f.escala_moeda='MIL' THEN 1000.0 ELSE 1.0 END) AS intangivel,
    SUM(CASE WHEN f.cd_conta IN ('1.01.03','1.02.02') AND f.ordem_exerc = 'ÚLTIMO'
             THEN f.vl_conta END)
        * MAX(CASE WHEN f.escala_moeda='MIL' THEN 1000.0 ELSE 1.0 END) AS imobilizado
FROM cvm_data.dfp_financeira f
WHERE f.versao = (
    SELECT MAX(f2.versao) FROM cvm_data.dfp_financeira f2
    WHERE REGEXP_REPLACE(f2.cnpj_cia,'[^0-9]','','g')
        = REGEXP_REPLACE(f.cnpj_cia,'[^0-9]','','g')
      AND EXTRACT(YEAR FROM f2.dt_refer) = EXTRACT(YEAR FROM f.dt_refer)
)
GROUP BY
    REGEXP_REPLACE(f.cnpj_cia,'[^0-9]','','g'),
    EXTRACT(YEAR FROM f.dt_refer)::INT
HAVING SUM(CASE WHEN f.cd_conta = '1' AND f.ordem_exerc = 'ÚLTIMO'
                THEN f.vl_conta END) > 0
"""

Q_BIG4 = """
SELECT
    REGEXP_REPLACE(a.cnpj_companhia,'[^0-9]','','g')  AS cnpj14,
    MAX(CASE WHEN UPPER(a.auditor) SIMILAR TO
        '%(PRICEWATERHOUSE|PWC|ERNST|KPMG|DELOITTE)%'
        THEN 1 ELSE 0 END)  AS big4
FROM cvm_data.cad_auditor a
GROUP BY REGEXP_REPLACE(a.cnpj_companhia,'[^0-9]','','g')
"""

Q_AGE = """
SELECT
    REGEXP_REPLACE(cnpj_companhia,'[^0-9]','','g')  AS cnpj14,
    data_constituicao
FROM cvm_data.cad_cia_aberta
WHERE data_constituicao IS NOT NULL
"""

Q_INTERNET = """
SELECT
    REGEXP_REPLACE(cnpj_companhia,'[^0-9]','','g')  AS cnpj14,
    CASE WHEN setor_atividade ILIKE ANY(ARRAY[
        '%tecnologia%','%software%','%internet%',
        '%telecomunica%','%inform%','%digital%'])
    THEN 1 ELSE 0 END  AS internet
FROM cvm_data.cad_cia_aberta
"""

Q_PREV_IPOS = """
SELECT
    REGEXP_REPLACE(a.cnpj_emissor,'[^0-9]','','g')  AS cnpj14,
    a.data_registro_oferta,
    COUNT(b.cnpj_emissor)  AS n_previous_ipos
FROM cvm_data.ipo_oferta_distribuicao a
LEFT JOIN cvm_data.ipo_oferta_distribuicao b
    ON  b.oferta_inicial = 'S'
    AND b.data_registro_oferta <  a.data_registro_oferta
    AND b.data_registro_oferta >= a.data_registro_oferta - INTERVAL '60 days'
    AND b.cnpj_emissor <> a.cnpj_emissor
WHERE a.oferta_inicial = 'S'
  AND a.cnpj_emissor IS NOT NULL
GROUP BY
    REGEXP_REPLACE(a.cnpj_emissor,'[^0-9]','','g'),
    a.data_registro_oferta
"""

Q_ACOES = """
SELECT DISTINCT ON (
    REGEXP_REPLACE(cnpj_cia,'[^0-9]','','g'),
    EXTRACT(YEAR FROM dt_refer)::INT
)
    REGEXP_REPLACE(cnpj_cia,'[^0-9]','','g')  AS cnpj14,
    EXTRACT(YEAR FROM dt_refer)::INT           AS ano,
    qt_acao_total_cap_integr
FROM cvm_data.dfp_composicao_capital
WHERE qt_acao_total_cap_integr > 0
ORDER BY
    REGEXP_REPLACE(cnpj_cia,'[^0-9]','','g'),
    EXTRACT(YEAR FROM dt_refer)::INT,
    versao DESC
"""

# ════════════════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════════════════

def winsorize(s, p=0.01):
    lo, hi = s.quantile(p), s.quantile(1-p)
    return s.clip(lo, hi)

def to_num(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    return df

CONT_VARS = ['first_day_return','culture_score','offer_price',
             'ln_sales','ln_age','leverage','capex_scaled',
             'intangibles_scaled','imobilizado_scaled',
             'market_return_30d','market_vol_30d','n_previous_ipos']

# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════
def main():
    log.info("="*60)
    log.info("Build Dataset IPO  (âncora: 98 prospectos)")
    log.info("="*60)

    engine = create_engine(DB_URL)

    with engine.connect() as c:
        anchor  = pd.read_sql(text(Q_ANCHOR),   c)
        ipo_raw = pd.read_sql(text(Q_IPO),       c)
        fdr_raw = pd.read_sql(text(Q_FDR),       c)
        market  = pd.read_sql(text(Q_MARKET),    c)
        dfp     = pd.read_sql(text(Q_DFP),       c)
        big4    = pd.read_sql(text(Q_BIG4),      c)
        age_df  = pd.read_sql(text(Q_AGE),       c)
        internet= pd.read_sql(text(Q_INTERNET),  c)
        prev    = pd.read_sql(text(Q_PREV_IPOS), c)
        acoes   = pd.read_sql(text(Q_ACOES),     c)

    # ── Normalizar cnpj14 em todas as tabelas ─────────────────────────────
    def zfill14(s):
        return s.astype(str).str.strip().str.zfill(14)

    for tb in [anchor, ipo_raw, fdr_raw, dfp, big4, age_df, internet, prev, acoes]:
        tb['cnpj14'] = zfill14(tb['cnpj14'])

    log.info(f"Âncora: {len(anchor)} empresas")

    # ── IPO: dedup por cnpj14 (keep= primeiro IPO) ────────────────────────
    ipo = (ipo_raw.sort_values('data_ipo')
                  .drop_duplicates('cnpj14', keep='first')
                  .copy())

    # ── FDR: dedup ────────────────────────────────────────────────────────
    fdr = fdr_raw.drop_duplicates('cnpj14', keep='first').copy()

    # ── Prev IPOs: dedup ──────────────────────────────────────────────────
    prev = (prev.sort_values('data_registro_oferta')
                .drop_duplicates('cnpj14', keep='first')
                .copy())

    # ── Dataset: começa pelo anchor ───────────────────────────────────────
    df = anchor.copy()

    # ── Merge IPO ─────────────────────────────────────────────────────────
    df = df.merge(ipo[['cnpj14','nome_emissor','data_ipo',
                        'ano_ipo','offer_price','ticker']],
                  on='cnpj14', how='left')
    df['data_ipo'] = pd.to_datetime(df['data_ipo'], errors='coerce')
    df['ano_ipo']  = pd.to_numeric(df['ano_ipo'],   errors='coerce').astype('Int64')
    log.info(f"  data_ipo preenchida: {df['data_ipo'].notna().sum()}/{len(df)}")

    # ── Merge First-Day Return ────────────────────────────────────────────
    df = df.merge(fdr[['cnpj14','first_day_return']], on='cnpj14', how='left')
    log.info(f"  first_day_return:    {df['first_day_return'].notna().sum()}/{len(df)}")

    # ── Merge Market (exact date match) ──────────────────────────────────
    market['data'] = pd.to_datetime(market['data'])
    market = to_num(market, ['market_return_30d','market_vol_30d'])
    df = df.merge(market.rename(columns={'data':'data_ipo'}),
                  on='data_ipo', how='left')

    # ── Merge DFP (ano-1 primeiro, fallback ano_ipo) ──────────────────────
    dfp = to_num(dfp, ['ano_dfp','ativo_total','receita_liquida',
                        'divida_total','lucro_liquido','capex',
                        'intangivel','imobilizado'])
    dfp_cols = ['cnpj14','ano_dfp','ativo_total','receita_liquida',
                'divida_total','lucro_liquido','capex','intangivel','imobilizado']

    df['ano_dfp'] = (df['ano_ipo'] - 1).astype('Int64')
    df = df.merge(dfp[dfp_cols], on=['cnpj14','ano_dfp'], how='left')

    # fallback: mesmo ano do IPO
    sem_dfp = df['ativo_total'].isna()
    if sem_dfp.sum() > 0:
        df.loc[sem_dfp, 'ano_dfp'] = df.loc[sem_dfp, 'ano_ipo'].astype('Int64')
        fill = df[sem_dfp][['cnpj14','ano_dfp']].merge(dfp[dfp_cols],
                                                        on=['cnpj14','ano_dfp'],
                                                        how='left')
        for col in ['ativo_total','receita_liquida','divida_total',
                    'lucro_liquido','capex','intangivel','imobilizado']:
            df.loc[sem_dfp, col] = fill[col].values
    log.info(f"  ativo_total:         {df['ativo_total'].notna().sum()}/{len(df)}")

    # ── Variáveis financeiras ─────────────────────────────────────────────
    at = df['ativo_total'].replace(0, np.nan)
    df['leverage']           = df['divida_total']    / at
    df['capex_scaled']       = df['capex']            / at
    df['intangibles_scaled'] = df['intangivel']       / at
    df['imobilizado_scaled'] = df['imobilizado']      / at
    df['ln_sales'] = np.log1p(
        pd.to_numeric(df['receita_liquida'], errors='coerce').clip(lower=0)
    )

    # ── EPS ───────────────────────────────────────────────────────────────
    acoes = to_num(acoes, ['ano','qt_acao_total_cap_integr'])
    acoes = acoes.rename(columns={'ano':'ano_dfp'})
    df = df.merge(acoes[['cnpj14','ano_dfp','qt_acao_total_cap_integr']],
                  on=['cnpj14','ano_dfp'], how='left')
    df['eps']          = (df['lucro_liquido']
                          / df['qt_acao_total_cap_integr'].replace(0, np.nan))
    df['positive_eps'] = (df['eps'] > 0).astype('Int64')

    # ── Big 4 ─────────────────────────────────────────────────────────────
    df = df.merge(big4, on='cnpj14', how='left')
    df['big4'] = pd.to_numeric(df['big4'], errors='coerce').fillna(0).astype(int)

    # ── Idade ─────────────────────────────────────────────────────────────
    age_df['data_constituicao'] = pd.to_datetime(age_df['data_constituicao'],
                                                  errors='coerce')
    age_df = (age_df.sort_values('data_constituicao')
                    .drop_duplicates('cnpj14', keep='first'))
    df = df.merge(age_df[['cnpj14','data_constituicao']], on='cnpj14', how='left')
    df['age_anos'] = ((df['data_ipo'] - df['data_constituicao'])
                      .dt.days / 365.25).clip(lower=0)
    df['ln_age']   = np.log1p(df['age_anos'])

    # ── Internet ──────────────────────────────────────────────────────────
    internet_g = internet.groupby('cnpj14')['internet'].max().reset_index()
    df = df.merge(internet_g, on='cnpj14', how='left')
    df['internet'] = df['internet'].fillna(0).astype(int)

    # ── Previous IPOs ─────────────────────────────────────────────────────
    df = df.merge(prev[['cnpj14','n_previous_ipos']], on='cnpj14', how='left')
    df['n_previous_ipos'] = (pd.to_numeric(df['n_previous_ipos'], errors='coerce')
                               .fillna(0).astype(int))

    # ── Price Revision (sem bookbuilding estruturado) ─────────────────────
    df['price_revision']     = np.nan
    df['price_revision_pos'] = np.nan

    # ── Winsorização ─────────────────────────────────────────────────────
    log.info("Aplicando winsorização p1-p99...")
    for v in CONT_VARS:
        if v in df.columns and df[v].notna().sum() > 10:
            df[v] = winsorize(df[v])

    # ── Exportar ──────────────────────────────────────────────────────────
    cols_out = [
        'cnpj14','cnpj_cia','ticker','nome_emissor','data_ipo','ano_ipo',
        'first_day_return','price_revision',
        'culture_score','score_innovation','score_integrity',
        'score_quality','score_respect','score_teamwork',
        'offer_price','big4','ln_sales','ln_age','leverage',
        'capex_scaled','intangibles_scaled','imobilizado_scaled',
        'positive_eps','internet','n_previous_ipos',
        'price_revision_pos','market_return_30d','market_vol_30d',
        'ativo_total','receita_liquida','age_anos','eps',
    ]
    cols_out = [c for c in cols_out if c in df.columns]
    df_out   = df[cols_out].drop_duplicates('cnpj14').copy()

    df_out.to_csv(CSV_OUT, index=False, encoding='utf-8-sig')

    log.info(f"\n{'='*60}")
    log.info(f"✔ {CSV_OUT}")
    log.info(f"  Observações: {len(df_out)}")
    log.info(f"\n  Cobertura das variáveis principais:")
    check = ['first_day_return','culture_score','leverage',
             'ln_sales','ln_age','big4','market_return_30d','positive_eps']
    for v in check:
        if v in df_out.columns:
            n = df_out[v].notna().sum()
            log.info(f"    {v:<28}: {n:>3}/{len(df_out)} ({100*n/len(df_out):.0f}%)")
    log.info("="*60)

if __name__ == "__main__":
    main()