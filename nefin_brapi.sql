-- ═════════════════════════════════════════════════════════════════════════════
-- Silver: nefin_risk_factors
-- Fonte : https://nefin.com.br/resources/risk_factors/nefin_factors.csv
-- Freq  : Diária (dias úteis B3) — desde 2001-01-02
-- Update: Manual (baixar novo CSV e recarregar) — NEFIN atualiza ~semanalmente
--
-- Variáveis do paper (Li et al. 2021b):
--   Market Return   → rm  (retorno diário do portfólio de mercado brasileiro)
--   Risk-Free       → risk_free (CDI/Selic diário — proxy do risk-free)
--   Rm-Rf           → rm_minus_rf (excesso de retorno de mercado)
--   Fama-French BR  → smb, hml, wml, iml (fatores adicionais p/ benchmark)
--
-- Uso principal: calcular Market Return (rm) e Market Volatility (std rolling
--   30 dias de rm) na data de cada IPO; benchmark para Post-IPO Return (BHAR).
--
-- PK: data (única por dia útil — sem múltiplas versões)
-- ═════════════════════════════════════════════════════════════════════════════

CREATE TABLE cvm_data.nefin_risk_factors (

    data                DATE        NOT NULL,

    -- ── Fatores de risco NEFIN ───────────────────────────────────────────────
    rm                  NUMERIC(18, 10),   -- retorno do portfólio de mercado (diário)
    risk_free           NUMERIC(18, 10),   -- taxa livre de risco diária (CDI/Selic)
    rm_minus_rf         NUMERIC(18, 10),   -- excesso de retorno de mercado (Rm - Rf)
    smb                 NUMERIC(18, 10),   -- Small Minus Big
    hml                 NUMERIC(18, 10),   -- High Minus Low (value vs growth)
    wml                 NUMERIC(18, 10),   -- Winners Minus Losers (momentum)
    iml                 NUMERIC(18, 10),   -- Illiquid Minus Liquid

    -- ── Auditoria ────────────────────────────────────────────────────────────
    dt_carga            TIMESTAMP   NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_nefin_risk_factors PRIMARY KEY (data)
);

COMMENT ON TABLE  cvm_data.nefin_risk_factors                IS 'Fatores de risco diários NEFIN/USP — Market Return, Risk-Free e Fama-French BR';
COMMENT ON COLUMN cvm_data.nefin_risk_factors.rm             IS 'Retorno diário do portfólio de mercado brasileiro (proxy Ibovespa value-weighted)';
COMMENT ON COLUMN cvm_data.nefin_risk_factors.risk_free      IS 'Taxa livre de risco diária (CDI/Selic)';
COMMENT ON COLUMN cvm_data.nefin_risk_factors.rm_minus_rf    IS 'Excesso de retorno de mercado = Rm - Rf';


-- ═════════════════════════════════════════════════════════════════════════════
-- Silver: nefin_ivol_br
-- Fonte : https://nefin.com.br/resources/volatility_index/IVol-BR.xls
-- Freq  : Variável (mensal/diária conforme disponibilidade)
-- Update: Manual
--
-- Variáveis do paper:
--   Market Volatility → ivol_br (índice de volatilidade implícita do Ibovespa)
--                       Alternativa: vol_realizada (std rolling 30d de rm)
--                       calculada via query sobre nefin_risk_factors.
--
-- Nota: O IVol-BR é forward-looking (implícita das opções sobre IBOVESPA).
--   Papers que usam trailing std usarão a view vw_market_volatility (abaixo).
-- ═════════════════════════════════════════════════════════════════════════════

CREATE TABLE cvm_data.nefin_ivol_br (

    data                DATE        NOT NULL,

    ivol_br             NUMERIC(18, 10),   -- índice de volatilidade implícita anualizado
    variance_premium    NUMERIC(18, 10),   -- prêmio de variância (componente do IVol-BR)
    risk_aversion       NUMERIC(18, 10),   -- coeficiente de aversão ao risco implícito

    dt_carga            TIMESTAMP   NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_nefin_ivol_br PRIMARY KEY (data)
);

COMMENT ON TABLE  cvm_data.nefin_ivol_br         IS 'Índice de volatilidade implícita brasileiro IVol-BR — NEFIN/USP';
COMMENT ON COLUMN cvm_data.nefin_ivol_br.ivol_br IS 'Volatilidade implícita anualizada do Ibovespa (análogo ao VIX americano)';


-- ═════════════════════════════════════════════════════════════════════════════
-- Silver: cotacoes_diarias
-- Fonte : brapi.dev /api/quote/{ticker}?range=max&interval=1d
-- Freq  : Diária (dias úteis B3)
-- Escopo: Apenas empresas da amostra de IPO (filtragem via ipo_oferta_distrib.)
--
-- Variáveis do paper:
--   First-Day Return  → (close_d1 - preco_oferta) / preco_oferta
--                       preco_oferta em ipo_oferta_distribuicao.preco_unitario
--   Post-IPO Return   → retorno acumulado ajustado por splits/dividendos
--                       calculado via adj_close sobre janela T (3m, 6m, 1a)
--   Avg. First-Day Return → média rolling 60 dias de First-Day Return
--
-- Chave de cruzamento: cnpj_cia → cad_valor_mobiliario.codigo_negociacao (ticker)
--
-- PK: (ticker, data) — uma linha por ativo por dia útil
-- Estratégia de carga: INSERT ... ON CONFLICT DO NOTHING
--   (dados históricos não mudam; adj_close pode ser recalculado por splits →
--    para refresh usar DELETE WHERE ticker = X AND data >= split_date)
-- ═════════════════════════════════════════════════════════════════════════════

CREATE TABLE cvm_data.cotacoes_diarias (

    ticker              VARCHAR(20) NOT NULL,  -- ex: PETR4, VALE3, BBAS3
    data                DATE        NOT NULL,

    -- ── OHLCV ────────────────────────────────────────────────────────────────
    open                NUMERIC(18, 6),
    high                NUMERIC(18, 6),
    low                 NUMERIC(18, 6),
    close               NUMERIC(18, 6),
    volume              BIGINT,

    -- ── Preço ajustado (splits + dividendos) ─────────────────────────────────
    adj_close           NUMERIC(18, 6),   -- campo adjustedClose da brapi.dev

    -- ── Controle ─────────────────────────────────────────────────────────────
    fonte               VARCHAR(20) NOT NULL DEFAULT 'brapi',
    dt_carga            TIMESTAMP   NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_cotacoes_diarias PRIMARY KEY (ticker, data)
);

CREATE INDEX idx_cotacoes_ticker ON cvm_data.cotacoes_diarias (ticker);
CREATE INDEX idx_cotacoes_data   ON cvm_data.cotacoes_diarias (data);

COMMENT ON TABLE  cvm_data.cotacoes_diarias          IS 'Cotações diárias OHLCV + adj_close via brapi.dev — escopo: IPOs da amostra';
COMMENT ON COLUMN cvm_data.cotacoes_diarias.adj_close IS 'Preço ajustado por splits e dividendos (adjustedClose brapi.dev)';


-- ═════════════════════════════════════════════════════════════════════════════
-- Gold Views — variáveis do paper prontas para uso
-- ═════════════════════════════════════════════════════════════════════════════

-- ── 1. Market Return e Market Volatility (trailing 30 dias) ─────────────────
-- Uso: JOIN com ipo_oferta_distribuicao por data_registro_oferta
--      market_return_30d = retorno acumulado Rm nos 30 dias anteriores ao IPO
--      market_vol_30d    = desvio padrão anualizado de rm nos 30 dias anteriores
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW cvm_data.vw_market_conditions AS
SELECT
    data,
    rm,
    risk_free,
    rm_minus_rf,

    -- Market Return: retorno acumulado dos 30 dias anteriores (inclusive)
    -- Nota: produto de (1+rm) na janela — equivale ao retorno composto
    EXP(SUM(LN(1.0 + rm)) OVER w30) - 1.0                          AS market_return_30d,

    -- Market Volatility: std(rm) * sqrt(252) na janela de 30 dias
    STDDEV_SAMP(rm) OVER w30 * SQRT(252.0)                         AS market_vol_30d_annualized,

    -- Contagem de observações na janela (para filtrar janelas incompletas)
    COUNT(*) OVER w30                                               AS obs_janela_30d

FROM cvm_data.nefin_risk_factors

WINDOW w30 AS (
    ORDER BY data
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
);

COMMENT ON VIEW cvm_data.vw_market_conditions IS
    'Market Return (retorno composto 30d) e Market Volatility (std anualizado 30d) — NEFIN/USP. '
    'JOIN com ipo_oferta_distribuicao ON data = data_registro_oferta.';


-- ── 2. First-Day Return ───────────────────────────────────────────────────────
-- Requer cotacoes_diarias carregado para os tickers de IPO
-- preco_oferta vem de ipo_oferta_distribuicao.preco_unitario
-- D+1: primeiro pregão após data_registro_oferta (MIN(data) > data_oferta)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW cvm_data.vw_first_day_return AS
WITH ipo_base AS (
    SELECT
        o.cnpj_emissor,
        o.data_registro_oferta,
        o.preco_unitario                                            AS preco_oferta,
        v.codigo_negociacao                                         AS ticker
    FROM cvm_data.ipo_oferta_distribuicao   o
    JOIN cvm_data.cad_valor_mobiliario      v
        ON v.cnpj_cia = o.cnpj_emissor
        AND v.tipo_valor_mobiliario ILIKE '%AÇÃO%'
    WHERE o.oferta_inicial      = 'S'
      AND o.preco_unitario      IS NOT NULL
      AND o.preco_unitario      > 0
),
primeiro_pregao AS (
    -- Primeiro dia de negociação após a data de oferta
    SELECT
        i.cnpj_emissor,
        i.ticker,
        i.preco_oferta,
        i.data_registro_oferta,
        MIN(c.data)                                                 AS data_d1
    FROM ipo_base               i
    JOIN cvm_data.cotacoes_diarias c
        ON c.ticker = i.ticker
        AND c.data > i.data_registro_oferta
    GROUP BY 1, 2, 3, 4
)
SELECT
    p.cnpj_emissor,
    p.ticker,
    p.data_registro_oferta,
    p.preco_oferta,
    p.data_d1,
    c.close                                                         AS close_d1,
    c.adj_close                                                     AS adj_close_d1,
    -- First-Day Return (raw, usando preco de fechamento ajustado)
    ROUND(
        ((c.adj_close - p.preco_oferta) / p.preco_oferta)::NUMERIC, 6
    )                                                               AS first_day_return,
    -- Dummy: retorno positivo no primeiro dia
    CASE WHEN c.adj_close > p.preco_oferta THEN 1 ELSE 0 END       AS first_day_positive
FROM primeiro_pregao            p
JOIN cvm_data.cotacoes_diarias  c
    ON c.ticker = p.ticker
    AND c.data  = p.data_d1;

COMMENT ON VIEW cvm_data.vw_first_day_return IS
    'First-Day Return de cada IPO: (adj_close_D1 - preco_oferta) / preco_oferta. '
    'Requer cotacoes_diarias carregado para os tickers da amostra.';


-- ── 3. Avg. First-Day Return (média rolling 60 dias) ─────────────────────────
CREATE OR REPLACE VIEW cvm_data.vw_avg_first_day_return AS
SELECT
    a.cnpj_emissor,
    a.ticker,
    a.data_registro_oferta,
    a.first_day_return,
    -- Média dos FDR dos IPOs realizados nos 60 dias anteriores (excluindo o próprio)
    AVG(b.first_day_return)                                         AS avg_fdr_60d,
    COUNT(b.cnpj_emissor)                                           AS n_ipos_60d
FROM cvm_data.vw_first_day_return   a
LEFT JOIN cvm_data.vw_first_day_return b
    ON b.data_registro_oferta < a.data_registro_oferta
    AND b.data_registro_oferta >= a.data_registro_oferta - INTERVAL '60 days'
GROUP BY 1, 2, 3, 4;

COMMENT ON VIEW cvm_data.vw_avg_first_day_return IS
    'Avg. First-Day Return: média dos FDR dos IPOs nos 60 dias anteriores a cada IPO.';


-- ── 4. Post-IPO Return (BHAR — Buy-and-Hold Abnormal Return) ─────────────────
-- Janelas: 3 meses, 6 meses, 1 ano após IPO (configuráveis)
-- Abnormal = retorno bruto da ação - retorno do mercado (Rm NEFIN) na mesma janela
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW cvm_data.vw_post_ipo_return AS
WITH janelas AS (
    SELECT unnest(ARRAY[90, 180, 365]) AS dias_corridos,
           unnest(ARRAY['3m', '6m', '1a']) AS label
),
ipo_tickers AS (
    SELECT DISTINCT
        o.cnpj_emissor,
        v.codigo_negociacao     AS ticker,
        MIN(c.data)             AS data_inicio   -- primeiro pregão
    FROM cvm_data.ipo_oferta_distribuicao   o
    JOIN cvm_data.cad_valor_mobiliario      v ON v.cnpj_cia = o.cnpj_emissor
    JOIN cvm_data.cotacoes_diarias          c ON c.ticker   = v.codigo_negociacao
    WHERE o.oferta_inicial = 'S'
      AND v.tipo_valor_mobiliario ILIKE '%AÇÃO%'
    GROUP BY 1, 2
),
retornos AS (
    SELECT
        i.cnpj_emissor,
        i.ticker,
        i.data_inicio,
        j.dias_corridos,
        j.label,
        -- Retorno bruto da ação na janela
        (   SELECT EXP(SUM(LN(1.0 + (c2.adj_close / LAG(c2.adj_close) OVER (PARTITION BY c2.ticker ORDER BY c2.data) - 1.0))))-1.0
            FROM cvm_data.cotacoes_diarias c2
            WHERE c2.ticker = i.ticker
              AND c2.data BETWEEN i.data_inicio AND i.data_inicio + j.dias_corridos
        )                                                           AS retorno_acao,
        -- Retorno do mercado (Rm NEFIN) na mesma janela
        (   SELECT EXP(SUM(LN(1.0 + rm)))-1.0
            FROM cvm_data.nefin_risk_factors
            WHERE data BETWEEN i.data_inicio AND i.data_inicio + j.dias_corridos
        )                                                           AS retorno_mercado
    FROM ipo_tickers    i
    CROSS JOIN janelas  j
)
SELECT
    cnpj_emissor,
    ticker,
    data_inicio,
    dias_corridos,
    label,
    ROUND(retorno_acao::NUMERIC, 6)                                 AS retorno_acao,
    ROUND(retorno_mercado::NUMERIC, 6)                              AS retorno_mercado,
    ROUND((retorno_acao - retorno_mercado)::NUMERIC, 6)             AS bhar   -- Buy-and-Hold Abnormal Return
FROM retornos;

COMMENT ON VIEW cvm_data.vw_post_ipo_return IS
    'Post-IPO Return (BHAR) em janelas de 3m, 6m e 1a. '
    'BHAR = retorno_acao - retorno_mercado (benchmark: Rm NEFIN).';
