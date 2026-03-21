-- ═════════════════════════════════════════════════════════════════════════════
-- FIX: views vw_first_day_return, vw_avg_first_day_return, vw_post_ipo_return
-- Correções aplicadas vs nefin_brapi.sql original:
--   v.cnpj_cia          → v.cnpj_companhia      (nome real em cad_valor_mobiliario)
--   v.tipo_valor_mobiliario → v.valor_mobiliario (nome real em cad_valor_mobiliario)
-- ═════════════════════════════════════════════════════════════════════════════

-- ── 2. First-Day Return ───────────────────────────────────────────────────────
CREATE OR REPLACE VIEW cvm_data.vw_first_day_return AS
WITH ipo_base AS (
    SELECT
        o.cnpj_emissor,
        o.data_registro_oferta,
        o.preco_unitario                                            AS preco_oferta,
        v.codigo_negociacao                                         AS ticker
    FROM cvm_data.ipo_oferta_distribuicao   o
    JOIN cvm_data.cad_valor_mobiliario      v
        ON v.cnpj_companhia = o.cnpj_emissor              -- CORRIGIDO
        AND v.valor_mobiliario ILIKE '%AÇÃO%'              -- CORRIGIDO
    WHERE o.oferta_inicial      = 'S'
      AND o.preco_unitario      IS NOT NULL
      AND o.preco_unitario      > 0
),
primeiro_pregao AS (
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
    ROUND(
        ((c.adj_close - p.preco_oferta) / p.preco_oferta)::NUMERIC, 6
    )                                                               AS first_day_return,
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
    AVG(b.first_day_return)                                         AS avg_fdr_60d,
    COUNT(b.cnpj_emissor)                                           AS n_ipos_60d
FROM cvm_data.vw_first_day_return   a
LEFT JOIN cvm_data.vw_first_day_return b
    ON b.data_registro_oferta <  a.data_registro_oferta
    AND b.data_registro_oferta >= a.data_registro_oferta - INTERVAL '60 days'
GROUP BY 1, 2, 3, 4;

COMMENT ON VIEW cvm_data.vw_avg_first_day_return IS
    'Avg. First-Day Return: média dos FDR dos IPOs nos 60 dias anteriores a cada IPO.';


-- ── 4. Post-IPO Return (BHAR) ─────────────────────────────────────────────────
CREATE OR REPLACE VIEW cvm_data.vw_post_ipo_return AS
WITH janelas AS (
    SELECT
        unnest(ARRAY[90, 180, 365])          AS dias_corridos,
        unnest(ARRAY['3m', '6m', '1a'])      AS label
),
ipo_tickers AS (
    SELECT DISTINCT
        o.cnpj_emissor,
        v.codigo_negociacao                                         AS ticker,
        MIN(c.data)                                                 AS data_inicio
    FROM cvm_data.ipo_oferta_distribuicao   o
    JOIN cvm_data.cad_valor_mobiliario      v
        ON v.cnpj_companhia = o.cnpj_emissor              -- CORRIGIDO
        AND v.valor_mobiliario ILIKE '%AÇÃO%'              -- CORRIGIDO
    JOIN cvm_data.cotacoes_diarias          c
        ON c.ticker = v.codigo_negociacao
    WHERE o.oferta_inicial = 'S'
    GROUP BY 1, 2
),
retornos AS (
    SELECT
        i.cnpj_emissor,
        i.ticker,
        i.data_inicio,
        j.dias_corridos,
        j.label,
        -- Retorno bruto da ação na janela (produto encadeado de retornos diários)
        (
            SELECT EXP(SUM(LN(NULLIF(
                       c2.adj_close /
                       NULLIF(LAG(c2.adj_close) OVER (
                           PARTITION BY c2.ticker ORDER BY c2.data
                       ), 0)
                   , 0)))) - 1.0
            FROM cvm_data.cotacoes_diarias c2
            WHERE c2.ticker = i.ticker
              AND c2.data BETWEEN i.data_inicio
                              AND i.data_inicio + j.dias_corridos
        )                                                           AS retorno_acao,
        -- Retorno do mercado (Rm NEFIN) na mesma janela
        (
            SELECT EXP(SUM(LN(1.0 + rm))) - 1.0
            FROM cvm_data.nefin_risk_factors
            WHERE data BETWEEN i.data_inicio
                           AND i.data_inicio + j.dias_corridos
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
    ROUND(retorno_acao::NUMERIC,    6)                              AS retorno_acao,
    ROUND(retorno_mercado::NUMERIC, 6)                              AS retorno_mercado,
    ROUND((retorno_acao - retorno_mercado)::NUMERIC, 6)             AS bhar
FROM retornos
WHERE retorno_acao    IS NOT NULL
  AND retorno_mercado IS NOT NULL;

COMMENT ON VIEW cvm_data.vw_post_ipo_return IS
    'Post-IPO Return (BHAR) em janelas de 3m, 6m e 1a. '
    'BHAR = retorno_acao - retorno_mercado (benchmark: Rm NEFIN).';
