-- ═════════════════════════════════════════════════════════════════════════════
-- AUDITORIA: dfp_financeira
-- Objetivo : Verificar integridade, completude e consistência dos dados
--            carregados na tabela Silver dfp_financeira.
-- Uso      : Execute bloco a bloco no pgAdmin. Cada bloco é independente.
-- ═════════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────────────────
-- BLOCO 1 — VISÃO GERAL: registros por ano e tipo de demonstração
-- Esperado: todos os 16 tipos presentes em cada ano carregado
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    dfp_ano,
    grupo_dfp,
    COUNT(*)                            AS qt_registros,
    COUNT(DISTINCT cnpj_cia)            AS qt_empresas,
    COUNT(DISTINCT dt_refer)            AS qt_datas_ref
FROM cvm_data.dfp_financeira
GROUP BY dfp_ano, grupo_dfp
ORDER BY dfp_ano, grupo_dfp;


-- ─────────────────────────────────────────────────────────────────────────────
-- BLOCO 2 — COMPLETUDE POR ANO: quantos tipos de demonstração cada ano tem
-- Esperado: 16 tipos por ano. Menos de 16 indica CSV ausente ou falha de carga
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    dfp_ano,
    COUNT(DISTINCT grupo_dfp)   AS qt_tipos_carregados,
    CASE WHEN COUNT(DISTINCT grupo_dfp) = 16
         THEN '✔ Completo'
         ELSE '✗ INCOMPLETO — verifique CSVs ausentes'
    END                         AS status
FROM cvm_data.dfp_financeira
GROUP BY dfp_ano
ORDER BY dfp_ano;


-- ─────────────────────────────────────────────────────────────────────────────
-- BLOCO 3 — CONSISTÊNCIA CONTÁBIL: Ativo Total = Passivo Total + PL
-- cd_conta '1' = Ativo Total (BPA), '2' = Passivo + PL Total (BPP)
-- Esperado: diferença zero ou próxima de zero para cada empresa/ano
-- Tolerância: admite diferença de até 1 unidade (arredondamento de escala)
-- ─────────────────────────────────────────────────────────────────────────────
WITH ativo AS (
    SELECT cnpj_cia, dfp_ano, dt_refer, versao, vl_conta AS vl_ativo
    FROM cvm_data.dfp_financeira
    WHERE grupo_dfp IN ('BPA_CON', 'BPA_IND')
      AND cd_conta = '1'
      AND ordem_exerc = 'ÚLTIMO'
),
passivo AS (
    SELECT cnpj_cia, dfp_ano, dt_refer, versao, vl_conta AS vl_passivo
    FROM cvm_data.dfp_financeira
    WHERE grupo_dfp IN ('BPP_CON', 'BPP_IND')
      AND cd_conta = '2'
      AND ordem_exerc = 'ÚLTIMO'
)
SELECT
    a.cnpj_cia,
    a.dfp_ano,
    a.grupo_dfp,       -- indica se veio do BPA_CON ou BPA_IND
    a.vl_ativo,
    p.vl_passivo,
    ABS(a.vl_ativo - p.vl_passivo)   AS diferenca,
    CASE WHEN ABS(a.vl_ativo - p.vl_passivo) <= 1
         THEN '✔ OK'
         ELSE '✗ DIVERGÊNCIA'
    END AS status_balanco
FROM ativo a
JOIN passivo p
    ON a.cnpj_cia = p.cnpj_cia
    AND a.dfp_ano = p.dfp_ano
    AND a.dt_refer = p.dt_refer
    AND a.versao   = p.versao
WHERE ABS(a.vl_ativo - p.vl_passivo) > 1   -- mostra apenas os divergentes
ORDER BY diferenca DESC
LIMIT 50;


-- ─────────────────────────────────────────────────────────────────────────────
-- BLOCO 4 — VERSÕES: verifica se só temos a versão mais alta por empresa/ano
-- Esperado: cada combinação (cnpj_cia, dt_refer, grupo_dfp, cd_conta) com
--           exatamente 1 registro. Se aparecer > 1, há problema no dedup.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    dfp_ano,
    grupo_dfp,
    COUNT(*)                            AS total_registros,
    COUNT(DISTINCT (cnpj_cia, dt_refer, cd_conta, ordem_exerc, coluna_df))
                                        AS combinacoes_unicas,
    COUNT(*) - COUNT(DISTINCT (cnpj_cia, dt_refer, cd_conta, ordem_exerc, coluna_df))
                                        AS duplicatas_remanescentes
FROM cvm_data.dfp_financeira
GROUP BY dfp_ano, grupo_dfp
HAVING COUNT(*) > COUNT(DISTINCT (cnpj_cia, dt_refer, cd_conta, ordem_exerc, coluna_df))
ORDER BY duplicatas_remanescentes DESC;
-- Resultado vazio = ✔ sem duplicatas remanescentes na tabela


-- ─────────────────────────────────────────────────────────────────────────────
-- BLOCO 5 — COBERTURA: empresas presentes na dfp_financeira vs cad_cia_aberta
-- Compara com a visão Gold vw_empresas_b3_nao_financeiras (396 empresas)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    f.dfp_ano,
    COUNT(DISTINCT f.cnpj_cia)              AS empresas_na_dfp,
    COUNT(DISTINCT v.cnpj_companhia)        AS empresas_no_gold,
    COUNT(DISTINCT v.cnpj_companhia)
        FILTER (WHERE f.cnpj_cia IS NULL)   AS empresas_gold_sem_dfp
FROM cvm_data.vw_empresas_b3_nao_financeiras v
LEFT JOIN cvm_data.dfp_financeira f
    ON f.cnpj_cia = v.cnpj_companhia
GROUP BY f.dfp_ano
ORDER BY f.dfp_ano;


-- ─────────────────────────────────────────────────────────────────────────────
-- BLOCO 6 — EMPRESAS DO GOLD SEM DFP EM UM ANO ESPECÍFICO
-- Troque o ano conforme necessário. Útil para identificar não-entregantes.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    v.cnpj_companhia,
    v.nome_empresarial,
    v.setor_atividade
FROM cvm_data.vw_empresas_b3_nao_financeiras v
WHERE NOT EXISTS (
    SELECT 1
    FROM cvm_data.dfp_financeira f
    WHERE f.cnpj_cia  = v.cnpj_companhia
      AND f.dfp_ano   = 2022               -- << ALTERE O ANO
      AND f.grupo_dfp = 'DRE_CON'
)
ORDER BY v.nome_empresarial;


-- ─────────────────────────────────────────────────────────────────────────────
-- BLOCO 7 — VALORES NULOS EM VL_CONTA: detecta contas sem valor
-- Pode ser legítimo (conta estrutural sem valor) ou problema de leitura
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    dfp_ano,
    grupo_dfp,
    COUNT(*)                            AS total,
    COUNT(*) FILTER (WHERE vl_conta IS NULL)
                                        AS qt_vl_null,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE vl_conta IS NULL) / COUNT(*), 2
    )                                   AS pct_null
FROM cvm_data.dfp_financeira
GROUP BY dfp_ano, grupo_dfp
HAVING COUNT(*) FILTER (WHERE vl_conta IS NULL) > 0
ORDER BY dfp_ano, pct_null DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- BLOCO 8 — ANOMALIAS DE VALOR: detecta valores extremos suspeitos
-- Contexto: escala_moeda = 'MIL' significa que vl_conta está em R$ mil
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    cnpj_cia,
    dfp_ano,
    grupo_dfp,
    cd_conta,
    ds_conta,
    escala_moeda,
    vl_conta,
    versao
FROM cvm_data.dfp_financeira
WHERE grupo_dfp IN ('BPA_CON','BPA_IND')
  AND cd_conta = '1'                -- Ativo Total
  AND ordem_exerc = 'ÚLTIMO'
  AND (
      vl_conta <= 0                 -- ativo total não pode ser zero ou negativo
      OR vl_conta > 1e15            -- acima de 1 quatrilhão em R$ mil = suspeito
  )
ORDER BY vl_conta DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- BLOCO 9 — CONSISTÊNCIA TEMPORAL: empresa deve ter dt_refer crescente por ano
-- Detecta casos onde dt_refer não corresponde ao dfp_ano esperado
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    cnpj_cia,
    dfp_ano,
    dt_refer,
    EXTRACT(YEAR FROM dt_refer)         AS ano_na_data,
    grupo_dfp
FROM cvm_data.dfp_financeira
WHERE EXTRACT(YEAR FROM dt_refer) != dfp_ano
  AND ordem_exerc = 'ÚLTIMO'
ORDER BY cnpj_cia, dfp_ano
LIMIT 50;
-- Resultado vazio = ✔ todos os registros com dt_refer coerente com dfp_ano


-- ─────────────────────────────────────────────────────────────────────────────
-- BLOCO 10 — RESUMO EXECUTIVO: totais gerais da carga
-- Visão consolidada para o apêndice metodológico do paper
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    MIN(dfp_ano)                        AS primeiro_ano,
    MAX(dfp_ano)                        AS ultimo_ano,
    COUNT(DISTINCT dfp_ano)             AS anos_carregados,
    COUNT(DISTINCT cnpj_cia)            AS empresas_distintas,
    COUNT(DISTINCT grupo_dfp)           AS tipos_demonstracao,
    COUNT(*)                            AS total_registros,
    MIN(dt_primeira_carga)              AS primeira_carga_em,
    MAX(dt_ultima_atualizacao)          AS ultima_atualizacao_em
FROM cvm_data.dfp_financeira;


-- ─────────────────────────────────────────────────────────────────────────────
-- BLOCO 11 — AUDITORIA ESPECÍFICA DMPL_IND: investigar o alto descarte
-- No log: DMPL_ind_2022 = 218.124 removidas (84%); DMPL_ind_2025 = 83%
-- Este bloco verifica quantas versões distintas existem por empresa
-- Deve ser executado direto nos CSVs (via Python) ou na RAW se existir
-- ─────────────────────────────────────────────────────────────────────────────
-- Proxy via Silver: verifica distribuição de versoes no que foi carregado
SELECT
    dfp_ano,
    grupo_dfp,
    versao,
    COUNT(*)                            AS qt_registros,
    COUNT(DISTINCT cnpj_cia)            AS qt_empresas
FROM cvm_data.dfp_financeira
WHERE grupo_dfp IN ('DMPL_IND', 'DMPL_CON')
GROUP BY dfp_ano, grupo_dfp, versao
ORDER BY dfp_ano, grupo_dfp, versao;
-- Versões altas (3, 4, 5+) em muitas empresas confirmam reapresentações em massa


-- ─────────────────────────────────────────────────────────────────────────────
-- BLOCO 12 — CROSS-CHECK: conta específica entre anos (série temporal)
-- Exemplo: Receita Líquida (cd_conta = '3.01') de uma empresa específica
-- Útil para verificar se a série temporal faz sentido antes do NLP/econometria
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    dfp_ano,
    dt_refer,
    ordem_exerc,
    versao,
    cd_conta,
    ds_conta,
    vl_conta,
    escala_moeda
FROM cvm_data.dfp_financeira
WHERE cnpj_cia  = '60.746.948/0001-12'   -- << SUBSTITUA PELO CNPJ QUE QUISER AUDITAR
  AND grupo_dfp = 'DRE_CON'
  AND cd_conta  = '3.01'
  AND ordem_exerc = 'ÚLTIMO'
ORDER BY dfp_ano, dt_refer;