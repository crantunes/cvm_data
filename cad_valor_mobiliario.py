"""
Script: load_cad_valor_mobiliario.py
Descrição: Popula a tabela Silver cvm_data.cad_valor_mobiliario a partir dos dados RAW
           da tabela cvm_data.raw_fca_cia_aberta_valor_mobiliario.

Padrão Silver Satélite:
  - FK: id_cad_cia_aberta → cad_cia_aberta
  - Granularidade: 1 linha por empresa + fca_ano + versao (histórico completo de painel)
  - Reapresentação de FCA no mesmo ano → nova linha (mantém histórico de versões)
  - CNPJ ausente no cad_cia_aberta → inserido automaticamente antes da satélite
  - Controle de auditoria: atualiza_cad + dt_primeira_carga + dt_ultima_atualizacao

Dependências:
    pip install pandas psycopg2-binary python-dotenv

Uso:
    python load_cad_valor_mobiliario.py
"""

import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
import logging
import sys

# ─── CONFIGURAÇÕES DE CONEXÃO (via .env) ─────────────────────────────────────
load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_URL  = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ─── ANOS A PROCESSAR ────────────────────────────────────────────────────────
ANOS = list(range(2010, 2027))

# ─── LOGGING ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("load_cad_valor_mobiliario.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ─── QUERY: busca registros RAW de um fca_ano ────────────────────────────────
SQL_SELECT_RAW = """
    SELECT
        cnpj_companhia,
        fca_ano,
        id_documento,
        versao,
        nome_empresarial,
        valor_mobiliario,
        sigla_classe_acao_preferencial,
        classe_acao_preferencial,
        codigo_negociacao,
        composicao_bdr_unit,
        mercado,
        sigla_entidade_administradora,
        entidade_administradora,
        data_inicio_negociacao,
        data_fim_negociacao,
        segmento,
        data_inicio_listagem,
        data_fim_listagem
    FROM cvm_data.raw_fca_cia_aberta_valor_mobiliario
    WHERE fca_ano = %s
    ORDER BY cnpj_companhia, versao, id_documento
"""

# ─── QUERY: busca id_cad_cia_aberta pelo CNPJ ────────────────────────────────
SQL_GET_ID_CAD = """
    SELECT id_cad_cia_aberta
    FROM cvm_data.cad_cia_aberta
    WHERE cnpj_companhia = %s
"""

# ─── QUERY: INSERT mínimo em cad_cia_aberta para CNPJs ausentes ──────────────
SQL_INSERT_CAD_MINIMO = """
    INSERT INTO cvm_data.cad_cia_aberta (
        cnpj_companhia,
        fca_ano,
        tipo_participante,
        indativo_cmp,
        atualiza_cad,
        dt_primeira_carga,
        dt_ultima_atualizacao
    ) VALUES (%s, %s, 'Companhia Aberta', 'não', 0, NOW(), NOW())
    ON CONFLICT (cnpj_companhia) DO NOTHING
    RETURNING id_cad_cia_aberta
"""

# ─── QUERY: verifica se o registro já existe na Silver ───────────────────────
# Granularidade: id_cad_cia_aberta + fca_ano + versao + id_documento
SQL_CHECK_EXISTS = """
    SELECT id_cad_valor_mobiliario
    FROM cvm_data.cad_valor_mobiliario
    WHERE id_cad_cia_aberta = %s
      AND fca_ano            = %s
      AND versao             = %s
      AND id_documento       = %s
"""

# ─── QUERY: INSERT Silver ────────────────────────────────────────────────────
SQL_INSERT = """
    INSERT INTO cvm_data.cad_valor_mobiliario (
        id_cad_cia_aberta,
        cnpj_companhia,
        fca_ano,
        id_documento,
        versao,
        nome_empresarial,
        valor_mobiliario,
        sigla_classe_acao_preferencial,
        classe_acao_preferencial,
        codigo_negociacao,
        composicao_bdr_unit,
        mercado,
        sigla_entidade_administradora,
        entidade_administradora,
        data_inicio_negociacao,
        data_fim_negociacao,
        segmento,
        data_inicio_listagem,
        data_fim_listagem,
        atualiza_cad,
        dt_primeira_carga,
        dt_ultima_atualizacao
    ) VALUES (
        %(id_cad_cia_aberta)s,
        %(cnpj_companhia)s,
        %(fca_ano)s,
        %(id_documento)s,
        %(versao)s,
        %(nome_empresarial)s,
        %(valor_mobiliario)s,
        %(sigla_classe_acao_preferencial)s,
        %(classe_acao_preferencial)s,
        %(codigo_negociacao)s,
        %(composicao_bdr_unit)s,
        %(mercado)s,
        %(sigla_entidade_administradora)s,
        %(entidade_administradora)s,
        %(data_inicio_negociacao)s,
        %(data_fim_negociacao)s,
        %(segmento)s,
        %(data_inicio_listagem)s,
        %(data_fim_listagem)s,
        0, NOW(), NOW()
    )
"""

# ─── QUERY: UPDATE Silver (reapresentação com mesma chave) ───────────────────
SQL_UPDATE = """
    UPDATE cvm_data.cad_valor_mobiliario SET
        nome_empresarial               = %(nome_empresarial)s,
        valor_mobiliario               = %(valor_mobiliario)s,
        sigla_classe_acao_preferencial = %(sigla_classe_acao_preferencial)s,
        classe_acao_preferencial       = %(classe_acao_preferencial)s,
        codigo_negociacao              = %(codigo_negociacao)s,
        composicao_bdr_unit            = %(composicao_bdr_unit)s,
        mercado                        = %(mercado)s,
        sigla_entidade_administradora  = %(sigla_entidade_administradora)s,
        entidade_administradora        = %(entidade_administradora)s,
        data_inicio_negociacao         = %(data_inicio_negociacao)s,
        data_fim_negociacao            = %(data_fim_negociacao)s,
        segmento                       = %(segmento)s,
        data_inicio_listagem           = %(data_inicio_listagem)s,
        data_fim_listagem              = %(data_fim_listagem)s,
        atualiza_cad                   = atualiza_cad + 1,
        dt_ultima_atualizacao          = NOW()
    WHERE id_cad_cia_aberta = %(id_cad_cia_aberta)s
      AND fca_ano            = %(fca_ano)s
      AND versao             = %(versao)s
      AND id_documento       = %(id_documento)s
"""

# ─── FUNÇÕES ──────────────────────────────────────────────────────────────────

def get_or_create_id_cad(cur, cnpj: str, fca_ano: int, nome: str) -> int:
    """
    Retorna o id_cad_cia_aberta para o CNPJ.
    Se não existir, insere registro mínimo em cad_cia_aberta e loga o caso.
    """
    cur.execute(SQL_GET_ID_CAD, (cnpj,))
    row = cur.fetchone()
    if row:
        return row[0]

    # CNPJ ausente → insere mínimo e loga
    log.warning(f"CNPJ {cnpj} ({nome}) ausente no cad_cia_aberta — inserindo registro mínimo.")
    cur.execute(SQL_INSERT_CAD_MINIMO, (cnpj, fca_ano))
    result = cur.fetchone()
    if result:
        return result[0]

    # Se ON CONFLICT DO NOTHING disparou (race condition), busca novamente
    cur.execute(SQL_GET_ID_CAD, (cnpj,))
    return cur.fetchone()[0]


def processar_ano(cur, ano: int) -> tuple[int, int, int]:
    """
    Processa todos os registros de valor mobiliário de um fca_ano.
    Retorna (inseridos, atualizados, ignorados).
    """
    cur.execute(SQL_SELECT_RAW, (ano,))
    rows = cur.fetchall()
    col_names = [desc[0] for desc in cur.description]

    inseridos   = 0
    atualizados = 0
    ignorados   = 0

    for row in rows:
        raw = dict(zip(col_names, row))
        cnpj = raw["cnpj_companhia"]

        # Resolve FK → id_cad_cia_aberta (cria se ausente)
        id_cad = get_or_create_id_cad(
            cur, cnpj, raw["fca_ano"], raw.get("nome_empresarial", "")
        )
        raw["id_cad_cia_aberta"] = id_cad

        # Verifica se já existe na Silver
        cur.execute(SQL_CHECK_EXISTS, (
            id_cad,
            raw["fca_ano"],
            raw["versao"],
            raw["id_documento"],
        ))
        existe = cur.fetchone()

        if existe is None:
            cur.execute(SQL_INSERT, raw)
            inseridos += 1
        else:
            cur.execute(SQL_UPDATE, raw)
            atualizados += 1

    return inseridos, atualizados, ignorados


# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    log.info("=== Início da carga Silver: cad_valor_mobiliario ===")

    try:
        conn = psycopg2.connect(DB_URL)
        conn.autocommit = False
    except psycopg2.OperationalError as e:
        log.error(f"Falha na conexão: {e}")
        sys.exit(1)

    total_ins = total_upd = total_ign = 0

    try:
        with conn.cursor() as cur:
            for ano in ANOS:
                log.info(f"── Processando fca_ano={ano} ──")
                ins, upd, ign = processar_ano(cur, ano)
                conn.commit()
                log.info(
                    f"   fca_ano={ano} → "
                    f"inseridos: {ins:,} | atualizados: {upd:,} | ignorados: {ign:,}"
                )
                total_ins += ins
                total_upd += upd
                total_ign += ign

    except Exception as e:
        conn.rollback()
        log.error(f"Erro durante o processamento: {e}")
        raise
    finally:
        conn.close()
        log.info("Conexão encerrada.")

    log.info("=== Carga Silver finalizada ===")
    log.info(
        f"Totais → inseridos: {total_ins:,} | "
        f"atualizados: {total_upd:,} | ignorados: {total_ign:,}"
    )


if __name__ == "__main__":
    main()