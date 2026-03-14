"""
Script: cad_cia_aberta.py
Descrição: Popula a tabela Silver cvm_data.cad_cia_aberta a partir dos dados RAW
           da tabela cvm_data.raw_fca_cia_aberta_geral.

Estratégia:
  - Processa fca_ano em ordem crescente (2010 → 2026)
  - Primeira ocorrência de um CNPJ → INSERT
  - Ocorrência subsequente com dados iguais → ignora
  - Ocorrência subsequente com dados diferentes → UPDATE + incrementa atualiza_cad
  - cnpj_companhia é UNIQUE (chave de negócio) — id_cad_cia_aberta é a PK SERIAL (chave relacional FK nas tabelas satélite)

Dependências:
    pip install pandas psycopg2-binary python-dotenv

Arquivo .env esperado (na mesma pasta do script):
    DB_USER=postgres
    DB_PASSWORD=sua_senha
    DB_HOST=localhost
    DB_PORT=5432
    DB_NAME=cvm_data

Uso:
    python cad_cia_aberta.py
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

# ─── ANOS A PROCESSAR (ordem crescente garante que o mais antigo entra primeiro) ─
ANOS = list(range(2010, 2027))   # 2010 até 2026 inclusive

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("load_cad_cia_aberta.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ─── CAMPOS COMPARÁVEIS (usados para detectar se houve mudança) ───────────────
# Excluímos campos de controle (atualiza_cad, dt_primeira_carga, dt_ultima_atualizacao)
# e campos fixos (tipo_participante, indativo_cmp, data_inicio_cmp)
CAMPOS_COMPARAVEIS = [
    "codigo_cvm",
    "nome_empresarial",
    "data_nome_empresarial",
    "nome_empresarial_anterior",
    "data_constituicao",
    "data_registro_cvm",
    "categoria_registro_cvm",
    "data_categoria_registro_cvm",
    "situacao_registro_cvm",
    "data_situacao_registro_cvm",
    "pais_origem",
    "pais_custodia_valores_mobiliarios",
    "setor_atividade",
    "descricao_atividade",
    "situacao_emissor",
    "data_situacao_emissor",
    "especie_controle_acionario",
    "data_especie_controle_acionario",
    "dia_encerramento_exercicio_social",
    "mes_encerramento_exercicio_social",
    "data_alteracao_exercicio_social",
    "pagina_web",
    "id_documento",
    "data_referencia",
    "versao",
    "fca_ano",
]

# ─── QUERY: busca registros RAW de um fca_ano ─────────────────────────────────
SQL_SELECT_RAW = """
    SELECT
        cnpj_companhia,
        codigo_cvm,
        fca_ano,
        nome_empresarial,
        data_nome_empresarial,
        nome_empresarial_anterior,
        data_constituicao,
        data_registro_cvm,
        categoria_registro_cvm,
        data_categoria_registro_cvm,
        situacao_registro_cvm,
        data_situacao_registro_cvm,
        pais_origem,
        pais_custodia_valores_mobiliarios,
        setor_atividade,
        descricao_atividade,
        situacao_emissor,
        data_situacao_emissor,
        especie_controle_acionario,
        data_especie_controle_acionario,
        dia_encerramento_exercicio_social,
        mes_encerramento_exercicio_social,
        data_alteracao_exercicio_social,
        pagina_web,
        id_documento,
        data_referencia,
        versao
    FROM cvm_data.raw_fca_cia_aberta_geral
    WHERE fca_ano = %s
    ORDER BY cnpj_companhia, versao DESC, data_referencia DESC
"""

# ─── QUERY: verifica se CNPJ já existe na Silver ──────────────────────────────
SQL_CHECK_EXISTS = """
    SELECT
        codigo_cvm, nome_empresarial, data_nome_empresarial,
        nome_empresarial_anterior, data_constituicao, data_registro_cvm,
        categoria_registro_cvm, data_categoria_registro_cvm,
        situacao_registro_cvm, data_situacao_registro_cvm,
        pais_origem, pais_custodia_valores_mobiliarios,
        setor_atividade, descricao_atividade, situacao_emissor,
        data_situacao_emissor, especie_controle_acionario,
        data_especie_controle_acionario, dia_encerramento_exercicio_social,
        mes_encerramento_exercicio_social, data_alteracao_exercicio_social,
        pagina_web, id_documento, data_referencia, versao, fca_ano
    FROM cvm_data.cad_cia_aberta
    WHERE cnpj_companhia = %s
"""

# ─── QUERY: INSERT novo registro ──────────────────────────────────────────────
SQL_INSERT = """
    INSERT INTO cvm_data.cad_cia_aberta (
        cnpj_companhia, codigo_cvm, fca_ano, nome_empresarial,
        data_nome_empresarial, nome_empresarial_anterior, data_constituicao,
        data_registro_cvm, tipo_participante, categoria_registro_cvm,
        data_categoria_registro_cvm, situacao_registro_cvm,
        data_situacao_registro_cvm, pais_origem, indativo_cmp, data_inicio_cmp,
        pais_custodia_valores_mobiliarios, setor_atividade, descricao_atividade,
        situacao_emissor, data_situacao_emissor, especie_controle_acionario,
        data_especie_controle_acionario, dia_encerramento_exercicio_social,
        mes_encerramento_exercicio_social, data_alteracao_exercicio_social,
        pagina_web, id_documento, data_referencia, versao,
        atualiza_cad, dt_primeira_carga, dt_ultima_atualizacao
    ) VALUES (
        %(cnpj_companhia)s, %(codigo_cvm)s, %(fca_ano)s, %(nome_empresarial)s,
        %(data_nome_empresarial)s, %(nome_empresarial_anterior)s, %(data_constituicao)s,
        %(data_registro_cvm)s, 'Companhia Aberta', %(categoria_registro_cvm)s,
        %(data_categoria_registro_cvm)s, %(situacao_registro_cvm)s,
        %(data_situacao_registro_cvm)s, %(pais_origem)s, 'não', NULL,
        %(pais_custodia_valores_mobiliarios)s, %(setor_atividade)s, %(descricao_atividade)s,
        %(situacao_emissor)s, %(data_situacao_emissor)s, %(especie_controle_acionario)s,
        %(data_especie_controle_acionario)s, %(dia_encerramento_exercicio_social)s,
        %(mes_encerramento_exercicio_social)s, %(data_alteracao_exercicio_social)s,
        %(pagina_web)s, %(id_documento)s, %(data_referencia)s, %(versao)s,
        0, NOW(), NOW()
    )
"""

# ─── QUERY: UPDATE registro existente com dados novos ─────────────────────────
SQL_UPDATE = """
    UPDATE cvm_data.cad_cia_aberta SET
        codigo_cvm                          = %(codigo_cvm)s,
        fca_ano                             = %(fca_ano)s,
        nome_empresarial                    = %(nome_empresarial)s,
        data_nome_empresarial               = %(data_nome_empresarial)s,
        nome_empresarial_anterior           = %(nome_empresarial_anterior)s,
        data_constituicao                   = %(data_constituicao)s,
        data_registro_cvm                   = %(data_registro_cvm)s,
        categoria_registro_cvm              = %(categoria_registro_cvm)s,
        data_categoria_registro_cvm         = %(data_categoria_registro_cvm)s,
        situacao_registro_cvm               = %(situacao_registro_cvm)s,
        data_situacao_registro_cvm          = %(data_situacao_registro_cvm)s,
        pais_origem                         = %(pais_origem)s,
        pais_custodia_valores_mobiliarios   = %(pais_custodia_valores_mobiliarios)s,
        setor_atividade                     = %(setor_atividade)s,
        descricao_atividade                 = %(descricao_atividade)s,
        situacao_emissor                    = %(situacao_emissor)s,
        data_situacao_emissor               = %(data_situacao_emissor)s,
        especie_controle_acionario          = %(especie_controle_acionario)s,
        data_especie_controle_acionario     = %(data_especie_controle_acionario)s,
        dia_encerramento_exercicio_social   = %(dia_encerramento_exercicio_social)s,
        mes_encerramento_exercicio_social   = %(mes_encerramento_exercicio_social)s,
        data_alteracao_exercicio_social     = %(data_alteracao_exercicio_social)s,
        pagina_web                          = %(pagina_web)s,
        id_documento                        = %(id_documento)s,
        data_referencia                     = %(data_referencia)s,
        versao                              = %(versao)s,
        atualiza_cad                        = atualiza_cad + 1,
        dt_ultima_atualizacao               = NOW()
    WHERE cnpj_companhia = %(cnpj_companhia)s
"""

# ─── FUNÇÕES ──────────────────────────────────────────────────────────────────

def registros_sao_iguais(raw: dict, existente: dict) -> bool:
    """Compara os campos comparáveis entre o registro RAW e o existente na Silver."""
    for campo in CAMPOS_COMPARAVEIS:
        v_raw = raw.get(campo)
        v_exi = existente.get(campo)
        # Normaliza None vs string vazia
        v_raw = None if v_raw == "" else v_raw
        v_exi = None if v_exi == "" else v_exi
        if v_raw != v_exi:
            return False
    return True


def processar_ano(cur, ano: int) -> tuple[int, int, int]:
    """
    Processa todos os CNPJs de um fca_ano.
    Retorna (inseridos, atualizados, ignorados).
    """
    cur.execute(SQL_SELECT_RAW, (ano,))
    rows = cur.fetchall()
    col_names = [desc[0] for desc in cur.description]

    inseridos  = 0
    atualizados = 0
    ignorados  = 0

    # Deduplica por CNPJ no próprio resultado RAW — mantém o registro
    # com maior versão / data_referencia (já ordenado pela query)
    cnpjs_vistos = {}
    for row in rows:
        raw = dict(zip(col_names, row))
        cnpj = raw["cnpj_companhia"]
        if cnpj not in cnpjs_vistos:
            cnpjs_vistos[cnpj] = raw   # já está ordenado: primeira = mais recente

    for cnpj, raw in cnpjs_vistos.items():
        # Verifica se CNPJ já existe na Silver
        cur.execute(SQL_CHECK_EXISTS, (cnpj,))
        existente_row = cur.fetchone()

        if existente_row is None:
            # ── INSERT ────────────────────────────────────────────────────────
            cur.execute(SQL_INSERT, raw)
            inseridos += 1
        else:
            col_exi = [desc[0] for desc in cur.description]
            existente = dict(zip(col_exi, existente_row))

            if registros_sao_iguais(raw, existente):
                # ── IGNORA ────────────────────────────────────────────────────
                ignorados += 1
            else:
                # ── UPDATE ────────────────────────────────────────────────────
                cur.execute(SQL_UPDATE, raw)
                atualizados += 1

    return inseridos, atualizados, ignorados


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    log.info("=== Início da carga Silver: cad_cia_aberta ===")

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