"""
Script: load_cad_auditor.py
Descrição: Popula a tabela Silver cvm_data.cad_auditor a partir da RAW
           raw_fca_cia_aberta_auditor, processando todos os anos disponíveis.

Padrão Silver:
  - get_or_create_id_cad: CNPJ ausente no cad_cia_aberta → INSERT mínimo + warning
  - Registro novo (cnpj+ano+versao+id_doc inédito) → INSERT com atualiza_cad=0
  - Mesmo registro com dados diferentes → UPDATE + atualiza_cad+1
  - year_already_loaded: proteção contra reprocessamento duplo

Dependências:
    pip install psycopg2-binary python-dotenv

Arquivo .env:
    DB_USER=postgres  DB_PASSWORD=...  DB_HOST=localhost  DB_PORT=5432  DB_NAME=cvm_data
"""

import os
import sys
import logging
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# ─── CONEXÃO ──────────────────────────────────────────────────────────────────
load_dotenv()
DB_URL = "postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}".format(**{
    k: os.getenv(k, "") for k in ["DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_NAME"]
})

BATCH_SIZE = 500

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("load_cad_auditor.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ─── COLUNAS DE DADOS (excluindo chave e auditoria) ───────────────────────────
DATA_COLS = [
    "data_referencia",
    "nome_empresarial",
    "auditor",
    "cpf_cnpj_auditor",
    "codigo_cvm_auditor",
    "origem_auditor",
    "data_inicio_atuacao_auditor",
    "data_fim_atuacao_auditor",
    "responsavel_tecnico",
    "cpf_responsavel_tecnico",
    "data_inicio_atuacao_responsavel_tecnico",
    "data_fim_atuacao_responsavel_tecnico",
]

# ─── FUNÇÕES ──────────────────────────────────────────────────────────────────

def get_anos(conn) -> list[int]:
    """Retorna os fca_anos distintos disponíveis na RAW, em ordem crescente."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT fca_ano
            FROM cvm_data.raw_fca_cia_aberta_auditor
            ORDER BY fca_ano
        """)
        return [r[0] for r in cur.fetchall()]


def year_already_loaded(conn, ano: int) -> bool:
    """Verifica se o ano já foi carregado na Silver."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(1) FROM cvm_data.cad_auditor WHERE fca_ano = %s",
            (ano,)
        )
        count = cur.fetchone()[0]
    if count > 0:
        log.warning(
            f"fca_ano={ano} já possui {count:,} registros em cad_auditor — pulando. "
            f"Para reprocessar: DELETE FROM cvm_data.cad_auditor WHERE fca_ano = {ano};"
        )
        return True
    return False


def get_or_create_id_cad(conn, cnpj: str) -> int:
    """
    Retorna id_cad_cia_aberta para o CNPJ.
    Se não existir, faz INSERT mínimo e loga warning.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id_cad_cia_aberta FROM cvm_data.cad_cia_aberta WHERE cnpj_companhia = %s",
            (cnpj,)
        )
        row = cur.fetchone()
        if row:
            return row[0]

        # INSERT mínimo — empresa presente na RAW mas ausente no cad_cia_aberta
        log.warning(f"CNPJ {cnpj} ausente em cad_cia_aberta — inserindo registro mínimo.")
        cur.execute("""
            INSERT INTO cvm_data.cad_cia_aberta
                (cnpj_companhia, tipo_participante, indativo_cmp, fca_ano, versao, atualiza_cad)
            VALUES (%s, 'Companhia Aberta', 'não', 0, 0, 0)
            RETURNING id_cad_cia_aberta
        """, (cnpj,))
        conn.commit()
        return cur.fetchone()[0]


def processar_ano(conn, ano: int) -> None:
    """Carrega todos os registros do fca_ano da RAW para a Silver cad_auditor."""

    log.info(f"  Buscando registros RAW para fca_ano={ano}...")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                cnpj_companhia,
                fca_ano,
                id_documento,
                versao,
                data_referencia,
                nome_empresarial,
                auditor,
                cpf_cnpj_auditor,
                codigo_cvm_auditor,
                origem_auditor,
                data_inicio_atuacao_auditor,
                data_fim_atuacao_auditor,
                responsavel_tecnico,
                cpf_responsavel_tecnico,
                data_inicio_atuacao_responsavel_tecnico,
                data_fim_atuacao_responsavel_tecnico
            FROM cvm_data.raw_fca_cia_aberta_auditor
            WHERE fca_ano = %s
            ORDER BY cnpj_companhia, versao, id_documento
        """, (ano,))
        rows = cur.fetchall()

    log.info(f"  {len(rows):,} registros RAW encontrados para fca_ano={ano}.")

    inseridos = 0
    atualizados = 0

    for row in rows:
        (cnpj, fca_ano, id_doc, versao,
         data_ref, nome_emp, auditor, cpf_cnpj_aud, cod_cvm_aud, origem_aud,
         dt_ini_aud, dt_fim_aud, resp_tec, cpf_resp, dt_ini_resp, dt_fim_resp) = row

        if not cnpj:
            continue

        id_cad = get_or_create_id_cad(conn, cnpj)

        with conn.cursor() as cur:
            # Verifica se a combinação chave já existe
            cur.execute("""
                SELECT id_cad_auditor, auditor, cpf_cnpj_auditor, responsavel_tecnico,
                       data_inicio_atuacao_auditor, data_fim_atuacao_auditor
                FROM cvm_data.cad_auditor
                WHERE id_cad_cia_aberta = %s
                  AND fca_ano = %s
                  AND versao = %s
                  AND id_documento = %s
            """, (id_cad, fca_ano, versao, id_doc))
            existing = cur.fetchone()

            if not existing:
                # INSERT novo
                cur.execute("""
                    INSERT INTO cvm_data.cad_auditor (
                        id_cad_cia_aberta, cnpj_companhia, fca_ano, id_documento, versao,
                        data_referencia, nome_empresarial, auditor, cpf_cnpj_auditor,
                        codigo_cvm_auditor, origem_auditor,
                        data_inicio_atuacao_auditor, data_fim_atuacao_auditor,
                        responsavel_tecnico, cpf_responsavel_tecnico,
                        data_inicio_atuacao_responsavel_tecnico,
                        data_fim_atuacao_responsavel_tecnico,
                        atualiza_cad
                    ) VALUES (
                        %s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        0
                    )
                """, (
                    id_cad, cnpj, fca_ano, id_doc, versao,
                    data_ref, nome_emp, auditor, cpf_cnpj_aud, cod_cvm_aud, origem_aud,
                    dt_ini_aud, dt_fim_aud, resp_tec, cpf_resp, dt_ini_resp, dt_fim_resp
                ))
                inseridos += 1

            else:
                # Verifica se algum dado mudou (compara campos relevantes)
                ex_id, ex_auditor, ex_cpf, ex_resp, ex_dt_ini, ex_dt_fim = existing
                mudou = (
                    ex_auditor != auditor or
                    ex_cpf != cpf_cnpj_aud or
                    ex_resp != resp_tec or
                    ex_dt_ini != dt_ini_aud or
                    ex_dt_fim != dt_fim_aud
                )
                if mudou:
                    cur.execute("""
                        UPDATE cvm_data.cad_auditor SET
                            data_referencia                         = %s,
                            nome_empresarial                        = %s,
                            auditor                                 = %s,
                            cpf_cnpj_auditor                        = %s,
                            codigo_cvm_auditor                      = %s,
                            origem_auditor                          = %s,
                            data_inicio_atuacao_auditor             = %s,
                            data_fim_atuacao_auditor                = %s,
                            responsavel_tecnico                     = %s,
                            cpf_responsavel_tecnico                 = %s,
                            data_inicio_atuacao_responsavel_tecnico = %s,
                            data_fim_atuacao_responsavel_tecnico    = %s,
                            atualiza_cad       = atualiza_cad + 1,
                            dt_ultima_atualizacao = NOW()
                        WHERE id_cad_auditor = %s
                    """, (
                        data_ref, nome_emp, auditor, cpf_cnpj_aud, cod_cvm_aud, origem_aud,
                        dt_ini_aud, dt_fim_aud, resp_tec, cpf_resp, dt_ini_resp, dt_fim_resp,
                        ex_id
                    ))
                    atualizados += 1

        conn.commit()

    log.info(f"  fca_ano={ano}: {inseridos:,} inseridos | {atualizados:,} atualizados.")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    log.info("=== Início da carga Silver: cad_auditor ===")

    try:
        conn = psycopg2.connect(DB_URL)
    except psycopg2.OperationalError as e:
        log.error(f"Falha na conexão: {e}")
        sys.exit(1)

    try:
        anos = get_anos(conn)
        log.info(f"Anos disponíveis na RAW: {anos}")

        for ano in anos:
            log.info(f"--- Processando fca_ano={ano} ---")
            if year_already_loaded(conn, ano):
                continue
            processar_ano(conn, ano)

    except Exception as e:
        conn.rollback()
        log.error(f"Erro durante a carga: {e}")
        raise
    finally:
        conn.close()
        log.info("Conexão encerrada.")

    log.info("=== Carga Silver cad_auditor finalizada com sucesso ===")


if __name__ == "__main__":
    main()
