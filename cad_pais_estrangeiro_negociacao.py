"""
Script: load_cad_pais_estrangeiro_negociacao.py
Descrição: Popula a tabela Silver cvm_data.cad_pais_estrangeiro_negociacao
           a partir da RAW raw_fca_cia_aberta_pais_estrangeiro_negociacao.

Padrão Silver idêntico ao cad_auditor / cad_dri.
"""

import os
import sys
import logging
import psycopg2
from dotenv import load_dotenv

load_dotenv()
DB_URL = "postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}".format(**{
    k: os.getenv(k, "") for k in ["DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_NAME"]
})

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("load_cad_pais_estrangeiro_negociacao.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


def get_anos(conn) -> list[int]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT fca_ano
            FROM cvm_data.raw_fca_cia_aberta_pais_estrangeiro_negociacao
            ORDER BY fca_ano
        """)
        return [r[0] for r in cur.fetchall()]


def year_already_loaded(conn, ano: int) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(1) FROM cvm_data.cad_pais_estrangeiro_negociacao WHERE fca_ano = %s",
            (ano,)
        )
        count = cur.fetchone()[0]
    if count > 0:
        log.warning(
            f"fca_ano={ano} já possui {count:,} registros — pulando. "
            f"Para reprocessar: DELETE FROM cvm_data.cad_pais_estrangeiro_negociacao WHERE fca_ano = {ano};"
        )
        return True
    return False


def get_or_create_id_cad(conn, cnpj: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id_cad_cia_aberta FROM cvm_data.cad_cia_aberta WHERE cnpj_companhia = %s",
            (cnpj,)
        )
        row = cur.fetchone()
        if row:
            return row[0]
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
    log.info(f"  Buscando registros RAW para fca_ano={ano}...")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                cnpj_companhia, fca_ano, id_documento, versao,
                data_referencia, nome_empresarial, pais, data_admissao_negociacao
            FROM cvm_data.raw_fca_cia_aberta_pais_estrangeiro_negociacao
            WHERE fca_ano = %s
            ORDER BY cnpj_companhia, versao, id_documento
        """, (ano,))
        rows = cur.fetchall()

    log.info(f"  {len(rows):,} registros RAW para fca_ano={ano}.")

    inseridos = atualizados = 0

    for row in rows:
        cnpj, fca_ano, id_doc, versao, data_ref, nome_emp, pais, dt_admissao = row

        if not cnpj:
            continue

        id_cad = get_or_create_id_cad(conn, cnpj)

        with conn.cursor() as cur:
            # Chave: empresa + ano + versao + documento + pais (pode ter múltiplos países)
            cur.execute("""
                SELECT id_cad_pais_estrangeiro_negociacao, pais, data_admissao_negociacao
                FROM cvm_data.cad_pais_estrangeiro_negociacao
                WHERE id_cad_cia_aberta = %s AND fca_ano = %s
                  AND versao = %s AND id_documento = %s
                  AND pais = %s
            """, (id_cad, fca_ano, versao, id_doc, pais))
            existing = cur.fetchone()

            if not existing:
                cur.execute("""
                    INSERT INTO cvm_data.cad_pais_estrangeiro_negociacao (
                        id_cad_cia_aberta, cnpj_companhia, fca_ano, id_documento, versao,
                        data_referencia, nome_empresarial, pais, data_admissao_negociacao,
                        atualiza_cad
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,0)
                """, (id_cad, cnpj, fca_ano, id_doc, versao,
                      data_ref, nome_emp, pais, dt_admissao))
                inseridos += 1
            else:
                ex_id, ex_pais, ex_dt = existing
                if ex_dt != dt_admissao:
                    cur.execute("""
                        UPDATE cvm_data.cad_pais_estrangeiro_negociacao SET
                            data_referencia          = %s,
                            nome_empresarial         = %s,
                            data_admissao_negociacao = %s,
                            atualiza_cad             = atualiza_cad + 1,
                            dt_ultima_atualizacao    = NOW()
                        WHERE id_cad_pais_estrangeiro_negociacao = %s
                    """, (data_ref, nome_emp, dt_admissao, ex_id))
                    atualizados += 1

        conn.commit()

    log.info(f"  fca_ano={ano}: {inseridos:,} inseridos | {atualizados:,} atualizados.")


def main():
    log.info("=== Início da carga Silver: cad_pais_estrangeiro_negociacao ===")
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
        log.error(f"Erro: {e}")
        raise
    finally:
        conn.close()
        log.info("Conexão encerrada.")

    log.info("=== Carga Silver cad_pais_estrangeiro_negociacao finalizada ===")


if __name__ == "__main__":
    main()
