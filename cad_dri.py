"""
Script: load_cad_dri.py
Descrição: Popula a tabela Silver cvm_data.cad_dri a partir da RAW
           raw_fca_cia_aberta_dri, processando todos os anos disponíveis.

Padrão Silver idêntico ao cad_auditor.
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
        logging.FileHandler("load_cad_dri.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


def get_anos(conn) -> list[int]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT fca_ano FROM cvm_data.raw_fca_cia_aberta_dri ORDER BY fca_ano
        """)
        return [r[0] for r in cur.fetchall()]


def year_already_loaded(conn, ano: int) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(1) FROM cvm_data.cad_dri WHERE fca_ano = %s", (ano,))
        count = cur.fetchone()[0]
    if count > 0:
        log.warning(
            f"fca_ano={ano} já possui {count:,} registros em cad_dri — pulando. "
            f"Para reprocessar: DELETE FROM cvm_data.cad_dri WHERE fca_ano = {ano};"
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
                cnpj_companhia, fca_ano, id_documento, versao, data_referencia,
                nome_empresarial, tipo_responsavel, responsavel, cpf_responsavel,
                tipo_endereco, logradouro, complemento, bairro, cidade,
                sigla_uf, uf, pais, cep,
                ddi_telefone, ddd_telefone, telefone,
                ddi_fax, ddd_fax, fax, email,
                data_inicio_atuacao, data_fim_atuacao
            FROM cvm_data.raw_fca_cia_aberta_dri
            WHERE fca_ano = %s
            ORDER BY cnpj_companhia, versao, id_documento
        """, (ano,))
        rows = cur.fetchall()

    log.info(f"  {len(rows):,} registros RAW para fca_ano={ano}.")

    inseridos = atualizados = 0

    for row in rows:
        (cnpj, fca_ano, id_doc, versao, data_ref,
         nome_emp, tipo_resp, responsavel, cpf_resp,
         tipo_end, logr, comp, bairro, cidade, sigla_uf, uf, pais, cep,
         ddi_tel, ddd_tel, tel, ddi_fax, ddd_fax, fax, email,
         dt_ini, dt_fim) = row

        if not cnpj:
            continue

        id_cad = get_or_create_id_cad(conn, cnpj)

        with conn.cursor() as cur:
            cur.execute("""
                SELECT id_cad_dri, responsavel, tipo_responsavel, data_inicio_atuacao, data_fim_atuacao
                FROM cvm_data.cad_dri
                WHERE id_cad_cia_aberta = %s AND fca_ano = %s AND versao = %s AND id_documento = %s
            """, (id_cad, fca_ano, versao, id_doc))
            existing = cur.fetchone()

            if not existing:
                cur.execute("""
                    INSERT INTO cvm_data.cad_dri (
                        id_cad_cia_aberta, cnpj_companhia, fca_ano, id_documento, versao,
                        data_referencia, nome_empresarial, tipo_responsavel, responsavel,
                        cpf_responsavel, tipo_endereco, logradouro, complemento, bairro,
                        cidade, sigla_uf, uf, pais, cep,
                        ddi_telefone, ddd_telefone, telefone,
                        ddi_fax, ddd_fax, fax, email,
                        data_inicio_atuacao, data_fim_atuacao, atualiza_cad
                    ) VALUES (
                        %s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,0
                    )
                """, (
                    id_cad, cnpj, fca_ano, id_doc, versao,
                    data_ref, nome_emp, tipo_resp, responsavel, cpf_resp,
                    tipo_end, logr, comp, bairro, cidade, sigla_uf, uf, pais, cep,
                    ddi_tel, ddd_tel, tel, ddi_fax, ddd_fax, fax, email,
                    dt_ini, dt_fim
                ))
                inseridos += 1
            else:
                ex_id, ex_resp, ex_tipo, ex_ini, ex_fim = existing
                if (ex_resp != responsavel or ex_tipo != tipo_resp or
                        ex_ini != dt_ini or ex_fim != dt_fim):
                    cur.execute("""
                        UPDATE cvm_data.cad_dri SET
                            data_referencia=COALESCE(%s,data_referencia),
                            nome_empresarial=%s, tipo_responsavel=%s, responsavel=%s,
                            cpf_responsavel=%s, tipo_endereco=%s, logradouro=%s,
                            complemento=%s, bairro=%s, cidade=%s, sigla_uf=%s, uf=%s,
                            pais=%s, cep=%s, ddi_telefone=%s, ddd_telefone=%s, telefone=%s,
                            ddi_fax=%s, ddd_fax=%s, fax=%s, email=%s,
                            data_inicio_atuacao=%s, data_fim_atuacao=%s,
                            atualiza_cad=atualiza_cad+1, dt_ultima_atualizacao=NOW()
                        WHERE id_cad_dri=%s
                    """, (
                        data_ref, nome_emp, tipo_resp, responsavel, cpf_resp,
                        tipo_end, logr, comp, bairro, cidade, sigla_uf, uf,
                        pais, cep, ddi_tel, ddd_tel, tel, ddi_fax, ddd_fax, fax, email,
                        dt_ini, dt_fim, ex_id
                    ))
                    atualizados += 1

        conn.commit()

    log.info(f"  fca_ano={ano}: {inseridos:,} inseridos | {atualizados:,} atualizados.")


def main():
    log.info("=== Início da carga Silver: cad_dri ===")
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

    log.info("=== Carga Silver cad_dri finalizada com sucesso ===")


if __name__ == "__main__":
    main()
