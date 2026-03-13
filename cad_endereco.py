"""
Script: load_cad_endereco.py
Descrição: Popula a tabela Silver cvm_data.cad_endereco
           a partir da RAW raw_fca_cia_aberta_endereco.

Padrão Silver idêntico ao cad_auditor / cad_dri / cad_pais_estrangeiro_negociacao.

Nota: a chave de unicidade inclui tipo_endereco, pois uma empresa pode ter
      múltiplos endereços (sede, correspondência, etc.) no mesmo documento.
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
        logging.FileHandler("load_cad_endereco.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


def get_anos(conn) -> list[int]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT fca_ano FROM cvm_data.raw_fca_cia_aberta_endereco ORDER BY fca_ano
        """)
        return [r[0] for r in cur.fetchall()]


def year_already_loaded(conn, ano: int) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(1) FROM cvm_data.cad_endereco WHERE fca_ano = %s", (ano,)
        )
        count = cur.fetchone()[0]
    if count > 0:
        log.warning(
            f"fca_ano={ano} já possui {count:,} registros em cad_endereco — pulando. "
            f"Para reprocessar: DELETE FROM cvm_data.cad_endereco WHERE fca_ano = {ano};"
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
                nome_empresarial, tipo_endereco, logradouro, complemento, bairro,
                cidade, sigla_uf, pais, cep, caixa_postal,
                ddi_telefone, ddd_telefone, telefone,
                ddi_fax, ddd_fax, fax, email
            FROM cvm_data.raw_fca_cia_aberta_endereco
            WHERE fca_ano = %s
            ORDER BY cnpj_companhia, versao, id_documento, tipo_endereco
        """, (ano,))
        rows = cur.fetchall()

    log.info(f"  {len(rows):,} registros RAW para fca_ano={ano}.")

    inseridos = atualizados = 0

    for row in rows:
        (cnpj, fca_ano, id_doc, versao, data_ref,
         nome_emp, tipo_end, logr, comp, bairro, cidade, sigla_uf, pais, cep, cx_postal,
         ddi_tel, ddd_tel, tel, ddi_fax, ddd_fax, fax, email) = row

        if not cnpj:
            continue

        id_cad = get_or_create_id_cad(conn, cnpj)

        with conn.cursor() as cur:
            # Chave: empresa + ano + versao + documento + tipo_endereco
            cur.execute("""
                SELECT id_cad_endereco, logradouro, cidade, cep
                FROM cvm_data.cad_endereco
                WHERE id_cad_cia_aberta = %s AND fca_ano = %s
                  AND versao = %s AND id_documento = %s
                  AND COALESCE(tipo_endereco,'') = COALESCE(%s,'')
            """, (id_cad, fca_ano, versao, id_doc, tipo_end))
            existing = cur.fetchone()

            if not existing:
                cur.execute("""
                    INSERT INTO cvm_data.cad_endereco (
                        id_cad_cia_aberta, cnpj_companhia, fca_ano, id_documento, versao,
                        data_referencia, nome_empresarial, tipo_endereco,
                        logradouro, complemento, bairro, cidade, sigla_uf, pais, cep, caixa_postal,
                        ddi_telefone, ddd_telefone, telefone,
                        ddi_fax, ddd_fax, fax, email,
                        atualiza_cad
                    ) VALUES (
                        %s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,0
                    )
                """, (
                    id_cad, cnpj, fca_ano, id_doc, versao,
                    data_ref, nome_emp, tipo_end, logr, comp, bairro, cidade,
                    sigla_uf, pais, cep, cx_postal,
                    ddi_tel, ddd_tel, tel, ddi_fax, ddd_fax, fax, email
                ))
                inseridos += 1
            else:
                ex_id, ex_logr, ex_cidade, ex_cep = existing
                if ex_logr != logr or ex_cidade != cidade or ex_cep != cep:
                    cur.execute("""
                        UPDATE cvm_data.cad_endereco SET
                            data_referencia = %s, nome_empresarial = %s,
                            logradouro = %s, complemento = %s, bairro = %s,
                            cidade = %s, sigla_uf = %s, pais = %s, cep = %s,
                            caixa_postal = %s, ddi_telefone = %s, ddd_telefone = %s,
                            telefone = %s, ddi_fax = %s, ddd_fax = %s, fax = %s,
                            email = %s,
                            atualiza_cad = atualiza_cad + 1,
                            dt_ultima_atualizacao = NOW()
                        WHERE id_cad_endereco = %s
                    """, (
                        data_ref, nome_emp, logr, comp, bairro, cidade, sigla_uf,
                        pais, cep, cx_postal, ddi_tel, ddd_tel, tel,
                        ddi_fax, ddd_fax, fax, email, ex_id
                    ))
                    atualizados += 1

        conn.commit()

    log.info(f"  fca_ano={ano}: {inseridos:,} inseridos | {atualizados:,} atualizados.")


def main():
    log.info("=== Início da carga Silver: cad_endereco ===")
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

    log.info("=== Carga Silver cad_endereco finalizada ===")


if __name__ == "__main__":
    main()
