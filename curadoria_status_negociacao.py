"""
Script: curadoria_status_negociacao.py
Descrição: Popula os campos dt_ultimo_pregao e status_negociacao na tabela
           cvm_data.cad_cia_aberta, cruzando dados da raw_fca_cia_aberta_valor_mobiliario
           com regras de negócio para classificar a qualidade do registro.

Regras de classificação (status_negociacao):
  'Ativo'       → tem ação listada, data_fim_listagem IS NULL, tem ticker
  'Suspenso'    → tem ação listada sem ticker OU data_fim_listagem futura
  'Incorporado' → situacao_emissor contém 'Incorpor' ou motivo_cancel = 'ELISÃO POR INCORPORAÇÃO'
  'Cancelado'   → situacao_registro_cvm = 'Cancelada' ou data_fim_listagem preenchida

Dependências:
    pip install psycopg2-binary python-dotenv

Uso:
    python curadoria_status_negociacao.py
"""

import os
import psycopg2
from dotenv import load_dotenv
import logging
import sys

# ─── CONEXÃO ─────────────────────────────────────────────────────────────────
load_dotenv()
DB_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("curadoria_status_negociacao.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ─── QUERY: agrega situação de cada CNPJ na raw_valor_mobiliario ─────────────
SQL_AGREGA_VM = """
    SELECT
        vm.cnpj_companhia,
        -- Tem ação equity ativa (sem data_fim)?
        BOOL_OR(
            vm.valor_mobiliario IN ('Ações Ordinárias','Ações Preferenciais','Units','BDRs')
            AND vm.data_fim_listagem IS NULL
            AND vm.mercado = 'Bolsa'
        ) AS tem_equity_ativo,
        -- Tem ticker em algum registro ativo?
        BOOL_OR(
            vm.codigo_negociacao IS NOT NULL
            AND vm.data_fim_listagem IS NULL
            AND vm.mercado = 'Bolsa'
        ) AS tem_ticker_ativo,
        -- Data mais recente de início de listagem (proxy de último pregão conhecido)
        MAX(
            CASE WHEN vm.data_fim_listagem IS NULL
                 AND vm.mercado = 'Bolsa'
                 THEN vm.data_inicio_listagem END
        ) AS dt_listagem_mais_recente,
        -- Tem data_fim_listagem preenchida em TODOS os registros equity?
        BOOL_AND(
            vm.data_fim_listagem IS NOT NULL
        ) AS todos_encerrados
    FROM cvm_data.raw_fca_cia_aberta_valor_mobiliario vm
    WHERE vm.valor_mobiliario IN (
        'Ações Ordinárias','Ações Preferenciais','Units','BDRs',
        'Certificados de depósito de valores mobiliários'
    )
    AND vm.mercado = 'Bolsa'
    GROUP BY vm.cnpj_companhia
"""

# ─── QUERY: dados do cadastro para cruzar ────────────────────────────────────
SQL_CAD = """
    SELECT
        id_cad_cia_aberta,
        cnpj_companhia,
        situacao_registro_cvm,
        situacao_emissor
    FROM cvm_data.cad_cia_aberta
"""

# ─── QUERY: UPDATE ───────────────────────────────────────────────────────────
SQL_UPDATE = """
    UPDATE cvm_data.cad_cia_aberta SET
        dt_ultimo_pregao      = %(dt_ultimo_pregao)s,
        status_negociacao     = %(status_negociacao)s,
        dt_ultima_atualizacao = NOW()
    WHERE cnpj_companhia = %(cnpj_companhia)s
"""


def classifica_status(cad: dict, vm: dict | None) -> tuple[str, object]:
    """
    Retorna (status_negociacao, dt_ultimo_pregao) para um CNPJ.
    """
    sit_reg     = (cad.get("situacao_registro_cvm") or "").lower()
    sit_emissor = (cad.get("situacao_emissor") or "").lower()

    # ── Sem dados na vm_mobiliario ────────────────────────────────────────────
    if vm is None:
        if "cancelad" in sit_reg:
            return "Cancelado", None
        return "Suspenso", None

    # ── Incorporação / Elisão ─────────────────────────────────────────────────
    if "incorpor" in sit_emissor or "elis" in sit_emissor:
        return "Incorporado", vm.get("dt_listagem_mais_recente")

    # ── Cancelado formalmente ─────────────────────────────────────────────────
    if "cancelad" in sit_reg or vm.get("todos_encerrados"):
        return "Cancelado", vm.get("dt_listagem_mais_recente")

    # ── Ativo com equity e ticker ─────────────────────────────────────────────
    if vm.get("tem_equity_ativo") and vm.get("tem_ticker_ativo"):
        return "Ativo", vm.get("dt_listagem_mais_recente")

    # ── Equity listado mas sem ticker (suspenso/sem negociação ativa) ─────────
    if vm.get("tem_equity_ativo") and not vm.get("tem_ticker_ativo"):
        return "Suspenso", vm.get("dt_listagem_mais_recente")

    # ── Fallback ──────────────────────────────────────────────────────────────
    return "Suspenso", vm.get("dt_listagem_mais_recente")


def main():
    log.info("=== Início: curadoria status_negociacao + dt_ultimo_pregao ===")

    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            # Carrega agregado da tabela de valor mobiliário
            log.info("Carregando dados de raw_fca_cia_aberta_valor_mobiliario...")
            cur.execute(SQL_AGREGA_VM)
            cols_vm = [d[0] for d in cur.description]
            vm_por_cnpj = {
                row[0]: dict(zip(cols_vm, row))
                for row in cur.fetchall()
            }
            log.info(f"  {len(vm_por_cnpj):,} CNPJs com dados de valor mobiliário")

            # Carrega cadastro
            log.info("Carregando cad_cia_aberta...")
            cur.execute(SQL_CAD)
            cols_cad = [d[0] for d in cur.description]
            cad_rows = [dict(zip(cols_cad, row)) for row in cur.fetchall()]
            log.info(f"  {len(cad_rows):,} registros no cadastro")

            # Processa e atualiza
            contadores = {"Ativo": 0, "Suspenso": 0, "Incorporado": 0, "Cancelado": 0}
            updates = []

            for cad in cad_rows:
                cnpj = cad["cnpj_companhia"]
                vm   = vm_por_cnpj.get(cnpj)
                status, dt_pregao = classifica_status(cad, vm)
                contadores[status] += 1
                updates.append({
                    "cnpj_companhia":  cnpj,
                    "status_negociacao": status,
                    "dt_ultimo_pregao":  dt_pregao,
                })

            # Executa updates em lote
            log.info(f"Executando {len(updates):,} updates...")
            for upd in updates:
                cur.execute(SQL_UPDATE, upd)

            conn.commit()

        log.info("=== Curadoria finalizada ===")
        log.info("Distribuição de status_negociacao:")
        for status, qtd in sorted(contadores.items(), key=lambda x: -x[1]):
            log.info(f"  {status:<15} → {qtd:,}")

    except Exception as e:
        conn.rollback()
        log.error(f"Erro: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()