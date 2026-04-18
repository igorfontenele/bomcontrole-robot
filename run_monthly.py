#!/usr/bin/env python3
"""
run_monthly.py
==============
Orquestrador do robô mensal. Executa em sequência:

  1. Login headless no BomControle (captura JWT)
  2. Baixa boletos/NFs/faturas do mês de referência
  3. Zipa a pasta do mês
  4. Sobe o zip no GitHub Releases
  5. Envia email via Resend com o link

Uso:
    python run_monthly.py                 # mês posterior (ex: rodando em 27/abr → mai)
    python run_monthly.py --mes 2026-05   # mês específico
    MES_REFERENCIA=posterior python run_monthly.py
"""

import argparse
import calendar
import logging
import os
import shutil
import sys
from datetime import date
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def _mes_posterior(hoje=None):
    hoje = hoje or date.today()
    ano, mes = hoje.year, hoje.month + 1
    if mes == 13:
        ano, mes = ano + 1, 1
    return ano, mes


def _mes_anterior(hoje=None):
    hoje = hoje or date.today()
    ano, mes = hoje.year, hoje.month - 1
    if mes == 0:
        ano, mes = ano - 1, 12
    return ano, mes


def _parse_mes(s):
    try:
        ano, mes = int(s[:4]), int(s[5:7])
        if not (1 <= mes <= 12):
            raise ValueError
        return ano, mes
    except Exception:
        raise SystemExit(f"Formato inválido: {s!r}. Use AAAA-MM (ex: 2026-05).")


def _resolver_mes(cli_mes=None):
    fonte = cli_mes or os.getenv("MES_REFERENCIA", "posterior")
    if fonte == "posterior":
        ano, mes = _mes_posterior()
    elif fonte == "anterior":
        ano, mes = _mes_anterior()
    else:
        ano, mes = _parse_mes(fonte)
    return ano, mes


def _rodar_login():
    from login_bomcontrole import fazer_login
    log.info("═══ ETAPA 1/5: Login headless BomControle ═══")
    if not fazer_login():
        raise SystemExit("Login falhou — abortando execução.")


def _rodar_download(ano, mes):
    import baixar_faturas as bf
    log.info("═══ ETAPA 2/5: Download do mês %04d-%02d ═══", ano, mes)
    bf._validar_jwt()
    ultimo = calendar.monthrange(ano, mes)[1]
    ini    = f"{ano:04d}-{mes:02d}-01 00:00:00"
    fim    = f"{ano:04d}-{mes:02d}-{ultimo:02d} 23:59:59"
    label  = f"{ano:04d}-{mes:02d}"
    stats, output_dir = bf.processar(ini, fim, label, buscar_fatura=True)
    return label, output_dir, stats


def _zipar(output_dir, label):
    log.info("═══ ETAPA 3/5: Compactando %s ═══", output_dir)
    zip_base = output_dir.parent / f"cobrancas_{label}"
    zip_path = shutil.make_archive(str(zip_base), "zip", root_dir=str(output_dir))
    zip_path = Path(zip_path)
    mb = zip_path.stat().st_size // (1024 * 1024)
    log.info("Zip criado: %s (%d MB)", zip_path, mb)
    return zip_path, mb


def _upload(zip_path, label):
    from upload_release import criar_release_e_upload
    log.info("═══ ETAPA 4/5: Upload GitHub Release ═══")
    return criar_release_e_upload(
        zip_path=zip_path,
        tag=f"cobrancas-{label}",
        titulo=f"Cobranças {label}",
        notas=f"Gerado automaticamente pelo robô em {date.today().isoformat()}.",
    )


def _email(label, url, mb, stats):
    from enviar_email import enviar_link_cobrancas
    log.info("═══ ETAPA 5/5: Email via Resend ═══")
    enviar_link_cobrancas(label, url, mb, stats)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mes", help="AAAA-MM; default lê MES_REFERENCIA do env (ou 'posterior').")
    parser.add_argument("--pular-login", action="store_true",
                        help="Reutiliza .api_headers.json existente (debug).")
    args = parser.parse_args()

    ano, mes = _resolver_mes(args.mes)
    log.info("Mês de referência: %04d-%02d", ano, mes)

    if not args.pular_login:
        _rodar_login()
    else:
        log.info("Pulando login (--pular-login).")

    label, output_dir, stats = _rodar_download(ano, mes)

    if not output_dir.exists() or not any(output_dir.iterdir()):
        log.warning("Nenhum PDF baixado para %s — nada a enviar.", label)
        return 0

    zip_path, mb = _zipar(output_dir, label)
    url = _upload(zip_path, label)
    _email(label, url, mb, stats)

    log.info("✔ Fluxo completo. Link: %s", url)
    return 0


if __name__ == "__main__":
    sys.exit(main())
