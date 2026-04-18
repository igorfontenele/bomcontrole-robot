#!/usr/bin/env python3
"""
enviar_email.py
===============
Envia email via Resend com link de download do zip de cobranças.
"""

import logging
import os
import sys

import requests

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

RESEND_API = "https://api.resend.com/emails"


def enviar_link_cobrancas(mes_label, download_url, tamanho_mb, stats=None):
    api_key = os.getenv("RESEND_API_KEY", "")
    sender  = os.getenv("RESEND_FROM_EMAIL", "onboarding@resend.dev")
    to      = os.getenv("RESEND_TO_EMAIL", "")
    if not api_key or not to:
        raise RuntimeError("RESEND_API_KEY e RESEND_TO_EMAIL são obrigatórios (env).")

    stats = stats or {}
    resumo_linhas = []
    if stats.get("boleto_ok"):  resumo_linhas.append(f"• {stats['boleto_ok']} boletos")
    if stats.get("nf_ok"):      resumo_linhas.append(f"• {stats['nf_ok']} notas fiscais")
    if stats.get("fatura_ok"):  resumo_linhas.append(f"• {stats['fatura_ok']} faturas")
    resumo = "<br/>".join(resumo_linhas) if resumo_linhas else "—"

    html = f"""
    <div style="font-family: -apple-system, Segoe UI, Roboto, sans-serif; max-width: 560px; margin: 0 auto;">
      <h2 style="color: #111;">Cobranças — {mes_label}</h2>
      <p>Oi, Paula! O robô terminou de baixar os PDFs de cobrança do mês <strong>{mes_label}</strong>.</p>
      <p><strong>Resumo:</strong><br/>{resumo}</p>
      <p>
        <a href="{download_url}" style="display: inline-block; background: #2563eb; color: #fff; padding: 12px 20px; border-radius: 6px; text-decoration: none; font-weight: 600;">
          Baixar ZIP ({tamanho_mb} MB)
        </a>
      </p>
      <p style="color: #666; font-size: 13px;">
        Link direto: <a href="{download_url}">{download_url}</a>
      </p>
      <hr style="border: none; border-top: 1px solid #eee; margin: 24px 0;"/>
      <p style="color: #999; font-size: 12px;">
        Este email foi enviado automaticamente. Em caso de dúvidas, fale com o Igor.
      </p>
    </div>
    """.strip()

    payload = {
        "from":    sender,
        "to":      [to] if isinstance(to, str) else list(to),
        "subject": f"Cobranças {mes_label} — PDFs disponíveis",
        "html":    html,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json",
    }

    log.info("Enviando email para %s...", to)
    r = requests.post(RESEND_API, json=payload, headers=headers, timeout=30)
    if r.status_code >= 400:
        log.error("Resend falhou (%d): %s", r.status_code, r.text)
        r.raise_for_status()

    resposta = r.json()
    log.info("✔ Email enviado (id=%s)", resposta.get("id"))
    return resposta


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mes", required=True)
    parser.add_argument("--url", required=True)
    parser.add_argument("--mb", type=int, default=0)
    args = parser.parse_args()
    try:
        enviar_link_cobrancas(args.mes, args.url, args.mb)
    except Exception as e:
        log.error("%s", e)
        sys.exit(1)
