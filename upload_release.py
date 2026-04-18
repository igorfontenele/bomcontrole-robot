#!/usr/bin/env python3
"""
upload_release.py
=================
Cria uma GitHub Release e faz upload de um arquivo como asset.
Retorna a URL pública de download.

Uso programático:
    url = criar_release_e_upload(zip_path=Path("pdfs_cobranca/2026-03.zip"),
                                 tag="cobrancas-2026-03",
                                 titulo="Cobranças 03/2026",
                                 notas="Gerado automaticamente em 27/04/2026")

Env vars:
    GITHUB_TOKEN  — PAT com escopo "contents: write" (ou "repo" classic)
    GITHUB_REPO   — "owner/repo"
"""

import logging
import os
import sys
from pathlib import Path

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

GITHUB_API = "https://api.github.com"


def _headers(token, accept="application/vnd.github+json"):
    return {
        "Authorization": f"Bearer {token}",
        "Accept":        accept,
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _obter_release_por_tag(repo, tag, token):
    r = requests.get(
        f"{GITHUB_API}/repos/{repo}/releases/tags/{tag}",
        headers=_headers(token), timeout=30,
    )
    if r.status_code == 200:
        return r.json()
    if r.status_code == 404:
        return None
    r.raise_for_status()


def _criar_release(repo, tag, titulo, notas, token):
    r = requests.post(
        f"{GITHUB_API}/repos/{repo}/releases",
        headers=_headers(token),
        json={
            "tag_name": tag,
            "name":     titulo,
            "body":     notas,
            "draft":    False,
            "prerelease": False,
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def _deletar_asset_existente(release, nome_arquivo, token):
    for asset in release.get("assets", []):
        if asset.get("name") == nome_arquivo:
            log.info("Removendo asset anterior com mesmo nome: %s", nome_arquivo)
            requests.delete(asset["url"], headers=_headers(token), timeout=30).raise_for_status()


def _upload_asset(release, arquivo, token):
    upload_url = release["upload_url"].split("{")[0]
    nome = arquivo.name
    with open(arquivo, "rb") as f:
        r = requests.post(
            upload_url,
            headers={
                **_headers(token, accept="application/vnd.github+json"),
                "Content-Type": "application/zip",
            },
            params={"name": nome},
            data=f,
            timeout=600,
        )
    r.raise_for_status()
    return r.json()


def criar_release_e_upload(zip_path, tag, titulo, notas):
    token = os.getenv("GITHUB_TOKEN", "")
    repo  = os.getenv("GITHUB_REPO", "")
    if not token or not repo:
        raise RuntimeError("GITHUB_TOKEN e GITHUB_REPO são obrigatórios (env).")
    if not zip_path.exists():
        raise FileNotFoundError(zip_path)

    log.info("Checando release existente: %s", tag)
    release = _obter_release_por_tag(repo, tag, token)
    if release:
        log.info("Release %s já existe — reutilizando.", tag)
        _deletar_asset_existente(release, zip_path.name, token)
    else:
        log.info("Criando release %s...", tag)
        release = _criar_release(repo, tag, titulo, notas, token)

    log.info("Upload do asset %s (%d MB)...",
             zip_path.name, zip_path.stat().st_size // (1024 * 1024))
    asset = _upload_asset(release, zip_path, token)

    download_url = asset["browser_download_url"]
    log.info("✔ Upload OK: %s", download_url)
    return download_url


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--zip", required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--titulo", required=True)
    parser.add_argument("--notas", default="")
    args = parser.parse_args()

    try:
        url = criar_release_e_upload(
            Path(args.zip), args.tag, args.titulo, args.notas,
        )
        print(url)
    except Exception as e:
        log.error("Falhou: %s", e)
        sys.exit(1)
