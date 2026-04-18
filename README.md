# bomcontrole-robot

Robô mensal que baixa boletos, notas fiscais e faturas do BomControle, zipa por mês,
publica o zip como GitHub Release e envia o link por email via Resend.

Roda no Railway via cron (dia 27 de cada mês, 3h BRT).

## Fluxo

```
Login headless (Selenium) → captura JWT
        ↓
Download mensal via API BomControle → pdfs_cobranca/AAAA-MM/
        ↓
Zip → cobrancas_AAAA-MM.zip
        ↓
GitHub Release (tag cobrancas-AAAA-MM) → URL pública
        ↓
Email via Resend com link de download
```

## Stack

- Python 3.11 + Selenium (Chromium headless)
- Railway (cron + deploy)
- GitHub Releases (armazenamento do zip)
- Resend (email)

## Arquivos

| Arquivo | Função |
|---|---|
| `run_monthly.py` | Orquestrador (entrypoint do cron) |
| `login_bomcontrole.py` | Login headless → `.api_headers.json` |
| `baixar_faturas.py` | Consome API BomControle, baixa PDFs |
| `upload_release.py` | Cria GitHub Release e faz upload do zip |
| `enviar_email.py` | Envia email via Resend |
| `Dockerfile` | Imagem com Chromium para Railway |
| `railway.json` | Config do deploy + cron schedule |

## Setup local

```bash
cp .env.example .env
# edite .env com API_KEY, LOGIN, SENHA, tokens etc
pip install -r requirements.txt
HEADLESS=false python login_bomcontrole.py   # testa login visualmente
python run_monthly.py --mes 2026-03          # roda um mês específico
```

## Deploy no Railway

1. New Project → Deploy from GitHub repo
2. Em **Variables**, cole todas as chaves do `.env.example`
3. Cron já vem do `railway.json` (`0 6 27 * *`)

## CI/CD

Workflow em `.github/workflows/deploy.yml`:
- Em qualquer push/PR: lint + syntax check
- Em push na `main`: deploy no Railway via CLI

Secrets necessários no repo:
- `RAILWAY_TOKEN` — Account token do Railway
- `RAILWAY_SERVICE` — nome do service no Railway
