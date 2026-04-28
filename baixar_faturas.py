"""
bomcontrole_download_pdfs.py
============================
Baixa automaticamente boletos (PDF) e notas fiscais (PDF) de cada
movimentação financeira via API do BomControle, salvando em estrutura:

    pdfs_cobranca/
        2025-01/
            NOME_DO_CLIENTE/
                boleto_YYYYMMDD_pN_VALOR_ID.pdf
                nf_YYYYMMDD_pN_VALOR_ID.pdf
        2025-02/
            ...

Cache local (por período):
    .cache_movimentacoes_PERIODO.json  — paginação (por mês/intervalo)
    .cache_faturas.json                — Fatura/Obter (compartilhado)
    .cache_baixados.json               — PDFs já baixados (compartilhado)

Se o script travar, basta rodar novamente: retoma de onde parou.

Uso:
    pip install requests

    python bomcontrole_download_pdfs.py --ano 2025
    python bomcontrole_download_pdfs.py --mes 2025-04
    python bomcontrole_download_pdfs.py --inicio 2025-04-01 --termino 2025-04-30
    python bomcontrole_download_pdfs.py          # usa DATA_INICIO/DATA_TERMINO do script
"""

import argparse
import calendar
import json
import os
import re
import time
import logging
from pathlib import Path

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ---------------------------------------------------------------------------
# CONFIGURAÇÃO (lê de env vars; mantém defaults seguros pra dev local)
# ---------------------------------------------------------------------------

API_KEY  = os.getenv("BOMCONTROLE_API_KEY", "")
BASE_URL = os.getenv("BOMCONTROLE_BASE_URL",
                     "https://apinewintegracao.bomcontrole.com.br/integracao")

# API interna do frontend — retorna LinkFatura via ObterMovimento.
# Não usa ApiKey, usa Bearer JWT obtido do browser (captura via login_bomcontrole.py).
# O token dura ~12h; o robô faz login headless a cada execução pra ter token fresco.
BASE_URL_INTERNO = os.getenv("BOMCONTROLE_BASE_URL_INTERNO",
                             "https://apinew.bomcontrole.com.br/api")
TENANT_ID        = os.getenv("BOMCONTROLE_TENANT_ID", "29860")
# JWT é lido preferencialmente de .api_headers.json (gravado pelo login headless).
# Env BOMCONTROLE_JWT_TOKEN serve como override manual (debug).
JWT_TOKEN        = os.getenv("BOMCONTROLE_JWT_TOKEN", "")

OUTPUT_DIR_BASE = Path("pdfs_cobranca")

# Usado apenas quando nenhum argumento de data é passado na CLI
DATA_INICIO  = "2025-01-01 00:00:00"
DATA_TERMINO = "2025-12-31 23:59:59"

# Tipo de data para filtrar as movimentações. Opções:
#   DataPadrao | DataPrevista | DataPagamento | DataCompetencia |
#   DataConciliacao | Criacao | UltimaAlteracao
TIPO_DATA = "DataVencimento"

FILTRO_STATUS  = None          # "Atrasado" | "Agendado" | "Quitado" | None
FILTRO_DESPESA = False         # False = só receitas (recomendado — exclui despesas e lançamentos internos)

ITENS_POR_PAGINA = 100

DELAY_LISTAGEM = 2.0           # segundos entre páginas da paginação
DELAY_FATURA   = 3.0           # segundos entre chamadas de Fatura/Obter (aumentado para evitar 429)
DELAY_DOWNLOAD = 0.3           # segundos entre downloads de PDF

MAX_RETRIES = 6

# BUSCAR_FATURA_FALLBACK: desabilitado — causa 429 excessivos pois a maioria
# das movimentações tem IdFatura mas a fatura retorna 400 (cancelada/inválida).
# LinkBoleto e LinkNF já vêm direto no Financeiro/Pesquisar para quem os tem.
# LinkFatura vem via JWT_TOKEN (ObterMovimento) — configure usando renovar_token.py
BUSCAR_FATURA_FALLBACK = False

# True = chama ObterMovimento (API interna) para obter LinkFatura
# Só funciona se TENANT_ID estiver correto acima
BUSCAR_LINK_FATURA = True

CACHE_DIR        = Path(".")
CACHE_FATURAS    = CACHE_DIR / ".cache_faturas.json"
CACHE_BAIXADOS   = CACHE_DIR / ".cache_baixados.json"
CACHE_MOVIMENTOS = CACHE_DIR / ".cache_movimentos.json"   # cache do ObterMovimento
API_HEADERS_JSON = CACHE_DIR / ".api_headers.json"        # receita salva pelo renovar_token.py

USER_AGENT_PADRAO = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def _cache_load(path: Path):
    if path.exists():
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {}


def _cache_save(path: Path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# HTTP com retry + backoff exponencial
# ---------------------------------------------------------------------------

def _headers():
    return {"Authorization": f"ApiKey {API_KEY}"}


def _request_com_retry(method, url, usa_auth=True, **kwargs):
    if usa_auth:
        kwargs.setdefault("headers", {}).update(_headers())

    for tentativa in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.request(method, url, timeout=60, **kwargs)

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                espera = float(retry_after) if retry_after else 5 * (2 ** (tentativa - 1))
                log.warning("429 — aguardando %.0fs (tentativa %d/%d)",
                            espera, tentativa, MAX_RETRIES)
                time.sleep(espera)
                continue

            if resp.status_code >= 500:
                espera = 5 * (2 ** (tentativa - 1))
                log.warning("HTTP %d — backoff %.0fs (tentativa %d/%d)",
                            resp.status_code, espera, tentativa, MAX_RETRIES)
                time.sleep(espera)
                continue

            resp.raise_for_status()
            return resp

        except requests.exceptions.ConnectionError as e:
            espera = 5 * (2 ** (tentativa - 1))
            log.warning("Conexão falhou (%s) — backoff %.0fs (tentativa %d/%d)",
                        e, espera, tentativa, MAX_RETRIES)
            time.sleep(espera)

    raise RuntimeError(f"Falhou após {MAX_RETRIES} tentativas: {url}")


def _get(path, params=None):
    url = f"{BASE_URL}/{path.lstrip('/')}"
    resp = _request_com_retry("GET", url, params=params or {})
    return resp.json()


_API_HEADERS_CACHE = None   # memoiza a leitura de .api_headers.json


def _token_do_recipe(recipe):
    """Extrai o JWT (sem 'Bearer ') de dentro do dict de headers gravado pelo login."""
    if not recipe or not isinstance(recipe, dict):
        return None
    headers = recipe.get("headers") or {}
    for k, v in headers.items():
        if k.lower() == "authorization" and isinstance(v, str) and "eyJ" in v:
            return v.split(" ")[-1].strip()
    return None


def _carregar_headers_interno():
    """
    Carrega headers reais capturados pelo login (.api_headers.json).
    Ordem de precedência para o token Bearer:
      1. env BOMCONTROLE_JWT_TOKEN (override manual)
      2. Authorization do próprio .api_headers.json (gravado pelo login headless)
    Fallback: se o arquivo não existir, usa headers mínimos (provavelmente vai dar 403).
    """
    global _API_HEADERS_CACHE
    if _API_HEADERS_CACHE is not None:
        return dict(_API_HEADERS_CACHE)

    recipe = None
    if API_HEADERS_JSON.exists():
        try:
            recipe = json.loads(API_HEADERS_JSON.read_text(encoding="utf-8"))
        except Exception as e:
            log.warning("Falha ao ler %s: %s — usando headers padrão.",
                        API_HEADERS_JSON, e)
            recipe = None

    token = JWT_TOKEN or _token_do_recipe(recipe) or ""

    if recipe and isinstance(recipe, dict) and recipe.get("headers"):
        headers = {}
        skip = {"authorization", "content-length", "host",
                "content-type", ":authority", ":method", ":path", ":scheme"}
        for k, v in recipe["headers"].items():
            if k.lower() in skip or k.startswith(":"):
                continue
            headers[k] = v
        headers["Authorization"] = f"Bearer {token}"
        headers.setdefault("Accept", "application/json, text/plain, */*")
        headers.setdefault("User-Agent", USER_AGENT_PADRAO)
        if recipe.get("cookies"):
            headers["Cookie"] = recipe["cookies"]
        log.debug("Headers internos carregados de %s (%d campos)",
                  API_HEADERS_JSON, len(headers))
    else:
        headers = {
            "Authorization":   f"Bearer {token}",
            "Accept":          "application/json, text/plain, */*",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
            "User-Agent":      USER_AGENT_PADRAO,
            "Origin":          "https://new.bomcontrole.com.br",
            "Referer":         "https://new.bomcontrole.com.br/",
            "x-tenant-id":     TENANT_ID,
        }
        log.warning("⚠ %s não encontrado — usando headers padrão (pode dar 403).",
                    API_HEADERS_JSON)
        log.warning("  Rode o login headless antes: python login_bomcontrole.py")

    _API_HEADERS_CACHE = headers
    return dict(headers)


def _jwt_atual():
    """Retorna o JWT que será usado (env override > recipe)."""
    if JWT_TOKEN:
        return JWT_TOKEN
    if API_HEADERS_JSON.exists():
        try:
            return _token_do_recipe(json.loads(API_HEADERS_JSON.read_text(encoding="utf-8"))) or ""
        except Exception:
            return ""
    return ""


def _get_interno(path, params=None):
    """Chama a API interna do frontend (apinew.bomcontrole.com.br) replicando os headers do browser."""
    url = f"{BASE_URL_INTERNO}/{path.lstrip('/')}"
    headers = _carregar_headers_interno()
    resp = _request_com_retry("GET", url, usa_auth=False,
                              headers=headers, params=params or {})
    return resp.json()


_LINK_FATURA_KEYS = (
    "LinkFatura", "linkFatura",
    "LinkCobranca", "linkCobranca",
    "UrlFatura", "urlFatura",
    "LinkFaturaCobranca", "LinkFaturaDigital",
)

# Usado para inspecionar a estrutura real da resposta uma única vez
_OBTER_MOVIMENTO_SAMPLE_LOGGED = False


def _extrair_link_fatura(data):
    """Procura a URL da fatura na resposta de ObterMovimento, tolerando variações de nome."""
    if not isinstance(data, dict):
        return None
    # 1) tenta campos conhecidos no topo
    for k in _LINK_FATURA_KEYS:
        v = data.get(k)
        if isinstance(v, str) and v.startswith("http"):
            return v
    # 2) varredura recursiva — qualquer chave que contenha "fatura" e valor http
    def _walk(obj):
        if isinstance(obj, dict):
            for kk, vv in obj.items():
                if isinstance(vv, str) and vv.startswith("http") and "fatura" in kk.lower():
                    return vv
                found = _walk(vv)
                if found:
                    return found
        elif isinstance(obj, list):
            for it in obj:
                found = _walk(it)
                if found:
                    return found
        return None
    return _walk(data)


def _buscar_link_fatura(guid_parcela):
    """
    Busca o LinkFatura via API interna ObterMovimento.
    Usa cache em CACHE_MOVIMENTOS para não repetir chamadas.
    Retorna a URL string ou None.
    """
    global _OBTER_MOVIMENTO_SAMPLE_LOGGED

    if not BUSCAR_LINK_FATURA or not guid_parcela:
        return None

    cache = _cache_load(CACHE_MOVIMENTOS)
    chave = str(guid_parcela)

    if chave in cache:
        entry = cache[chave]
        # Cache com marcador de erro → tenta novamente nesta execução
        if isinstance(entry, dict) and entry.get("erro"):
            pass
        else:
            return entry  # None ou string

    try:
        time.sleep(DELAY_FATURA)
        data = _get_interno("MovimentacaoFinanceira/ObterMovimento",
                            params={"idParcela": guid_parcela})

        # Loga a estrutura na primeira chamada bem-sucedida para diagnóstico
        if not _OBTER_MOVIMENTO_SAMPLE_LOGGED and isinstance(data, dict):
            _OBTER_MOVIMENTO_SAMPLE_LOGGED = True
            keys_url = {k: v for k, v in data.items()
                        if isinstance(v, str) and v.startswith("http")}
            log.info("  ObterMovimento amostra — chaves: %s", list(data.keys())[:30])
            if keys_url:
                log.info("  ObterMovimento URLs no topo: %s", keys_url)

        link = _extrair_link_fatura(data)
        cache[chave] = link
        if link:
            log.info("  LinkFatura obtido para %s", guid_parcela)
    except Exception as e:
        log.warning("  ObterMovimento/%s → %s", guid_parcela, e)
        # Marca como erro (não como None) para permitir retry em execuções futuras
        cache[chave] = {"erro": True, "msg": str(e)[:200]}

    _cache_save(CACHE_MOVIMENTOS, cache)
    entry = cache[chave]
    return entry if isinstance(entry, str) else None


def _download_pdf(url, dest):
    try:
        # PDFs no S3 são públicos — tenta sem auth primeiro
        resp = _request_com_retry("GET", url, usa_auth=False,
                                  allow_redirects=True, stream=True)
        # Se retornou HTML (redirect para login), tenta com auth
        if "html" in resp.headers.get("Content-Type", ""):
            resp = _request_com_retry("GET", url, usa_auth=True,
                                      allow_redirects=True, stream=True)
        dest.parent.mkdir(parents=True, exist_ok=True)
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        log.info("    ✔ %s (%d KB)", dest, dest.stat().st_size // 1024)
        return True
    except Exception as e:
        log.warning("    ✘ %s → %s", url, e)
        return False


# ---------------------------------------------------------------------------
# Sanitização de nomes
# ---------------------------------------------------------------------------

def _safe(text):
    text = str(text).strip()
    text = re.sub(r'[\\/:*?"<>|]', "_", text)
    text = re.sub(r'\s+', "_", text)
    return text or "sem_nome"


# ---------------------------------------------------------------------------
# Extração de campos de cada item
# ---------------------------------------------------------------------------

def _nome_cliente(item):
    return (
        item.get("NomeFantasiaClienteFornecedor")
        or item.get("NomeClienteFornecedor")
        or "cliente_desconhecido"
    )


def _tem_cliente(item):
    """
    Retorna True somente se a movimentação tem cliente real vinculado.
    Usado para evitar chamar Fatura/Obter em lançamentos internos/bancários.
    """
    id_cliente = item.get("IdCliente")
    # Sem IdCliente = lançamento sem cliente (despesa, transferência, etc.)
    if not id_cliente or int(id_cliente) <= 0:
        return False
    # Nome deve ser identificado (não placeholder)
    nome = _nome_cliente(item)
    placeholders = {
        "cliente_desconhecido", "NÃO IDENTIFICADO", "", "BRADESCO",
        "CAIXA ECONÔMICA FEDERAL", "ITAÚ UNIBANCO", "SANTANDER", 
        "SICOOB", "SICREDI", "BANCO DO BRASIL", "NUBANK",
    }
    return nome not in placeholders


def _sufixo_arquivo(item):
    venc       = str(item.get("DataVencimento", ""))[:10].replace("-", "")
    parcela    = item.get("NumeroParcela", "")
    p_str      = f"p{parcela}" if parcela else ""
    valor      = item.get("Valor", "")
    v_str      = f"{valor:.2f}".replace(".", ",") if isinstance(valor, (int, float)) else str(valor)
    guid       = str(item.get("IdMovimentacaoFinanceiraParcela", ""))
    guid_curto = guid.split("-")[0] if "-" in guid else guid[:8]
    return "_".join(x for x in [venc, p_str, v_str, guid_curto] if x)


# ---------------------------------------------------------------------------
# Listagem paginada com cache por período
# ---------------------------------------------------------------------------

def listar_movimentacoes(ini, fim):
    cache_key  = f"{ini}|{fim}|{TIPO_DATA}|{FILTRO_STATUS}|{FILTRO_DESPESA}"
    cache_slug = re.sub(r'[^a-zA-Z0-9_-]', "_", f"{ini[:10]}_{fim[:10]}")
    cache_path = CACHE_DIR / f".cache_movimentacoes_{cache_slug}.json"

    cache = _cache_load(cache_path)
    if isinstance(cache, dict) and cache.get("cache_key") == cache_key:
        todas = cache.get("itens", [])
        log.info("Cache carregado: %d itens (%s → %s) — pulando paginação",
                 len(todas), ini[:10], fim[:10])
        return todas

    usa_detalhada = (FILTRO_STATUS is not None) or (FILTRO_DESPESA is not None)
    endpoint = "Financeiro/PesquisaDetalhada" if usa_detalhada else "Financeiro/Pesquisar"

    params_base = {
        "dataInicio":               ini,
        "dataTermino":              fim,
        "tipoData":                 TIPO_DATA,
        "paginacao.itensPorPagina": ITENS_POR_PAGINA,
    }
    if usa_detalhada:
        if FILTRO_STATUS  is not None: params_base["status"]  = FILTRO_STATUS
        if FILTRO_DESPESA is not None: params_base["despesa"] = str(FILTRO_DESPESA).lower()

    todas, pagina = [], 1
    while True:
        log.info("Buscando movimentações — página %d …", pagina)
        time.sleep(DELAY_LISTAGEM)
        data  = _get(endpoint, {**params_base, "paginacao.numeroDaPagina": pagina})
        itens = data.get("Itens", [])
        if not itens:
            break
        todas.extend(itens)
        total = data.get("TotalItens", len(todas))
        log.info("  %d / %d carregados", len(todas), total)
        # Salva incrementalmente (protege contra crash durante paginação)
        _cache_save(cache_path, {"cache_key": cache_key, "itens": todas})
        if len(todas) >= total:
            break
        pagina += 1

    log.info("Total de movimentações: %d", len(todas))
    return todas


# ---------------------------------------------------------------------------
# Fatura/Obter com cache compartilhado (usado só se BUSCAR_FATURA_FALLBACK=True)
# ---------------------------------------------------------------------------

def _buscar_fatura(id_fatura):
    cache = _cache_load(CACHE_FATURAS)
    chave = str(id_fatura)
    if chave in cache:
        return cache[chave]
    try:
        time.sleep(DELAY_FATURA)
        fatura = _get(f"Fatura/Obter/{id_fatura}")
        cache[chave] = fatura
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else 0
        log.warning("  Fatura/%s → HTTP %d (cacheando)", id_fatura, status)
        cache[chave] = {"erro": True, "status": status}
    except Exception as e:
        log.warning("  Fatura/%s → %s", id_fatura, e)
        cache[chave] = {"erro": True}
    _cache_save(CACHE_FATURAS, cache)
    return cache[chave]


def _links_via_fatura(id_fatura):
    """
    Retorna (link_boleto, link_nf, link_fatura) via Fatura/Obter.
    LinkFatura não está na documentação oficial mas a API retorna na prática
    para faturas mais recentes (2026+).
    """
    f = _buscar_fatura(id_fatura)
    if f.get("erro"):
        return None, None, None
    return (
        f.get("LinkBoleto"),
        f.get("LinkNotaFiscal"),
        f.get("LinkFatura"),   # campo extra não documentado, presente em faturas recentes
    )


# ---------------------------------------------------------------------------
# Diagnóstico (mostra amostra dos primeiros N itens)
# ---------------------------------------------------------------------------

def diagnosticar(movimentacoes, n=20):
    log.info("══════════ DIAGNÓSTICO (primeiros %d itens) ══════════", n)
    tem_boleto = tem_nf = 0
    for i, item in enumerate(movimentacoes[:n]):
        lb  = item.get("LinkBoletoBancario") or ""
        lf  = item.get("LinkNotaFiscalServico") or ""
        cli = _nome_cliente(item)[:35]
        idf = item.get("IdFatura") or ""
        log.info("  [%02d] %-35s | boleto:%-5s | nf:%-5s | IdFatura:%s",
                 i + 1, cli, "✔" if lb else "✘", "✔" if lf else "✘", idf or "-")
        if lb: tem_boleto += 1
        if lf: tem_nf += 1
    log.info("  → %d/%d com boleto | %d/%d com NF",
             tem_boleto, min(n, len(movimentacoes)),
             tem_nf,     min(n, len(movimentacoes)))

    # Estatísticas do total
    total = len(movimentacoes)
    com_id_fatura    = sum(1 for x in movimentacoes if x.get("IdFatura"))
    nao_identificado = sum(1 for x in movimentacoes if _nome_cliente(x) in ("cliente_desconhecido", "NÃO IDENTIFICADO", ""))
    com_boleto_total = sum(1 for x in movimentacoes if x.get("LinkBoletoBancario"))
    com_nf_total     = sum(1 for x in movimentacoes if x.get("LinkNotaFiscalServico"))
    log.info("  Total: %d | com IdFatura: %d | sem cliente: %d | com boleto: %d | com NF: %d",
             total, com_id_fatura, nao_identificado, com_boleto_total, com_nf_total)
    log.info("  BUSCAR_FATURA_FALLBACK = %s  |  TIPO_DATA = %s",
             BUSCAR_FATURA_FALLBACK, TIPO_DATA)
    # Avisa se há muitos sem IdFatura (evita 429 desnecessários)
    sem_id_fatura = total - com_id_fatura
    if sem_id_fatura > 100:
        log.info("  ℹ %d movimentações sem IdFatura — BUSCAR_FATURA_FALLBACK não terá efeito nelas.",
                 sem_id_fatura)
    log.info("")


# ---------------------------------------------------------------------------
# Processamento de um período
# ---------------------------------------------------------------------------

def processar(ini, fim, label, buscar_fatura=None):
    # Se não foi passado explicitamente, usa a configuração global
    _buscar_fatura = BUSCAR_LINK_FATURA if buscar_fatura is None else buscar_fatura
    output_dir = OUTPUT_DIR_BASE / label
    output_dir.mkdir(parents=True, exist_ok=True)
    log.info("Pasta de saída: %s", output_dir.resolve())

    movimentacoes = listar_movimentacoes(ini, fim)
    diagnosticar(movimentacoes)

    baixados = _cache_load(CACHE_BAIXADOS)
    stats    = {"boleto_ok": 0, "boleto_err": 0, "nf_ok": 0, "nf_err": 0,
                "fatura_ok": 0, "fatura_err": 0, "sem_doc": 0}
    dirty    = False

    for idx, item in enumerate(movimentacoes, 1):
        nome_cliente = _nome_cliente(item)
        sufixo       = _sufixo_arquivo(item)
        pasta        = output_dir / _safe(nome_cliente)
        guid_parcela = item.get("IdMovimentacaoFinanceiraParcela")

        link_boleto = item.get("LinkBoletoBancario") or None
        link_nf     = item.get("LinkNotaFiscalServico") or None
        link_fatura = None

        # Se já tem links diretos da listagem, ótimo.
        # Se não tem E não tem cliente identificado, pula sem fazer nenhuma chamada.
        if not link_boleto and not link_nf and not _tem_cliente(item):
            stats["sem_doc"] += 1
            continue

        # Fallback via Fatura/Obter — só para receitas com cliente identificado
        eh_receita = not item.get("Debito", True)
        if BUSCAR_FATURA_FALLBACK and _tem_cliente(item) and eh_receita and (not link_boleto or not link_nf or not link_fatura):
            id_fatura = item.get("IdFatura")
            if id_fatura:
                lb, lnf, lf = _links_via_fatura(id_fatura)
                link_boleto = link_boleto or lb
                link_nf     = link_nf or lnf
                link_fatura = link_fatura or lf

        # Busca LinkFatura via API interna (ObterMovimento)
        if _buscar_fatura and guid_parcela:
            link_fatura = _buscar_link_fatura(guid_parcela)

        if not link_boleto and not link_nf and not link_fatura:
            stats["sem_doc"] += 1
            continue

        log.info("[%d/%d] ↓ %-45s [%s]", idx, len(movimentacoes), nome_cliente, sufixo)

        def _baixar(tipo, link, stat_ok, stat_err):
            nonlocal dirty
            dest     = pasta / (_safe(f"{tipo}_{sufixo}") + ".pdf")
            dest_str = str(dest)

            if dest_str in baixados:
                log.info("    já no cache, pulando: %s", dest.name)
                stats[stat_ok] += 1
                return
            if dest.exists():
                baixados[dest_str] = True
                dirty = True
                stats[stat_ok] += 1
                log.info("    já existe no disco: %s", dest.name)
                return

            ok = _download_pdf(link, dest)
            if ok:
                baixados[dest_str] = True
                dirty = True
                stats[stat_ok] += 1
            else:
                stats[stat_err] += 1
            time.sleep(DELAY_DOWNLOAD)

            # Persiste a cada 50 downloads para não perder progresso
            if dirty and (stats["boleto_ok"] + stats["nf_ok"]) % 50 == 0:
                _cache_save(CACHE_BAIXADOS, baixados)
                dirty = False

        if link_boleto: _baixar("boleto", link_boleto, "boleto_ok", "boleto_err")
        if link_nf:     _baixar("nf",     link_nf,     "nf_ok",     "nf_err")
        if link_fatura: _baixar("fatura", link_fatura, "fatura_ok", "fatura_err")

    if dirty:
        _cache_save(CACHE_BAIXADOS, baixados)

    log.info("")
    log.info("══════════════ RESUMO — %s ══════════════", label)
    log.info("Boletos baixados        : %d", stats["boleto_ok"])
    log.info("Boletos com erro        : %d", stats["boleto_err"])
    log.info("Notas fiscais baixadas  : %d", stats["nf_ok"])
    log.info("Notas fiscais com erro  : %d", stats["nf_err"])
    log.info("Faturas baixadas        : %d", stats["fatura_ok"])
    log.info("Faturas com erro        : %d", stats["fatura_err"])
    log.info("Sem documento algum     : %d", stats["sem_doc"])
    log.info("Pasta de saída          : %s", output_dir.resolve())

    return stats, output_dir


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args():
    parser = argparse.ArgumentParser(
        description="Baixa boletos e NFs do BomControle organizados por cliente e período."
    )
    grupo = parser.add_mutually_exclusive_group()
    grupo.add_argument(
        "--ano", metavar="AAAA",
        help="Ano inteiro — processa mês a mês, ex: 2025"
    )
    grupo.add_argument(
        "--mes", metavar="AAAA-MM",
        help="Mês específico, ex: 2025-04"
    )
    grupo.add_argument(
        "--inicio", metavar="AAAA-MM-DD",
        help="Início de intervalo livre (use com --termino)"
    )
    parser.add_argument(
        "--termino", metavar="AAAA-MM-DD",
        help="Fim de intervalo livre (obrigatório com --inicio)"
    )
    return parser.parse_args()


def _validar_jwt():
    """Verifica se há JWT (env ou .api_headers.json) e se não expirou."""
    token = _jwt_atual()
    if not token:
        log.warning("JWT ausente — LinkFatura não será baixado.")
        log.warning("Rode 'python login_bomcontrole.py' para capturar um token novo.")
        return False
    try:
        import base64
        payload = token.split(".")[1]
        payload += "=" * (4 - len(payload) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload))
        exp = claims.get("exp", 0)
        restante = exp - time.time()
        if restante <= 0:
            import datetime
            log.warning("JWT expirado! (Expirou em %s)",
                        datetime.datetime.fromtimestamp(exp))
            return False
        log.info("JWT válido por mais %.0f minutos.", restante / 60)
        return True
    except Exception as e:
        log.warning("JWT inválido: %s", e)
        return False


def _processar_mes(ano, mes, buscar_fatura=True):
    ultimo_dia = calendar.monthrange(ano, mes)[1]
    ini   = f"{ano:04d}-{mes:02d}-01 00:00:00"
    fim   = f"{ano:04d}-{mes:02d}-{ultimo_dia:02d} 23:59:59"
    label = f"{ano:04d}-{mes:02d}"
    log.info("")
    log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    log.info("Processando %s  (%s → %s)", label, ini[:10], fim[:10])
    log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    processar(ini, fim, label, buscar_fatura=buscar_fatura)


if __name__ == "__main__":
    if not API_KEY:
        log.error("BOMCONTROLE_API_KEY não definido. Configure via env (.env ou Railway vars).")
        raise SystemExit(1)

    # Valida JWT antes de qualquer coisa — desativa BUSCAR_LINK_FATURA se inválido
    buscar_fatura_ativo = BUSCAR_LINK_FATURA and _validar_jwt()
    if BUSCAR_LINK_FATURA and not buscar_fatura_ativo:
        log.warning("BUSCAR_LINK_FATURA desativado para esta execução (token ausente ou expirado).")

    # Se há JWT válido, limpa entradas None/erro do cache de movimentos
    # (resíduo de execuções anteriores sem token) para permitir nova tentativa.
    if buscar_fatura_ativo and CACHE_MOVIMENTOS.exists():
        _cache_mov = _cache_load(CACHE_MOVIMENTOS)
        if isinstance(_cache_mov, dict):
            _antes = len(_cache_mov)
            _cache_mov = {k: v for k, v in _cache_mov.items()
                          if isinstance(v, str) and v.startswith("http")}
            _removidos = _antes - len(_cache_mov)
            if _removidos:
                log.info("Cache de movimentos: limpou %d entradas sem link (retry).", _removidos)
                _cache_save(CACHE_MOVIMENTOS, _cache_mov)

    args = _parse_args()
    OUTPUT_DIR_BASE.mkdir(parents=True, exist_ok=True)

    if args.ano:
        try:
            ano = int(args.ano)
        except ValueError:
            log.error("Formato inválido para --ano. Use AAAA, ex: 2025")
            raise SystemExit(1)
        log.info("Modo ano: processando 12 meses de %d  (TIPO_DATA=%s)", ano, TIPO_DATA)
        for mes in range(1, 13):
            _processar_mes(ano, mes, buscar_fatura=buscar_fatura_ativo)
        log.info("")
        log.info("✔ Ano %d concluído. Pastas em: %s", ano, OUTPUT_DIR_BASE.resolve())

    elif args.mes:
        try:
            ano, mes = int(args.mes[:4]), int(args.mes[5:7])
        except (ValueError, IndexError):
            log.error("Formato inválido para --mes. Use AAAA-MM, ex: 2025-04")
            raise SystemExit(1)
        _processar_mes(ano, mes, buscar_fatura=buscar_fatura_ativo)

    elif args.inicio:
        if not args.termino:
            log.error("--inicio requer --termino. Ex: --inicio 2025-04-01 --termino 2025-04-30")
            raise SystemExit(1)
        ini   = f"{args.inicio} 00:00:00"
        fim   = f"{args.termino} 23:59:59"
        label = f"{args.inicio}_{args.termino}"
        log.info("Período : %s → %s  (TIPO_DATA=%s)", ini[:10], fim[:10], TIPO_DATA)
        processar(ini, fim, label, buscar_fatura=buscar_fatura_ativo)

    else:
        ini   = DATA_INICIO
        fim   = DATA_TERMINO
        if ini[8:10] == "01" and fim[8:10] in ("28", "29", "30", "31"):
            label = ini[:7]
        else:
            label = f"{ini[:10]}_{fim[:10]}"
        log.info("Sem argumento — usando datas do script: %s → %s  (TIPO_DATA=%s)",
                 ini[:10], fim[:10], TIPO_DATA)
        processar(ini, fim, label, buscar_fatura=buscar_fatura_ativo)
