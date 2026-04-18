#!/usr/bin/env python3
"""
renovar_token.py
================
Faz login automático no BomControle via Selenium e captura o JWT token,
atualizando automaticamente o baixar_faturas.py.

Uso:
    pip install selenium webdriver-manager
    python renovar_token.py
"""

import os
import re
import sys
import json
import time
import base64
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONFIGURAÇÃO
# ---------------------------------------------------------------------------
SUBDOMINIO = os.getenv("BOMCONTROLE_SUBDOMINIO", "finacon")
LOGIN      = os.getenv("BOMCONTROLE_LOGIN",      "")
SENHA      = os.getenv("BOMCONTROLE_SENHA",      "")

SCRIPT_ALVO    = Path("baixar_faturas.py")
HEADERS_SAIDA  = Path(".api_headers.json")   # receita com headers reais do browser
# ---------------------------------------------------------------------------


def screenshot(driver, nome):
    path = f"/tmp/bc_{nome}.png"
    driver.save_screenshot(path)
    log.info("Screenshot salvo: %s", path)


_INTERCEPTOR_JS = r"""
(function () {
    if (window.__tokenInterceptorInstalled) return;
    window.__tokenInterceptorInstalled = true;
    window.__capturedTokens__ = [];

    function push(val) {
        if (!val) return;
        var s = String(val);
        if (s.indexOf('eyJ') < 0) return;
        var m = s.match(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/);
        if (m) window.__capturedTokens__.push(m[0]);
    }

    var origFetch = window.fetch;
    if (origFetch) {
        window.fetch = function (input, init) {
            try {
                var h = (init && init.headers) || (input && input.headers);
                if (h) {
                    if (typeof h.get === 'function') {
                        push(h.get('authorization') || h.get('Authorization'));
                    } else {
                        push(h.authorization || h.Authorization);
                    }
                }
            } catch (e) {}
            return origFetch.apply(this, arguments);
        };
    }

    var origSet = XMLHttpRequest.prototype.setRequestHeader;
    XMLHttpRequest.prototype.setRequestHeader = function (name, value) {
        try {
            if (/^authorization$/i.test(name)) push(value);
        } catch (e) {}
        return origSet.apply(this, arguments);
    };
})();
"""


def _extrair_token_capturado(driver):
    try:
        captured = driver.execute_script("return window.__capturedTokens__ || [];")
    except Exception:
        captured = []
    for raw in captured or []:
        if raw and "eyJ" in raw:
            return raw.split(" ")[-1].strip()
    return None


def _extrair_token_perf_logs(driver):
    try:
        logs = driver.get_log("performance")
    except Exception:
        return None
    for entry in logs:
        try:
            msg = json.loads(entry["message"])["message"]
            if msg.get("method") not in ("Network.requestWillBeSent", "Network.requestWillBeSentExtraInfo"):
                continue
            params = msg.get("params", {})
            headers = (params.get("request", {}) or {}).get("headers", {}) or params.get("headers", {}) or {}
            auth = headers.get("authorization") or headers.get("Authorization") or ""
            if "eyJ" in auth:
                return auth.split(" ")[-1].strip()
        except Exception:
            pass
    return None


def _capturar_headers_api(driver):
    """
    Procura nos logs de performance a primeira request para apinew.bomcontrole.com.br/api/
    com Authorization Bearer, e salva TODOS os headers + cookies em HEADERS_SAIDA.
    Isso permite ao baixar_faturas.py replicar exatamente a chamada do browser.
    """
    try:
        logs = driver.get_log("performance")
    except Exception as e:
        log.warning("Não consegui ler performance logs: %s", e)
        return False

    encontrado = None
    for entry in logs:
        try:
            msg = json.loads(entry["message"])["message"]
            if msg.get("method") != "Network.requestWillBeSent":
                continue
            req = (msg.get("params", {}) or {}).get("request", {}) or {}
            url = req.get("url", "")
            if "apinew.bomcontrole.com.br/api/" not in url:
                continue
            headers = req.get("headers", {}) or {}
            # Precisa ter Authorization — senão é request pública
            if not any(k.lower() == "authorization" for k in headers.keys()):
                continue
            encontrado = {
                "sample_url": url,
                "method":     req.get("method", "GET"),
                "headers":    headers,
            }
            break
        except Exception:
            pass

    if not encontrado:
        log.warning("Nenhuma request autenticada para apinew/api/ foi capturada.")
        return False

    # Coleta cookies do domínio bomcontrole.com.br para enviar junto
    try:
        cookies = driver.get_cookies() or []
        cookie_str = "; ".join(
            f"{c['name']}={c['value']}"
            for c in cookies
            if "bomcontrole" in (c.get("domain") or "")
        )
    except Exception:
        cookie_str = ""

    encontrado["cookies"] = cookie_str

    try:
        HEADERS_SAIDA.write_text(
            json.dumps(encontrado, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        log.info("✔ Headers reais salvos em %s (%d headers, %s cookies)",
                 HEADERS_SAIDA, len(encontrado["headers"]),
                 "com" if cookie_str else "sem")
        return True
    except Exception as e:
        log.warning("Falha ao salvar %s: %s", HEADERS_SAIDA, e)
        return False


def _extrair_token_storage(driver):
    script = r"""
        var t = null;
        ['sessionStorage','localStorage'].forEach(function(s){
            try {
                Object.keys(window[s]).forEach(function(k){
                    var v = window[s].getItem(k);
                    if (v && v.indexOf('eyJ') >= 0) {
                        var m = v.match(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/);
                        if (m) t = m[0];
                    }
                });
            } catch(e) {}
        });
        return t;
    """
    try:
        result = driver.execute_script(script)
        if result and "eyJ" in result:
            return result.strip()
    except Exception:
        pass
    return None


def obter_token_via_selenium():
    try:
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.keys import Keys
    except ImportError:
        log.error("Rode: pip install selenium webdriver-manager")
        return None

    try:
        from webdriver_manager.chrome import ChromeDriverManager
        driver_path = ChromeDriverManager().install()
    except Exception as e:
        log.warning("webdriver-manager falhou: %s", e)
        driver_path = None

    opts = Options()
    # Descomenta para rodar headless:
    # opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1280,900")
    opts.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    log.info("Iniciando Chrome...")
    try:
        if driver_path:
            driver = webdriver.Chrome(service=Service(driver_path), options=opts)
        else:
            driver = webdriver.Chrome(options=opts)
    except Exception as e:
        log.error("Erro ao iniciar Chrome: %s", e)
        return None

    token = None
    wait = WebDriverWait(driver, 25)

    try:
        # Instala o interceptor ANTES de qualquer navegação — roda em cada documento novo
        try:
            driver.execute_cdp_cmd("Network.enable", {})
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": _INTERCEPTOR_JS})
            log.info("Interceptor instalado via CDP.")
        except Exception as e:
            log.warning("Falha ao instalar interceptor via CDP: %s", e)

        # ── Passo 1: abre a página de login ──────────────────────────────
        log.info("Abrindo https://new.bomcontrole.com.br/login ...")
        driver.get("https://new.bomcontrole.com.br/login")
        time.sleep(2)
        screenshot(driver, "01_login_aberto")
        log.info("URL: %s | Título: %s", driver.current_url, driver.title)

        # Fallback: reinjeta se não pegou via CDP
        try:
            driver.execute_script(_INTERCEPTOR_JS)
        except Exception:
            pass

        # ── Passo 2: preenche EMAIL e avança ─────────────────────────────
        log.info("Etapa 1: email")
        email_input = wait.until(EC.element_to_be_clickable((
            By.XPATH,
            "//input[(@type='email' or @type='text') and not(@type='hidden')]"
        )))
        email_input.clear()
        email_input.send_keys(LOGIN)
        log.info("Email preenchido: %s", LOGIN)
        time.sleep(0.5)
        screenshot(driver, "02_email_preenchido")

        avancou = False
        for btn_text in ["Avançar", "Avancar", "Próximo", "Proximo", "Continuar", "Next"]:
            try:
                btn = driver.find_element(By.XPATH, f"//button[contains(normalize-space(.), '{btn_text}')]")
                btn.click()
                log.info("Clicou em '%s'", btn_text)
                avancou = True
                break
            except Exception:
                pass
        if not avancou:
            email_input.send_keys(Keys.RETURN)
            log.info("Enter enviado no campo de email")

        # ── Passo 3: espera campo SENHA aparecer ─────────────────────────
        log.info("Etapa 2: senha — aguardando campo aparecer...")
        password_input = wait.until(EC.element_to_be_clickable((
            By.XPATH, "//input[@type='password']"
        )))
        time.sleep(0.5)
        password_input.clear()
        password_input.send_keys(SENHA)
        log.info("Senha preenchida")
        screenshot(driver, "03_senha_preenchida")

        entrou = False
        for btn_text in ["Entrar", "Login", "Acessar", "Logar", "Sign in"]:
            try:
                btn = driver.find_element(By.XPATH, f"//button[contains(normalize-space(.), '{btn_text}')]")
                btn.click()
                log.info("Clicou em '%s'", btn_text)
                entrou = True
                break
            except Exception:
                pass
        if not entrou:
            password_input.send_keys(Keys.RETURN)
            log.info("Enter enviado no campo de senha")

        # ── Passo 4: aguarda navegação pós-login ─────────────────────────
        log.info("Aguardando navegação pós-login...")
        try:
            wait.until(lambda d: "/login" not in d.current_url)
        except Exception:
            log.warning("URL ainda contém /login — pode haver erro de credencial/2FA.")
        time.sleep(3)
        screenshot(driver, "04_pos_login")
        log.info("URL pós-login: %s", driver.current_url)

        # ── Passo 5: força chamadas autenticadas em páginas financeiras ──
        # Precisamos GARANTIR que o browser fez pelo menos uma request a
        # apinew.bomcontrole.com.br/api/ para capturar os headers reais.
        log.info("Navegando para disparar requisições autenticadas à API interna...")
        for url in (
            "https://new.bomcontrole.com.br/financeiro/contasareceber",
            "https://new.bomcontrole.com.br/financeiro/movimentacaofinanceira",
        ):
            try:
                driver.get(url)
                time.sleep(5)
            except Exception as e:
                log.warning("Navegação falhou em %s: %s", url, e)

        screenshot(driver, "05_pos_nav")

        # Captura TOKEN + HEADERS a partir dos logs
        token = _extrair_token_capturado(driver) or _extrair_token_perf_logs(driver)
        _capturar_headers_api(driver)

        if token:
            log.info("✔ Token capturado após navegação.")
            return token

        screenshot(driver, "05_pos_nav")

        # ── Passo 6: fallback storage ───────────────────────────────────
        log.info("Tentando via sessionStorage/localStorage...")
        token = _extrair_token_storage(driver)
        if token:
            log.info("✔ Token encontrado via storage!")
            return token

    except Exception as e:
        log.error("Erro durante automação: %s", e)
        try:
            screenshot(driver, "erro")
        except Exception:
            pass
    finally:
        try:
            driver.quit()
        except Exception:
            pass

    return token


def verificar_token(token):
    try:
        payload = token.split(".")[1]
        payload += "=" * (4 - len(payload) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload))
        exp = claims.get("exp", 0)
        restante = exp - time.time()
        if restante <= 0:
            log.warning("Token expirado.")
            return False
        log.info("Token válido por mais %.0f minutos.", restante / 60)
        log.info("Usuário: %s (%s)", claims.get("usrn", "?"), claims.get("usr", "?"))
        return True
    except Exception:
        return False


def atualizar_script(token):
    if not SCRIPT_ALVO.exists():
        log.error("%s não encontrado.", SCRIPT_ALVO)
        return False
    conteudo = SCRIPT_ALVO.read_text(encoding="utf-8")
    novo = re.sub(r'JWT_TOKEN\s*=\s*"[^"]*"', f'JWT_TOKEN        = "{token}"', conteudo)
    if novo == conteudo:
        log.error("Linha JWT_TOKEN não encontrada em %s", SCRIPT_ALVO)
        return False
    SCRIPT_ALVO.write_text(novo, encoding="utf-8")
    log.info("✔ JWT_TOKEN atualizado em %s", SCRIPT_ALVO)
    return True


if __name__ == "__main__":
    log.info("=== Renovador de Token BomControle ===")

    token = obter_token_via_selenium()

    if not token:
        log.error("Não foi possível capturar o token.")
        log.error("Verifique os screenshots em /tmp/bc_*.png para diagnosticar.")
        sys.exit(1)

    verificar_token(token)

    if atualizar_script(token):
        log.info("✔ Pronto! Agora rode: python %s --ano 2026", SCRIPT_ALVO)
    else:
        sys.exit(1)