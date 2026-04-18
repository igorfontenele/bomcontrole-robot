#!/usr/bin/env python3
"""
login_bomcontrole.py
====================
Faz login headless no BomControle via Selenium e grava .api_headers.json
contendo os headers reais (com Authorization Bearer) + cookies do browser.

O baixar_faturas.py consome esse arquivo para chamar a API interna
(apinew.bomcontrole.com.br) como se fosse o browser.

Credenciais: lidas de env vars (BOMCONTROLE_LOGIN / BOMCONTROLE_SENHA).
Headless: HEADLESS=true (default no Railway).

Exit code 0 em sucesso, 1 em falha.
"""

import base64
import json
import logging
import os
import sys
import time
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

LOGIN     = os.getenv("BOMCONTROLE_LOGIN", "")
SENHA     = os.getenv("BOMCONTROLE_SENHA", "")
HEADLESS  = os.getenv("HEADLESS", "true").lower() in ("1", "true", "yes")

HEADERS_SAIDA = Path(".api_headers.json")

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


def _capturar_auth_request(driver):
    """Varre os performance logs procurando uma request pra apinew/api/ com Authorization."""
    try:
        logs = driver.get_log("performance")
    except Exception as e:
        log.warning("Falha ao ler performance logs: %s", e)
        return None

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
            if not any(k.lower() == "authorization" and "eyJ" in v
                       for k, v in headers.items()):
                continue
            return {
                "sample_url": url,
                "method":     req.get("method", "GET"),
                "headers":    headers,
            }
        except Exception:
            continue
    return None


def _verificar_token(token):
    try:
        payload = token.split(".")[1]
        payload += "=" * (4 - len(payload) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload))
        exp = claims.get("exp", 0)
        restante = exp - time.time()
        if restante <= 0:
            return False
        log.info("Token válido por mais %.0f minutos (%s)",
                 restante / 60, claims.get("usr", "?"))
        return True
    except Exception as e:
        log.warning("Token não pôde ser validado: %s", e)
        return False


def fazer_login():
    if not LOGIN or not SENHA:
        log.error("BOMCONTROLE_LOGIN e BOMCONTROLE_SENHA são obrigatórios (env).")
        return False

    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait
    except ImportError:
        log.error("selenium não instalado. Rode: pip install -r requirements.txt")
        return False

    opts = Options()
    if HEADLESS:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1280,900")
    # Anti-bot detection (BomControle parece bloquear headless padrão)
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    chromedriver_path = os.getenv("CHROMEDRIVER_PATH")
    chrome_bin        = os.getenv("CHROME_BIN")
    if chrome_bin:
        opts.binary_location = chrome_bin

    log.info("Iniciando Chrome (headless=%s)...", HEADLESS)
    try:
        if chromedriver_path:
            driver = webdriver.Chrome(service=Service(chromedriver_path), options=opts)
        else:
            # fallback: webdriver-manager (só pra dev local)
            from webdriver_manager.chrome import ChromeDriverManager
            driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()), options=opts
            )
    except Exception as e:
        log.error("Falha ao iniciar Chrome: %s", e)
        return False

    wait = WebDriverWait(driver, 30)
    debug_dir = Path("/tmp")
    try:
        driver.execute_cdp_cmd("Network.enable", {})
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": _INTERCEPTOR_JS},
        )
        # Sobrescreve navigator.webdriver (principal flag de detecção)
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"},
        )

        log.info("Abrindo página de login...")
        driver.get("https://new.bomcontrole.com.br/login")
        time.sleep(2)

        log.info("Preenchendo email: %s", LOGIN)
        email_input = wait.until(EC.element_to_be_clickable((
            By.XPATH,
            "//input[(@type='email' or @type='text') and not(@type='hidden')]",
        )))
        email_input.clear()
        email_input.send_keys(LOGIN)

        clicou = False
        for txt in ("Avançar", "Avancar", "Próximo", "Proximo", "Continuar"):
            try:
                driver.find_element(
                    By.XPATH, f"//button[contains(normalize-space(.), '{txt}')]"
                ).click()
                clicou = True
                break
            except Exception:
                continue
        if not clicou:
            email_input.send_keys(Keys.RETURN)

        log.info("Preenchendo senha...")
        password_input = wait.until(EC.element_to_be_clickable((
            By.XPATH, "//input[@type='password']",
        )))
        password_input.clear()
        password_input.send_keys(SENHA)

        clicou = False
        for txt in ("Entrar", "Login", "Acessar", "Logar"):
            try:
                driver.find_element(
                    By.XPATH, f"//button[contains(normalize-space(.), '{txt}')]"
                ).click()
                clicou = True
                break
            except Exception:
                continue
        if not clicou:
            password_input.send_keys(Keys.RETURN)

        log.info("Aguardando pós-login...")
        try:
            wait.until(lambda d: "/login" not in d.current_url)
        except Exception:
            log.warning("URL ainda contém /login após 30s — salvando screenshot pra debug.")
            try:
                driver.save_screenshot(str(debug_dir / "bc_login_stuck.png"))
                (debug_dir / "bc_login_stuck.html").write_text(
                    driver.page_source, encoding="utf-8"
                )
                log.warning("Debug salvo em: %s", debug_dir)
            except Exception:
                pass
            return False

        time.sleep(3)

        # Dispara requests autenticadas nas páginas financeiras pra capturar headers reais
        for url in (
            "https://new.bomcontrole.com.br/financeiro/contasareceber",
            "https://new.bomcontrole.com.br/financeiro/movimentacaofinanceira",
        ):
            try:
                driver.get(url)
                time.sleep(5)
            except Exception as e:
                log.warning("Falha navegando %s: %s", url, e)

        req_capturada = _capturar_auth_request(driver)
        if not req_capturada:
            log.error("Nenhuma request autenticada foi capturada.")
            return False

        # Coleta cookies do domínio
        cookies = driver.get_cookies() or []
        cookie_str = "; ".join(
            f"{c['name']}={c['value']}"
            for c in cookies if "bomcontrole" in (c.get("domain") or "")
        )
        req_capturada["cookies"] = cookie_str

        # Extrai e valida o token
        auth = next(
            (v for k, v in req_capturada["headers"].items() if k.lower() == "authorization"),
            "",
        )
        token = auth.split(" ")[-1].strip() if auth else ""
        if not _verificar_token(token):
            log.error("Token capturado é inválido ou expirado.")
            return False

        HEADERS_SAIDA.write_text(
            json.dumps(req_capturada, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        log.info("✔ %s salvo (%d headers, %s cookies)",
                 HEADERS_SAIDA, len(req_capturada["headers"]),
                 "com" if cookie_str else "sem")
        return True

    except Exception as e:
        log.exception("Erro no login: %s", e)
        return False
    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(0 if fazer_login() else 1)
