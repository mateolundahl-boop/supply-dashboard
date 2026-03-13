#!/usr/bin/env python3
"""
Supply Dashboard — Local Server with Refresh
=============================================
Serves the dashboard and provides an API to regenerate it from Redshift.

Usage: python server.py [--port 8765] [--no-push]
Then open http://localhost:8765
"""

import http.server
import json
import os
import subprocess
import sys
import threading
import time
import pathlib
import argparse

REPO_DIR = pathlib.Path(__file__).parent
INDEX_PATH = REPO_DIR / "index.html"
GENERATOR = REPO_DIR / "generate_supply_dashboard.py"
PAGES_URL = "https://mateolundahl-boop.github.io/supply-dashboard/"

# Global state for refresh status
refresh_state = {"running": False, "last_update": None, "last_error": None, "log": []}


def run_refresh(push_to_github=True):
    """Run the dashboard generator and optionally push to GitHub Pages."""
    global refresh_state
    refresh_state["running"] = True
    refresh_state["log"] = []
    refresh_state["last_error"] = None

    def log(msg):
        refresh_state["log"].append(msg)
        print(f"  [refresh] {msg}", flush=True)

    try:
        log("Regenerando dashboard desde Redshift...")
        result = subprocess.run(
            [sys.executable, str(GENERATOR)],
            capture_output=True, text=True, cwd=str(REPO_DIR), timeout=600
        )

        if result.returncode != 0:
            log(f"ERROR: {result.stderr[-500:]}")
            refresh_state["last_error"] = result.stderr[-500:]
            return

        log("Dashboard generado OK")

        if push_to_github:
            log("Pusheando a GitHub Pages...")
            subprocess.run(["git", "add", "index.html"], cwd=str(REPO_DIR))
            commit_result = subprocess.run(
                ["git", "commit", "-m", f"Dashboard update {time.strftime('%Y-%m-%d %H:%M')}"],
                capture_output=True, text=True, cwd=str(REPO_DIR)
            )
            if commit_result.returncode == 0:
                push_result = subprocess.run(
                    ["git", "push", "origin", "main"],
                    capture_output=True, text=True, cwd=str(REPO_DIR)
                )
                if push_result.returncode == 0:
                    log(f"Publicado en {PAGES_URL}")
                else:
                    log(f"Push failed (link publico no actualizado): {push_result.stderr[:200]}")
            else:
                log("Sin cambios nuevos para pushear")

        refresh_state["last_update"] = time.strftime("%Y-%m-%d %H:%M:%S")
        log("Listo!")

    except subprocess.TimeoutExpired:
        log("ERROR: Timeout (>10 min)")
        refresh_state["last_error"] = "Timeout"
    except Exception as e:
        log(f"ERROR: {e}")
        refresh_state["last_error"] = str(e)
    finally:
        refresh_state["running"] = False


class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    """HTTP handler that serves the dashboard + refresh API."""

    def __init__(self, *args, push_to_github=True, **kwargs):
        self.push_to_github = push_to_github
        super().__init__(*args, directory=str(REPO_DIR), **kwargs)

    def do_GET(self):
        if self.path == "/api/status":
            self._json_response(refresh_state)
        elif self.path == "/api/refresh":
            if refresh_state["running"]:
                self._json_response({"ok": False, "msg": "Ya hay un refresh en curso"})
            else:
                thread = threading.Thread(target=run_refresh, args=(self.push_to_github,))
                thread.daemon = True
                thread.start()
                self._json_response({"ok": True, "msg": "Refresh iniciado"})
        elif self.path == "/" or self.path == "/index.html":
            # Serve index.html with refresh button injected
            self._serve_dashboard()
        else:
            super().do_GET()

    def _json_response(self, data):
        body = json.dumps(data).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def _serve_dashboard(self):
        if not INDEX_PATH.exists():
            self.send_error(404, "index.html not found. Run generate_supply_dashboard.py first.")
            return

        html = INDEX_PATH.read_text(encoding="utf-8")

        # Inject refresh button + polling script right before </body>
        refresh_widget = """
<!-- Refresh Widget (injected by server.py) -->
<style>
#refresh-widget {
    position: fixed; bottom: 20px; right: 20px; z-index: 99999;
    display: flex; flex-direction: column; align-items: flex-end; gap: 8px;
    font-family: 'Inter', sans-serif;
}
#refresh-btn {
    background: linear-gradient(135deg, #0467FC, #0350C4);
    color: white; border: none; border-radius: 12px;
    padding: 12px 20px; font-size: 14px; font-weight: 600;
    cursor: pointer; box-shadow: 0 4px 16px rgba(4,103,252,0.4);
    display: flex; align-items: center; gap: 8px;
    transition: all 0.2s;
}
#refresh-btn:hover { transform: translateY(-2px); box-shadow: 0 6px 20px rgba(4,103,252,0.5); }
#refresh-btn:disabled { background: #444; cursor: not-allowed; transform: none; box-shadow: none; }
#refresh-btn .spinner {
    display: none; width: 16px; height: 16px;
    border: 2px solid rgba(255,255,255,0.3); border-top-color: white;
    border-radius: 50%; animation: spin 0.8s linear infinite;
}
#refresh-btn.loading .spinner { display: inline-block; }
#refresh-btn.loading .icon { display: none; }
@keyframes spin { to { transform: rotate(360deg); } }
#refresh-log {
    background: rgba(0,0,0,0.85); color: #ccc; border-radius: 8px;
    padding: 10px 14px; font-size: 11px; max-width: 350px;
    display: none; line-height: 1.5; backdrop-filter: blur(8px);
}
#refresh-log.visible { display: block; }
#refresh-meta {
    font-size: 10px; color: rgba(255,255,255,0.4); text-align: right;
}
</style>
<div id="refresh-widget">
    <div id="refresh-log"></div>
    <div id="refresh-meta"></div>
    <button id="refresh-btn" onclick="startRefresh()">
        <span class="icon">&#x1F504;</span>
        <span class="spinner"></span>
        <span class="label">Actualizar</span>
    </button>
</div>
<script>
(function() {
    let polling = false;

    window.startRefresh = function() {
        const btn = document.getElementById('refresh-btn');
        if (btn.disabled) return;
        fetch('/api/refresh').then(r => r.json()).then(d => {
            if (d.ok) {
                btn.classList.add('loading');
                btn.disabled = true;
                btn.querySelector('.label').textContent = 'Actualizando...';
                polling = true;
                pollStatus();
            }
        }).catch(() => {
            document.getElementById('refresh-log').textContent = 'Error: servidor no disponible';
            document.getElementById('refresh-log').classList.add('visible');
        });
    };

    function pollStatus() {
        if (!polling) return;
        fetch('/api/status').then(r => r.json()).then(st => {
            const logEl = document.getElementById('refresh-log');
            if (st.log && st.log.length > 0) {
                logEl.innerHTML = st.log.map(l => l.startsWith('ERROR') ? '<span style="color:#FF4757">' + l + '</span>' : l).join('<br>');
                logEl.classList.add('visible');
            }
            if (!st.running) {
                polling = false;
                const btn = document.getElementById('refresh-btn');
                btn.classList.remove('loading');
                btn.disabled = false;
                if (st.last_error) {
                    btn.querySelector('.label').textContent = 'Reintentar';
                } else {
                    btn.querySelector('.label').textContent = 'Listo!';
                    setTimeout(() => { window.location.reload(); }, 1500);
                }
            } else {
                setTimeout(pollStatus, 2000);
            }
        }).catch(() => { setTimeout(pollStatus, 3000); });
    }

    // Show last update time
    fetch('/api/status').then(r => r.json()).then(st => {
        if (st.last_update) {
            document.getElementById('refresh-meta').textContent = 'Ultima actualizacion: ' + st.last_update;
        }
    }).catch(() => {});
})();
</script>
"""
        html = html.replace("</body>", refresh_widget + "\n</body>")

        body = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        # Quieter logs — only show API calls
        if "/api/" in (args[0] if args else ""):
            super().log_message(format, *args)


def main():
    parser = argparse.ArgumentParser(description="Supply Dashboard Server")
    parser.add_argument("--port", type=int, default=8765, help="Port (default: 8765)")
    parser.add_argument("--no-push", action="store_true", help="Skip GitHub push on refresh")
    args = parser.parse_args()

    handler_class = lambda *a, **kw: DashboardHandler(*a, push_to_github=not args.no_push, **kw)

    server = http.server.HTTPServer(("", args.port), handler_class)
    url = f"http://localhost:{args.port}"

    print(f"""
═══════════════════════════════════════════════════════════
  Supply Dashboard Server
  🔗 {url}
  {"📤 Push to GitHub Pages: ON" if not args.no_push else "📤 Push to GitHub Pages: OFF (--no-push)"}
  Ctrl+C para parar
═══════════════════════════════════════════════════════════
""")

    # Open in browser
    import webbrowser
    webbrowser.open(url)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")
        server.server_close()


if __name__ == "__main__":
    main()
