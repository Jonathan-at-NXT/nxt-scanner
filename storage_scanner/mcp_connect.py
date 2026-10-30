"""„Claude verbinden“: trägt das NXT Data MCP in Claude Code und/oder Claude Desktop ein.

Idempotent: zweimal ausführen ändert nichts. Claude Code über die CLI (`claude mcp remove` + `add`,
Bereich user), Claude Desktop über claude_desktop_config.json (nur mcpServers.nxt-data, alles andere bleibt;
vorher Sicherungskopie). Gestartet wird das MCP aus dem App-Binary mit --mcp (kein Python nötig).
"""
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

NAME = "nxt-data"
DESKTOP_CONFIG = Path.home() / "Library/Application Support/Claude/claude_desktop_config.json"
# launchd startet die App mit minimalem PATH – darum die üblichen Installationsorte direkt prüfen
CLAUDE_CANDIDATES = [Path.home() / ".local/bin/claude", Path.home() / ".claude/local/claude",
                     Path("/opt/homebrew/bin/claude"), Path("/usr/local/bin/claude")]


def server_command() -> list[str]:
    """Befehl, der das MCP startet: im gebauten .app das Binary selbst, in Dev das Modul."""
    if getattr(sys, "frozen", False):
        # Immer die Kopie in /Programme eintragen: die ersetzt der Auto-Updater sicher, so kommt das MCP mit jedem Update mit
        from .updater import APP_INSTALL_PATH
        installed = APP_INSTALL_PATH / "Contents" / "MacOS" / "NXT Scanner"
        return [str(installed) if installed.exists() else sys.executable, "--mcp"]
    return [sys.executable, "-m", "storage_scanner.mcp_server"]


def find_claude_cli() -> str | None:
    for p in CLAUDE_CANDIDATES:
        if p.is_file() and os.access(p, os.X_OK):
            return str(p)
    return shutil.which("claude")


def connect_claude_code(cli: str, cmd: list[str], run=subprocess.run) -> str:
    run([cli, "mcp", "remove", NAME, "-s", "user"], capture_output=True, text=True, timeout=60)
    r = run([cli, "mcp", "add", NAME, "-s", "user", "--env", "NXT_DATA_MODE=remote", "--", *cmd],
            capture_output=True, text=True, timeout=60)
    if r.returncode != 0:
        raise RuntimeError((r.stderr or r.stdout or "unbekannter Fehler").strip()[:300])
    return "Claude Code"


def connect_claude_desktop(cmd: list[str], path: Path = DESKTOP_CONFIG) -> str:
    cfg = {}
    if path.exists():
        raw = path.read_text(encoding="utf-8")
        cfg = json.loads(raw) if raw.strip() else {}
        entry = {"command": cmd[0], "args": cmd[1:], "env": {"NXT_DATA_MODE": "remote"}}
        if (cfg.get("mcpServers") or {}).get(NAME) == entry:
            return "Claude Desktop"                          # schon eingetragen
        path.with_name(path.name + ".vor-nxt-data").write_text(raw, encoding="utf-8")
    cfg.setdefault("mcpServers", {})[NAME] = {"command": cmd[0], "args": cmd[1:], "env": {"NXT_DATA_MODE": "remote"}}
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)
    return "Claude Desktop"


def connect() -> dict:
    """Trägt das MCP überall ein, wo Claude installiert ist. Ergebnis: {ok: [...], fehler: [...], fehlt: bool}."""
    cmd = server_command()
    ok, fehler = [], []
    cli = find_claude_cli()
    if cli:
        try:
            ok.append(connect_claude_code(cli, cmd))
        except Exception as e:
            fehler.append(f"Claude Code: {e}")
    if DESKTOP_CONFIG.parent.is_dir() or Path("/Applications/Claude.app").exists():
        try:
            ok.append(connect_claude_desktop(cmd))
        except Exception as e:
            fehler.append(f"Claude Desktop: {e}")
    return {"ok": ok, "fehler": fehler, "fehlt": not cli and not ok and not fehler}
