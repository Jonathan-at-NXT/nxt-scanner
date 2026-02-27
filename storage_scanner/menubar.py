"""NXT Studios – Menubar-App für Storage Scanner.

Zeigt Status, letzte Aktionen und ermöglicht manuellen Scan.
Erkennt neue Datenträger automatisch und rescannt stündlich.
Fragt beim ersten Start nach dem Benutzernamen.
"""

import json
import queue
import re
import threading
import webbrowser
from datetime import datetime
from pathlib import Path

import rumps
from AppKit import NSApplication, NSApplicationActivationPolicyAccessory

from . import __version__
from .paths import (
    CONFIG_PATH, LOG_PATH, LAST_SCAN_PATH, REPORTS_DIR,
    ensure_dirs, migrate_legacy_data,
)
from .updater import check_for_update

RESCAN_INTERVAL_SECONDS = 3600  # 1 Stunde

IGNORED_VOLUMES = {"Macintosh HD", "Macintosh HD - Data", "Recovery", "Preboot", "VM", "Update"}

AUTO_SCAN_PATTERNS = [
    re.compile(r"^nxt\s+\d+$", re.IGNORECASE),       # NXT 005, nxt 007, ...
    re.compile(r"^tower\s+\d+$", re.IGNORECASE),      # TOWER 1, Tower 2, ...
    re.compile(r"^nxt\s+hub\s+\d+$", re.IGNORECASE),  # NXT HUB 1, nxt hub 2, ...
]


def is_auto_scan_volume(name: str) -> bool:
    return any(p.match(name) for p in AUTO_SCAN_PATTERNS)


def load_config() -> dict:
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def save_config(config: dict) -> None:
    with open(CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=2)


class StorageScannerApp(rumps.App):
    def __init__(self):
        super().__init__("NXT", quit_button=None)

        self._queue = queue.Queue()
        self._current_scan = None
        self._known_volumes: set[str] = set()
        self._scan_times = self._load_scan_times()
        self._user_name = load_config().get("user_name", "")
        self._worker = threading.Thread(target=self._process_queue, daemon=True)
        self._worker.start()

        self.name_item = rumps.MenuItem(
            f"Angemeldet als: {self._user_name}" if self._user_name else "Kein Name gesetzt",
            callback=self.change_name,
        )
        self.status_item = rumps.MenuItem("Status")
        self.queue_item = rumps.MenuItem("Warteschlange")
        self.last_scan_item = rumps.MenuItem("Letzter Sync: –")
        self.volumes_menu = rumps.MenuItem("Volumes")
        self.log_menu = rumps.MenuItem("Log")
        self.scan_all_item = rumps.MenuItem("Jetzt alle scannen", callback=self.scan_all)
        self.version_item = rumps.MenuItem(f"Version {__version__}")
        self.update_item = rumps.MenuItem("Auf Updates prüfen...", callback=self._manual_update_check)
        self._update_info = None
        self.quit_item = rumps.MenuItem("Beenden", callback=self.quit_app)

        self.menu = [
            self.name_item,
            None,
            self.status_item,
            self.queue_item,
            self.volumes_menu,
            None,
            self.last_scan_item,
            None,
            self.log_menu,
            None,
            self.scan_all_item,
            None,
            self.version_item,
            self.update_item,
            None,
            self.quit_item,
        ]

        # Initiale Volumes als bekannt setzen (kein sofortiger Scan beim App-Start)
        self._known_volumes = set(self.get_mounted_volumes())

    # ── Name ─────────────────────────────────────────────────────────

    def change_name(self, _):
        response = rumps.Window(
            message="Name eingeben:",
            title="NXT Storage Scanner",
            default_text=self._user_name,
            ok="Speichern",
            cancel="Abbrechen",
            dimensions=(300, 24),
        ).run()
        if response.clicked and response.text.strip():
            self._user_name = response.text.strip()
            config = load_config()
            config["user_name"] = self._user_name
            save_config(config)
            self.name_item.title = f"Angemeldet als: {self._user_name}"

    # ── Daten ───────────────────────────────────────────────────────

    def get_mounted_volumes(self) -> list[str]:
        volumes_dir = Path("/Volumes")
        if not volumes_dir.exists():
            return []
        return sorted(
            entry.name
            for entry in volumes_dir.iterdir()
            if entry.is_dir() and entry.name not in IGNORED_VOLUMES
        )

    def get_log_lines(self, count: int = 8) -> list[str]:
        if not LOG_PATH.exists():
            return ["Noch keine Aktivität"]
        lines = LOG_PATH.read_text().strip().splitlines()
        return lines[-count:] if lines else ["Noch keine Aktivität"]

    def get_last_scan_info(self) -> str:
        if not LOG_PATH.exists():
            return "–"
        lines = LOG_PATH.read_text().strip().splitlines()
        for line in reversed(lines):
            if "abgeschlossen" in line:
                return line[:19]
        return "–"

    def _queued_names(self) -> list[str]:
        with self._queue.mutex:
            return list(self._queue.queue)

    def _load_scan_times(self) -> dict[str, str]:
        if LAST_SCAN_PATH.exists():
            try:
                with open(LAST_SCAN_PATH) as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError):
                pass
        return {}

    def _save_scan_times(self) -> None:
        with open(LAST_SCAN_PATH, "w") as f:
            json.dump(self._scan_times, f, indent=2)

    def _seconds_since_last_scan(self, volume_name: str) -> float:
        last = self._scan_times.get(volume_name)
        if not last:
            return float("inf")
        try:
            return (datetime.now() - datetime.fromisoformat(last)).total_seconds()
        except (ValueError, TypeError):
            return float("inf")

    # ── Auto-Scan: Neue Volumes erkennen + stündlicher Rescan ─────

    @rumps.timer(10)
    def auto_scan_check(self, _):
        """Alle 10 Sekunden: neue Volumes erkennen + prüfen ob Rescan fällig."""
        current = set(self.get_mounted_volumes())
        new_volumes = current - self._known_volumes
        self._known_volumes = current

        # Neue Auto-Scan-Volumes sofort in die Queue
        for name in sorted(new_volumes):
            if is_auto_scan_volume(name):
                self._log(f"Neuer Datenträger erkannt: {name}")
                self.enqueue(name, silent=True)

        # Rescan fälliger Volumes
        for name in sorted(current):
            if not is_auto_scan_volume(name):
                continue
            if self._seconds_since_last_scan(name) >= RESCAN_INTERVAL_SECONDS:
                self.enqueue(name, silent=True)

    # ── Menü aktualisieren ──────────────────────────────────────────

    @rumps.timer(5)
    def refresh(self, _):
        self._update_menu()

    def _update_menu(self):
        volumes = self.get_mounted_volumes()
        current = self._current_scan
        pending = self._queued_names()

        # Status
        if current:
            remaining = len(pending)
            if remaining > 0:
                self.status_item.title = f"  Scanne {current}  (+{remaining} in Warteschlange)"
            else:
                self.status_item.title = f"  Scanne {current}..."
            self.title = "NXT ⟳"
        else:
            self.status_item.title = f"  {len(volumes)} Datenträger verbunden"
            self.title = "NXT"

        # Warteschlange
        for key in list(self.queue_item):
            del self.queue_item[key]
        if current or pending:
            if current:
                self.queue_item.add(rumps.MenuItem(f"  ⟳ {current} (läuft)"))
            for name in pending:
                self.queue_item.add(rumps.MenuItem(f"  ⏳ {name}"))
            self.queue_item.title = f"Warteschlange ({(1 if current else 0) + len(pending)})"
        else:
            self.queue_item.title = "Warteschlange (leer)"
            self.queue_item.add(rumps.MenuItem("  Keine Scans ausstehend"))

        # Volumes
        for key in list(self.volumes_menu):
            del self.volumes_menu[key]
        if volumes:
            for vol in volumes:
                self.volumes_menu.add(rumps.MenuItem(
                    f"Scannen: {vol}",
                    callback=lambda sender, v=vol: self.enqueue(v),
                ))
        else:
            self.volumes_menu.add(rumps.MenuItem("Keine externen Volumes"))

        # Letzter Scan
        self.last_scan_item.title = f"Letzter Sync: {self.get_last_scan_info()}"

        # Log
        for key in list(self.log_menu):
            del self.log_menu[key]
        for line in self.get_log_lines():
            self.log_menu.add(rumps.MenuItem(line))

    # ── Queue-Logik ─────────────────────────────────────────────────

    def enqueue(self, volume_name: str, silent: bool = False):
        """Fügt ein Volume zur Warteschlange hinzu (Duplikate vermieden)."""
        pending = self._queued_names()
        if volume_name == self._current_scan or volume_name in pending:
            if not silent:
                rumps.notification("NXT Storage Scanner", "", f"{volume_name} ist bereits in der Warteschlange.")
            return
        self._queue.put(volume_name)
        if not silent:
            rumps.notification("NXT Storage Scanner", "Zur Warteschlange hinzugefügt", volume_name)

    def _process_queue(self):
        while True:
            volume_name = self._queue.get()
            self._current_scan = volume_name

            self._do_scan(volume_name)

            self._current_scan = None
            self._queue.task_done()

    def _do_scan(self, volume_name: str):
        from .scan import run_scan
        from .notion_sync import run_sync

        volume_path = f"/Volumes/{volume_name}"
        report_name = volume_name.replace(" ", "_") + "_report.json"
        report_path = REPORTS_DIR / report_name

        try:
            run_scan(volume_path, str(report_path))
            run_sync(str(report_path))

            self._scan_times[volume_name] = datetime.now().isoformat()
            self._save_scan_times()

            self._log(f"Scan + Sync abgeschlossen: {volume_name}")
            rumps.notification("NXT Storage Scanner", "Fertig", f"{volume_name} → Notion aktualisiert")
        except Exception as e:
            self._notify_error(volume_name, str(e))

    def _log(self, message: str):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(LOG_PATH, "a") as f:
            f.write(f"{timestamp}  {message}\n")

    def _notify_error(self, volume_name: str, error: str):
        self._log(f"FEHLER bei {volume_name}: {error.strip()[:200]}")
        rumps.notification("NXT Storage Scanner", "Fehler", f"{volume_name}: {error[:100]}")

    # ── Aktionen ────────────────────────────────────────────────────

    def scan_all(self, _):
        volumes = self.get_mounted_volumes()
        if not volumes:
            rumps.notification("NXT Storage Scanner", "", "Kein externer Datenträger gefunden.")
            return
        for vol in volumes:
            self.enqueue(vol)

    # ── Update-Check ──────────────────────────────────────────────

    @rumps.timer(6 * 3600)
    def periodic_update_check(self, _):
        threading.Thread(target=self._do_update_check, daemon=True).start()

    def _manual_update_check(self, _):
        threading.Thread(target=self._do_update_check, args=(True,), daemon=True).start()

    def _do_update_check(self, notify_if_current: bool = False):
        info = check_for_update()
        if info:
            self._update_info = info
            self.update_item.title = f"Update verfügbar: v{info['version']}"
            self.update_item.callback = self._open_download
            rumps.notification(
                "NXT Scanner Update",
                f"Version {info['version']} verfügbar",
                info.get("release_notes", "Neue Version verfügbar."),
            )
        elif notify_if_current:
            rumps.notification("NXT Scanner", "", f"Du hast die neueste Version ({__version__}).")

    def _open_download(self, _):
        if self._update_info:
            webbrowser.open(self._update_info["download_url"])

    def quit_app(self, _):
        rumps.quit_application()


def ask_for_setup() -> None:
    """Fragt beim ersten Start nach Name, Notion-Token und Parent Page ID."""
    config = load_config()
    changed = False

    # Name
    if not config.get("user_name"):
        response = rumps.Window(
            message="Bitte gib deinen Namen ein.\nSo tracken wir, wer zuletzt welche SSD genutzt hat.",
            title="NXT Storage Scanner – Setup (1/3)",
            default_text="",
            ok="Weiter",
            cancel=False,
            dimensions=(300, 24),
        ).run()
        name = response.text.strip()
        if name:
            config["user_name"] = name
            changed = True

    # Notion Token
    if not config.get("notion_token"):
        response = rumps.Window(
            message="Notion Integration Token eingeben.\n"
                    "(Notion Settings → Connections → Develop or manage integrations)",
            title="NXT Storage Scanner – Setup (2/3)",
            default_text="ntn_...",
            ok="Weiter",
            cancel=False,
            dimensions=(400, 24),
        ).run()
        token = response.text.strip()
        if token and token != "ntn_...":
            config["notion_token"] = token
            changed = True

    # Parent Page ID
    if not config.get("notion_parent_page_id"):
        response = rumps.Window(
            message="Notion Parent Page ID eingeben.\n"
                    "(Die Seite unter der die Datenbanken erstellt werden.\n"
                    "URL: notion.so/XXXXXXXX → die ID ist der letzte Teil.)",
            title="NXT Storage Scanner – Setup (3/3)",
            default_text="",
            ok="Fertig",
            cancel=False,
            dimensions=(400, 24),
        ).run()
        page_id = response.text.strip()
        if page_id:
            config["notion_parent_page_id"] = page_id
            changed = True

    if changed:
        save_config(config)


def register_login_item() -> None:
    """Registriert die App als Login-Item (startet automatisch bei Anmeldung)."""
    try:
        import subprocess
        app_path = "/Applications/NXT Scanner.app"
        # Prüfen ob bereits als Login-Item registriert
        result = subprocess.run(
            ["osascript", "-e",
             'tell application "System Events" to get the name of every login item'],
            capture_output=True, text=True,
        )
        if "NXT Scanner" in result.stdout:
            return

        subprocess.run(
            ["osascript", "-e",
             f'tell application "System Events" to make login item at end '
             f'with properties {{path:"{app_path}", hidden:false}}'],
            capture_output=True, text=True,
        )
    except Exception:
        pass


if __name__ == "__main__":
    NSApplication.sharedApplication().setActivationPolicy_(NSApplicationActivationPolicyAccessory)
    migrate_legacy_data()
    ensure_dirs()
    ask_for_setup()
    register_login_item()
    StorageScannerApp().run()
