"""Entry-Point für PyInstaller – startet die NXT Scanner Menubar-App.

Mit --mcp startet stattdessen das NXT Data MCP (stdio) für Claude, ohne Menüleiste und Singleton-Lock.
"""

import sys

if __name__ == "__main__" and "--mcp" in sys.argv:
    from storage_scanner.mcp_server import main as mcp_main
    mcp_main()
    sys.exit(0)

from storage_scanner.menubar import (  # noqa: E402
    NSApplication, NSApplicationActivationPolicyAccessory,
    migrate_legacy_data, ensure_dirs, ask_for_setup, register_launchd_agent,
    acquire_singleton_lock, StorageScannerApp,
)

if __name__ == "__main__":
    NSApplication.sharedApplication().setActivationPolicy_(NSApplicationActivationPolicyAccessory)
    if not acquire_singleton_lock():
        sys.exit(0)
    migrate_legacy_data()
    ensure_dirs()
    ask_for_setup()
    register_launchd_agent()
    StorageScannerApp().run()
