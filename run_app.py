"""Entry-Point für PyInstaller – startet die NXT Scanner Menubar-App."""

import sys

from storage_scanner.menubar import (
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
