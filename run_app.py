"""Entry-Point für PyInstaller – startet die NXT Scanner Menubar-App."""

from storage_scanner.menubar import (
    NSApplication, NSApplicationActivationPolicyAccessory,
    migrate_legacy_data, ensure_dirs, ask_for_setup, StorageScannerApp,
)

if __name__ == "__main__":
    NSApplication.sharedApplication().setActivationPolicy_(NSApplicationActivationPolicyAccessory)
    migrate_legacy_data()
    ensure_dirs()
    ask_for_setup()
    StorageScannerApp().run()
