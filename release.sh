#!/bin/bash
set -euo pipefail

# ── NXT Scanner – Release-Skript ──────────────────────────────────
# Baut die .app und erstellt ein .dmg für die Verteilung.

BOLD="\033[1m"
GREEN="\033[32m"
RED="\033[31m"
RESET="\033[0m"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Version aus Python-Package lesen
VERSION=$(python3 -c "import sys; sys.path.insert(0,'.'); from storage_scanner import __version__; print(__version__)")
echo -e "${BOLD}NXT Scanner v${VERSION} – Build starten${RESET}"
echo ""

# ── 1. Aufräumen ──────────────────────────────────────────────────
echo -e "${BOLD}[1/5] Aufräumen...${RESET}"
rm -rf build/ dist/

# ── 2. Abhängigkeiten prüfen ─────────────────────────────────────
echo -e "${BOLD}[2/5] Abhängigkeiten prüfen...${RESET}"

if ! command -v pyinstaller &>/dev/null; then
    echo "  PyInstaller nicht gefunden. Installiere..."
    python3 -m pip install --quiet pyinstaller
fi

if ! command -v create-dmg &>/dev/null; then
    echo -e "${RED}create-dmg nicht gefunden.${RESET}"
    echo "  Installieren mit: brew install create-dmg"
    exit 1
fi

echo "  Alles vorhanden"

# ── 3. App bauen ─────────────────────────────────────────────────
echo -e "${BOLD}[3/5] App bauen (PyInstaller)...${RESET}"
python3 -m PyInstaller nxt_scanner.spec --noconfirm --clean 2>&1 | tail -5

if [ ! -d "dist/NXT Scanner.app" ]; then
    echo -e "${RED}Build fehlgeschlagen – keine .app erstellt.${RESET}"
    exit 1
fi
echo "  dist/NXT Scanner.app erstellt"

# ── 4. Code-Signierung ──────────────────────────────────────────
echo -e "${BOLD}[4/5] Code-Signierung (ad-hoc)...${RESET}"
codesign --force --deep --sign - "dist/NXT Scanner.app"
echo "  Signiert"

# ── 5. DMG erstellen ────────────────────────────────────────────
echo -e "${BOLD}[5/5] DMG erstellen...${RESET}"
DMG_NAME="NXT-Scanner-${VERSION}.dmg"
rm -f "dist/${DMG_NAME}"

create-dmg \
    --volname "NXT Scanner" \
    --window-pos 200 120 \
    --window-size 600 400 \
    --icon-size 100 \
    --icon "NXT Scanner.app" 150 185 \
    --hide-extension "NXT Scanner.app" \
    --app-drop-link 450 185 \
    --no-internet-enable \
    "dist/${DMG_NAME}" \
    "dist/NXT Scanner.app"

echo ""
echo -e "${GREEN}${BOLD}Build abgeschlossen!${RESET}"
echo ""
echo "  App:  dist/NXT Scanner.app"
echo "  DMG:  dist/${DMG_NAME}"
echo ""
echo "  Nächste Schritte:"
echo "    1. App testen (Doppelklick auf .app)"
echo "    2. version.json anpassen (release_notes)"
echo "    3. git tag v${VERSION} && git push --tags"
echo "    4. GitHub Release erstellen + DMG hochladen"
echo "    5. version.json committen + pushen"
