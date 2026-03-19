#!/bin/bash
set -euo pipefail

# ── NXT Scanner – Release-Skript ──────────────────────────────────
# Baut die .app für eine bestimmte Architektur und erstellt DMG + ZIP.
#
# Nutzung:
#   ./release.sh                    # Native Architektur (arm64 auf Apple Silicon)
#   ./release.sh --arch arm64       # ARM Build
#   ./release.sh --arch x86_64      # Intel Build (benötigt Rosetta)
#   ./release.sh --admin            # Admin-Build: nur .app, kein DMG/ZIP, wird gestartet
#   ./release.sh --all              # ARM + Intel + Admin in einem Durchlauf

BOLD="\033[1m"
GREEN="\033[32m"
RED="\033[31m"
RESET="\033[0m"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Python 3.13 mit PyInstaller + Dependencies verwenden
PYTHON="/Library/Frameworks/Python.framework/Versions/3.13/bin/python3"
if [ ! -x "$PYTHON" ]; then
    PYTHON="python3"
fi

# ── Argumente parsen ─────────────────────────────────────────────
ARCH=""
ADMIN=false
ALL=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --arch)   ARCH="$2"; shift 2 ;;
        --admin)  ADMIN=true; shift ;;
        --all)    ALL=true; shift ;;
        *)        echo "Unbekannte Option: $1"; exit 1 ;;
    esac
done

# --all: Rekursiv für alle Varianten aufrufen
if $ALL; then
    echo -e "${BOLD}=== Baue alle Varianten ===${RESET}"
    echo ""
    "$0" --arch arm64
    echo ""
    echo -e "${BOLD}──────────────────────────────────────${RESET}"
    echo ""
    "$0" --arch x86_64
    echo ""
    echo -e "${BOLD}──────────────────────────────────────${RESET}"
    echo ""
    "$0" --admin
    exit 0
fi

# Architektur bestimmen
if [ -z "$ARCH" ]; then
    ARCH=$(uname -m)
fi

# Version aus Python-Package lesen
VERSION=$($PYTHON -c "import sys; sys.path.insert(0,'.'); from storage_scanner import __version__; print(__version__)")

if $ADMIN; then
    echo -e "${BOLD}NXT Scanner v${VERSION} – Admin Build${RESET}"
else
    echo -e "${BOLD}NXT Scanner v${VERSION} – Build (${ARCH})${RESET}"
fi
echo ""

# ── 1. Aufräumen ──────────────────────────────────────────────────
echo -e "${BOLD}[1/6] Aufräumen...${RESET}"
rm -rf build/ dist/

# ── 2. Abhängigkeiten prüfen ─────────────────────────────────────
echo -e "${BOLD}[2/6] Abhängigkeiten prüfen...${RESET}"

if ! $PYTHON -m PyInstaller --version &>/dev/null; then
    echo "  PyInstaller nicht gefunden. Installiere..."
    $PYTHON -m pip install --quiet pyinstaller
fi

if ! $ADMIN; then
    if ! command -v create-dmg &>/dev/null; then
        echo -e "${RED}create-dmg nicht gefunden.${RESET}"
        echo "  Installieren mit: brew install create-dmg"
        exit 1
    fi
fi

echo "  Alles vorhanden"

# ── 3. App bauen ─────────────────────────────────────────────────
echo -e "${BOLD}[3/6] App bauen (PyInstaller, ${ARCH})...${RESET}"

if [ "$ARCH" = "x86_64" ]; then
    arch -x86_64 $PYTHON -m PyInstaller nxt_scanner.spec --noconfirm --clean 2>&1 | tail -5
else
    $PYTHON -m PyInstaller nxt_scanner.spec --noconfirm --clean 2>&1 | tail -5
fi

if [ ! -d "dist/NXT Scanner.app" ]; then
    echo -e "${RED}Build fehlgeschlagen – keine .app erstellt.${RESET}"
    exit 1
fi
echo "  dist/NXT Scanner.app erstellt (${ARCH})"

# ── 4. Code-Signierung ──────────────────────────────────────────
echo -e "${BOLD}[4/6] Code-Signierung (ad-hoc)...${RESET}"
codesign --force --deep --sign - "dist/NXT Scanner.app"
echo "  Signiert"

# ── Admin: App starten und fertig ────────────────────────────────
if $ADMIN; then
    echo ""
    echo -e "${GREEN}${BOLD}Admin-Build abgeschlossen!${RESET}"
    echo "  Starte App..."
    open "dist/NXT Scanner.app"
    exit 0
fi

# ── 5. ZIP erstellen (für Auto-Update) ──────────────────────────
echo -e "${BOLD}[5/6] ZIP erstellen (für Auto-Update)...${RESET}"
ZIP_NAME="NXT-Scanner-${VERSION}-${ARCH}.zip"
(cd dist && zip -r -q "${ZIP_NAME}" "NXT Scanner.app")
echo "  dist/${ZIP_NAME} erstellt"

# ── 6. DMG erstellen ────────────────────────────────────────────
echo -e "${BOLD}[6/6] DMG erstellen...${RESET}"
DMG_NAME="NXT-Scanner-${VERSION}-${ARCH}.dmg"
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
echo -e "${GREEN}${BOLD}Build abgeschlossen! (${ARCH})${RESET}"
echo ""
echo "  App:  dist/NXT Scanner.app"
echo "  ZIP:  dist/${ZIP_NAME}"
echo "  DMG:  dist/${DMG_NAME}"
echo ""
echo "  Nächste Schritte:"
echo "    1. App testen (Doppelklick auf .app)"
echo "    2. version.json anpassen (release_notes)"
echo "    3. git tag v${VERSION} && git push --tags"
echo "    4. GitHub Release erstellen + DMG + ZIP hochladen"
echo "    5. version.json committen + pushen"
