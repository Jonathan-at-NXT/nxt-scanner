# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec für NXT Scanner."""

import sys
import os

spec_dir = os.path.dirname(os.path.abspath(SPEC))
sys.path.insert(0, spec_dir)
from storage_scanner import __version__

# Team-Build (NXT_TEAM=1): ohne Admin-Teile (Karten-Ingest, Kopieren, Notion-Auswertung).
# Admin-Build für Sicherungs-/Haupt-/Intel-Mac wird nur lokal verteilt, nie veröffentlicht.
TEAM = os.environ.get("NXT_TEAM") == "1"
ADMIN_ONLY = ['storage_scanner.ingest', 'storage_scanner.transfer', 'storage_scanner.admin']

a = Analysis(
    [os.path.join(spec_dir, 'run_app.py')],
    pathex=[spec_dir],
    binaries=[],
    datas=[
        (os.path.join(spec_dir, 'resources', 'menubar_iconTemplate.png'), 'resources'),
        (os.path.join(spec_dir, 'resources', 'menubar_iconTemplate@2x.png'), 'resources'),
    ] + ([(os.path.join(spec_dir, 'resources', 'bin', 'smartctl'), os.path.join('resources', 'bin'))]
         if os.path.exists(os.path.join(spec_dir, 'resources', 'bin', 'smartctl')) else []),
    hiddenimports=[m for m in [
        # App-Module
        'storage_scanner',
        'storage_scanner.scan',
        'storage_scanner.notion_sync',
        'storage_scanner.rules',
        'storage_scanner.analyzer',
        'storage_scanner.report',
        'storage_scanner.utils',
        'storage_scanner.paths',
        'storage_scanner.updater',
        'storage_scanner.admin',
        'storage_scanner.db',
        'storage_scanner.catalog',
        'storage_scanner.transfer',
        'storage_scanner.outbox',
        'storage_scanner.health',
        'storage_scanner.progress',
        'storage_scanner.d1_client',
        'storage_scanner.ingest',
        'storage_scanner.ingest.camera_map',
        'storage_scanner.ingest.organize',
        'storage_scanner.ingest.verify',
        'storage_scanner.ingest.copier',
        'storage_scanner.ingest.store',
        'storage_scanner.ingest.orchestrator',
        'storage_scanner.ingest.device_monitor',
        'storage_scanner.ingest.d1_projects',
        # rumps
        'rumps',
        # PyObjC
        'AppKit',
        'Foundation',
        'objc',
        'PyObjCTools',
        # httpx + Abhängigkeiten
        'httpx',
        'httpx._transports',
        'httpx._transports.default',
        'httpcore',
        'httpcore._sync',
        'httpcore._async',
        'h11',
        'certifi',
        'idna',
        'sniffio',
        'anyio',
        'anyio._backends',
        'anyio._backends._asyncio',
        # tqdm
        'tqdm',
        'tqdm.auto',
    ] if not (TEAM and any(m == x or m.startswith(x + '.') for x in ADMIN_ONLY))],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'tkinter', 'unittest', 'pydoc', 'doctest',
        # ARM-only Packages – nicht von der App genutzt, blockieren x86_64-Build
        'PIL', 'Pillow', 'numpy', 'pandas', 'matplotlib', 'scipy', 'sklearn',
        'contourpy', 'cv2', 'hf_transfer', 'hf_xet', 'kiwisolver',
        'markupsafe', 'mlx', 'regex', 'safetensors', 'sentencepiece',
        'tokenizers', 'torch', 'yaml',
    ] + (ADMIN_ONLY if TEAM else []),
    noarchive=False,
    optimize=0,
)

pyz = PYZ(a.pure, a.zipped_data)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='NXT Scanner',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=False,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=False,
    name='NXT Scanner',
)

app = BUNDLE(
    coll,
    name='NXT Scanner.app',
    icon=os.path.join(spec_dir, 'resources', 'AppIcon.icns'),
    bundle_identifier='com.nxtstudios.nxt-scanner',
    version=__version__,
    info_plist={
        'CFBundleName': 'NXT Scanner',
        'CFBundleDisplayName': 'NXT Scanner',
        'CFBundleVersion': __version__,
        'CFBundleShortVersionString': __version__,
        'CFBundleIdentifier': 'com.nxtstudios.nxt-scanner',
        'LSMinimumSystemVersion': '12.0',
        'LSUIElement': True,
        'NSHighResolutionCapable': True,
        'NSRequiresAquaSystemAppearance': False,
    },
)

# ── Nachbearbeitung: eigene libexpat ───────────────────────────────
# Homebrew-Python baut pyexpat für das macOS des Build-Rechners; auf älterem macOS fehlt der
# System-libexpat dann ein Symbol (_XML_SetAllocTrackerActivationThreshold) und die App startet
# nicht. Darum eine eigene libexpat mitliefern und pyexpat darauf zeigen lassen.
import glob, shutil, subprocess
_expat = '/opt/homebrew/opt/expat/lib/libexpat.1.dylib'
_fw = os.path.join(DISTPATH, 'NXT Scanner.app', 'Contents', 'Frameworks')
if not os.path.exists(_expat):
    raise SystemExit('libexpat fehlt: brew install expat')
shutil.copy2(os.path.realpath(_expat), os.path.join(_fw, 'libexpat.1.dylib'))
os.chmod(os.path.join(_fw, 'libexpat.1.dylib'), 0o755)
subprocess.run(['install_name_tool', '-id', '@rpath/libexpat.1.dylib', os.path.join(_fw, 'libexpat.1.dylib')], check=True)
for _so in {os.path.realpath(p) for p in glob.glob(os.path.join(_fw, '**', 'pyexpat*.so'), recursive=True)}:
    _rel = os.path.relpath(_fw, os.path.dirname(_so))
    subprocess.run(['install_name_tool', '-change', '/usr/lib/libexpat.1.dylib',
                    f'@loader_path/{_rel}/libexpat.1.dylib', _so], check=True)
