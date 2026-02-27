# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec für NXT Scanner."""

import sys
import os

spec_dir = os.path.dirname(os.path.abspath(SPEC))
sys.path.insert(0, spec_dir)
from storage_scanner import __version__

a = Analysis(
    [os.path.join(spec_dir, 'run_app.py')],
    pathex=[spec_dir],
    binaries=[],
    datas=[],
    hiddenimports=[
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
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['tkinter', 'unittest', 'pydoc', 'doctest'],
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
    icon=None,  # TODO: Icon hinzufügen wenn vorhanden: 'resources/NXT Scanner.icns'
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
