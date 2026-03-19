# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec für NXT Reformat Tool."""

import sys
import os

spec_dir = os.path.dirname(os.path.abspath(SPEC))

a = Analysis(
    [os.path.join(spec_dir, 'reformat_tool.py')],
    pathex=[spec_dir],
    binaries=[],
    datas=[],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'tkinter', 'unittest', 'pydoc', 'doctest',
        'PIL', 'Pillow', 'numpy', 'pandas', 'matplotlib', 'scipy', 'sklearn',
        'contourpy', 'cv2', 'hf_transfer', 'hf_xet', 'kiwisolver',
        'markupsafe', 'mlx', 'regex', 'safetensors', 'sentencepiece',
        'tokenizers', 'torch', 'yaml',
    ],
    noarchive=False,
    optimize=0,
)

pyz = PYZ(a.pure, a.zipped_data)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='NXT Reformat Tool',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=True,
)
