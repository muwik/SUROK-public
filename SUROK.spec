# -*- mode: python ; coding: utf-8 -*-


block_cipher = None


a = Analysis(
    ['surokGUI.py'],
    pathex=['/Users/ivannochovkin/PycharmProjects/lessinitea/venv/lib/python3.8/site-packages'],
    binaries=[],
    datas=[('assets', 'assets'), ('templates', 'templates'), ('settings.json', '.')],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='SUROK',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='marmot_logo.icns',
)
coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='SUROK',
)
app = BUNDLE(
    coll,
    name='SUROK.app',
    icon='marmot_logo.icns',
    bundle_identifier=None,
)
