"""
Script para gerar o executável (.exe) do aplicativo.
Execute: python build_exe.py
"""
import subprocess
import sys
import os

def build():
    # Garante que PyInstaller está instalado
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pyinstaller"])

    base_dir = os.path.dirname(os.path.abspath(__file__))
    app_path = os.path.join(base_dir, "app.py")

    # Monta o comando PyInstaller
    cmd = [
        sys.executable, "-m", "PyInstaller",
        "--noconfirm",
        "--onefile",
        "--windowed",
        "--name", "TratamentoDados",
        "--icon", "NONE",
        # Incluir tkdnd (necessário para drag-and-drop)
        "--hidden-import", "tkinterdnd2",
        "--hidden-import", "customtkinter",
        "--hidden-import", "google.genai",
        "--hidden-import", "python_calamine",
        "--hidden-import", "openpyxl",
        "--hidden-import", "_logo_data",
        "--hidden-import", "ahocorasick",
        # Coletar dados do customtkinter
        "--collect-all", "customtkinter",
        "--collect-all", "tkinterdnd2",
        "--collect-all", "google",
        app_path,
    ]

    # Incluir _logo_data.py (logo embutida) como dado adicional
    logo_data_path = os.path.join(base_dir, "_logo_data.py")
    if os.path.exists(logo_data_path):
        cmd.insert(-1, "--add-data")
        cmd.insert(-1, f"{logo_data_path};.")
        print("  Logo embutida incluída: _logo_data.py")

    print("\n🔨 Construindo executável...")
    print(f"  Comando: {' '.join(cmd)}\n")
    subprocess.check_call(cmd)

    print("\n" + "=" * 60)
    print("✅ Executável gerado em: dist/TratamentoDados.exe")
    print("   Logo embutida no executável — nenhum arquivo extra necessário!")
    print("=" * 60)


if __name__ == "__main__":
    build()
