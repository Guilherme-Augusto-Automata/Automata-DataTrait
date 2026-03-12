"""
Script para gerar o executável (.exe) do aplicativo.
Execute: python build_exe.py  (a partir da raiz do projeto)
"""
import subprocess
import sys
import os


def build():
    # Garante que PyInstaller está instalado
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pyinstaller"])

    project_dir = os.path.dirname(os.path.abspath(__file__))
    main_path = os.path.join(project_dir, "main.py")

    # Monta o comando PyInstaller
    cmd = [
        sys.executable, "-m", "PyInstaller",
        "--noconfirm",
        "--onefile",
        "--windowed",
        "--name", "TratamentoDados",
        "--icon", "NONE",
        # Adiciona a raiz do projeto ao sys.path para encontrar os pacotes locais
        "--paths", project_dir,
        # Hidden imports: libs lazy-loaded ou usadas como engine string
        "--hidden-import", "tkinterdnd2",
        "--hidden-import", "customtkinter",
        "--hidden-import", "google.genai",
        "--hidden-import", "anthropic",
        "--hidden-import", "python_calamine",
        "--hidden-import", "openpyxl",
        "--hidden-import", "polars",
        "--hidden-import", "pyarrow",
        "--hidden-import", "_logo_data",
        "--hidden-import", "ahocorasick",
        # Coletar dados completos dos pacotes que usam arquivos de tema/assets
        "--collect-all", "customtkinter",
        "--collect-all", "tkinterdnd2",
        "--collect-all", "google",
        "--collect-all", "anthropic",
        main_path,
    ]

    # Incluir _logo_data.py (logo embutida) como dado adicional
    logo_data_path = os.path.join(project_dir, "Legacy", "_logo_data.py")
    if os.path.exists(logo_data_path):
        cmd.insert(-1, "--add-data")
        cmd.insert(-1, f"{logo_data_path};.")
        print("  Logo embutida incluída: Legacy/_logo_data.py")

    print("\n🔨 Construindo executável...")
    print(f"  Comando: {' '.join(cmd)}\n")
    subprocess.check_call(cmd, cwd=project_dir)

    print("\n" + "=" * 60)
    print("✅ Executável gerado em: dist/TratamentoDados.exe")
    print("=" * 60)


if __name__ == "__main__":
    build()
