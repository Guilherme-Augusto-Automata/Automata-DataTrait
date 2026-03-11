"""
Preparação de dados para o banco de dados.
Mapeia colunas do arquivo tratado para o formato padrão.
Cada função faz uma única coisa.
"""

import re
import os
import time

import pandas as pd

from config.settings import DB_COLUMN_MAP, DB_EMPTY_COLS, DB_OUTPUT_COLUMNS
from infrastructure.file_io import ler_arquivo, exportar_banco


# ============================================================
# ORQUESTRADOR
# ============================================================

def processar_banco_dados(input_path: str, output_dir: str, formato: str,
                          linhas_por_arquivo: int,
                          log_callback, done_callback) -> None:
    """Orquestra a preparação do arquivo para o banco de dados."""
    try:
        df = _carregar_arquivo(input_path, log_callback)
        resultado = _mapear_colunas(df, log_callback)
        _padronizar_datas(resultado, log_callback)
        _adicionar_colunas_vazias(resultado, log_callback)
        _adicionar_colunas_repetidas(resultado, log_callback)
        resultado = _reordenar_colunas(resultado, log_callback)
        _log_resumo(resultado, df, log_callback)

        base_name = os.path.splitext(os.path.basename(input_path))[0] + "_banco"
        _dividir_e_exportar(resultado, base_name, output_dir, formato,
                            linhas_por_arquivo, log_callback)
        done_callback(True)
    except Exception as e:
        log_callback(f"❌ ERRO: {e}")
        done_callback(False)


# ============================================================
# CARREGAMENTO
# ============================================================

def _carregar_arquivo(input_path: str, log_callback) -> pd.DataFrame:
    """Carrega o arquivo de entrada como texto."""
    log_callback("📂 Carregando arquivo...")
    t0 = time.perf_counter()
    df = ler_arquivo(input_path, dtype=str)
    log_callback(f"  ✓ {len(df):,} linhas × {len(df.columns)} colunas "
                 f"({time.perf_counter()-t0:.1f}s)")
    return df


# ============================================================
# MAPEAMENTO DE COLUNAS
# ============================================================

def _find_column(df_columns_upper: list, candidates: list) -> str | None:
    """Encontra a coluna que corresponde aos candidatos (case-insensitive).
    Prefere a última ocorrência (colunas tratadas, adicionadas ao final)."""
    for cand in candidates:
        cand_upper = cand.upper().strip()
        found = None
        for orig, upper in df_columns_upper:
            if upper == cand_upper:
                found = orig
        if found is not None:
            return found
    return None


def _mapear_colunas(df: pd.DataFrame, log_callback) -> pd.DataFrame:
    """Mapeia colunas do DataFrame original para colunas destino."""
    log_callback("\n🔄 Mapeando colunas...")
    cols_upper = [(c, c.upper().strip()) for c in df.columns]
    resultado = pd.DataFrame(index=df.index)
    mapeamentos_ok = 0

    for destino, candidatos in DB_COLUMN_MAP:
        col_encontrada = _find_column(cols_upper, candidatos)
        if col_encontrada is not None:
            resultado[destino] = df[col_encontrada].values
            log_callback(f"  ✓ '{col_encontrada}' → {destino}")
            mapeamentos_ok += 1
        else:
            resultado[destino] = ""
            nomes = ", ".join(candidatos)
            log_callback(f"  ⚠️ {destino} — nenhuma coluna encontrada ({nomes})")

    log_callback(f"\n📊 Resumo: {mapeamentos_ok}/{len(DB_COLUMN_MAP)} colunas mapeadas")
    return resultado


# ============================================================
# PADRONIZAÇÃO DE DATAS
# ============================================================

_RE_ISO_DATETIME = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})(?:\s+\d{2}:\d{2}:\d{2})?$"
)
_RE_SLASH_DMY = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")
_RE_DIGITS_8 = re.compile(r"^(\d{1,2})(\d{2})(\d{4})$")


def _padronizar_datas(resultado: pd.DataFrame, log_callback) -> None:
    """Padroniza a coluna FECHA_LEVANTE para dd/mm/yyyy."""
    if "FECHA_LEVANTE" not in resultado.columns:
        return

    log_callback("\n📅 Padronizando datas (FECHA_LEVANTE → dd/mm/yyyy)...")
    resultado["FECHA_LEVANTE"] = (
        resultado["FECHA_LEVANTE"]
        .fillna("")
        .astype(str)
        .str.strip()
        .apply(_normalizar_data)
    )

    # Estatísticas
    total = len(resultado)
    validas = int(resultado["FECHA_LEVANTE"].str.match(
        r"^\d{2}/\d{2}/\d{4}$", na=False).sum())
    log_callback(f"  ✓ {validas:,}/{total:,} datas padronizadas")
    if validas < total:
        invalidas = resultado.loc[
            ~resultado["FECHA_LEVANTE"].str.match(r"^\d{2}/\d{2}/\d{4}$", na=False),
            "FECHA_LEVANTE"
        ]
        amostras = invalidas[invalidas != ""].head(5).tolist()
        if amostras:
            log_callback(f"  ⚠️ Amostras não reconhecidas: {amostras}")


def _normalizar_data(valor: str) -> str:
    """Converte um valor de data para dd/mm/yyyy."""
    if not valor or valor in ("nan", "None", "NaN", ""):
        return ""

    # 2025-01-06 00:00:00  ou  2025-01-06
    m = _RE_ISO_DATETIME.match(valor)
    if m:
        return f"{m.group(3)}/{m.group(2)}/{m.group(1)}"

    # 3/12/2025  ou  03/12/2025
    m = _RE_SLASH_DMY.match(valor)
    if m:
        return f"{int(m.group(1)):02d}/{int(m.group(2)):02d}/{m.group(3)}"

    # 3122025  ou  27052025  (d+mm+yyyy ou dd+mm+yyyy → 7 ou 8 dígitos)
    digits = valor.replace(" ", "")
    m = _RE_DIGITS_8.match(digits)
    if m:
        return f"{int(m.group(1)):02d}/{m.group(2)}/{m.group(3)}"

    # Fallback: tentar pandas
    try:
        dt = pd.to_datetime(valor, dayfirst=True)
        return dt.strftime("%d/%m/%Y")
    except (ValueError, TypeError):
        return valor


# ============================================================
# COLUNAS DERIVADAS
# ============================================================

def _adicionar_colunas_vazias(resultado: pd.DataFrame, log_callback) -> None:
    """Adiciona colunas que devem ficar vazias."""
    for col_vazia in DB_EMPTY_COLS:
        resultado[col_vazia] = ""


def _adicionar_colunas_repetidas(resultado: pd.DataFrame,
                                 log_callback) -> None:
    """Duplica colunas que precisam aparecer com dois nomes."""
    resultado["IMPORTADORES"] = resultado["RAZON_SOCIAL_IMPORTADOR"].values
    resultado["CANTIDAD"] = resultado["CANTIDAD_DCMS"].values
    resultado["VALOR_FOB_USD_2"] = resultado["VALOR_FOB_USD"].values
    log_callback("\n📋 Colunas repetidas:")
    log_callback("  ✓ RAZON_SOCIAL_IMPORTADOR → IMPORTADORES")
    log_callback("  ✓ CANTIDAD_DCMS → CANTIDAD")
    log_callback("  ✓ VALOR_FOB_USD → VALOR_FOB_USD_2")


def _reordenar_colunas(resultado: pd.DataFrame,
                       log_callback) -> pd.DataFrame:
    """Reordena colunas conforme a ordem padrão do banco."""
    return resultado[DB_OUTPUT_COLUMNS]


# ============================================================
# RESUMO
# ============================================================

def _log_resumo(resultado: pd.DataFrame, df_original: pd.DataFrame,
                log_callback) -> None:
    """Loga resumo do resultado final."""
    log_callback(f"  Total de linhas: {len(resultado):,}")
    log_callback(f"  Total de colunas: {len(resultado.columns)}")


# ============================================================
# DIVISÃO E EXPORTAÇÃO
# ============================================================

def _dividir_e_exportar(resultado: pd.DataFrame, base_name: str,
                        output_dir: str, formato: str,
                        linhas_por_arquivo: int, log_callback) -> None:
    """Divide o resultado em partes e exporta cada uma."""
    total_linhas = len(resultado)

    if linhas_por_arquivo > 0 and total_linhas > linhas_por_arquivo:
        _exportar_em_partes(resultado, base_name, output_dir, formato,
                            linhas_por_arquivo, log_callback)
    else:
        exportar_banco(resultado, output_dir, base_name, formato, log_callback)


def _exportar_em_partes(resultado: pd.DataFrame, base_name: str,
                        output_dir: str, formato: str,
                        linhas_por_arquivo: int, log_callback) -> None:
    """Exporta DataFrame dividido em múltiplos arquivos."""
    total_linhas = len(resultado)
    n_partes = (total_linhas + linhas_por_arquivo - 1) // linhas_por_arquivo
    log_callback(f"\n✂️ Dividindo em {n_partes} parte(s) de até "
                 f"{linhas_por_arquivo:,} linhas...")

    for i in range(n_partes):
        inicio = i * linhas_por_arquivo
        fim = min((i + 1) * linhas_por_arquivo, total_linhas)
        parte_df = resultado.iloc[inicio:fim]
        parte_name = f"{base_name}_parte{i+1}"
        log_callback(f"\n📦 Parte {i+1}/{n_partes}: linhas {inicio+1:,} a "
                     f"{fim:,} ({len(parte_df):,} linhas)")
        exportar_banco(parte_df, output_dir, parte_name, formato, log_callback)
