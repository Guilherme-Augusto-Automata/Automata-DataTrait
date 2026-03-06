"""
Tratamento de dados — Equador.
Cópia de colunas por posição em arquivos de importação.
"""

import os
import time
import pandas as pd

from infrastructure.file_io import ler_arquivo, exportar_resultado, col_idx


def processar_equador(input_path: str, output_dir: str, formato: str,
                      log_callback, done_callback) -> None:
    """Orquestra o processamento do Equador."""
    try:
        df = _carregar_arquivo(input_path, log_callback)
        df = _garantir_colunas_ate_az(df)
        _copiar_colunas(df, log_callback)
        _exportar(df, input_path, output_dir, formato, log_callback)
        done_callback(True)
    except Exception as e:
        log_callback(f"❌ ERRO: {e}")
        done_callback(False)


def _carregar_arquivo(input_path: str, log_callback) -> pd.DataFrame:
    """Carrega o arquivo do Equador."""
    log_callback("📂 Carregando arquivo do Equador...")
    t0 = time.perf_counter()
    df = ler_arquivo(input_path, dtype=str)
    log_callback(f"  ✓ {len(df):,} linhas × {len(df.columns)} colunas "
                 f"({time.perf_counter()-t0:.1f}s)")
    return df


def _garantir_colunas_ate_az(df: pd.DataFrame) -> pd.DataFrame:
    """Adiciona colunas extras até a posição AZ, se necessário."""
    needed = col_idx("AZ") + 1 - len(df.columns)
    if needed > 0:
        extras = pd.DataFrame(
            {f"_extra_{len(df.columns)+i}": pd.array([None]*len(df))
             for i in range(needed)}
        )
        df = pd.concat([df, extras], axis=1)
    return df


def _copiar_colunas(df: pd.DataFrame, log_callback) -> None:
    """Copia dados entre colunas — operação vetorizada."""
    log_callback("🔄 Copiando dados entre colunas...")
    t0 = time.perf_counter()
    df.iloc[:, col_idx("AW")] = df.iloc[:, col_idx("AQ")].values
    df.iloc[:, col_idx("AX")] = df.iloc[:, col_idx("AR")].values
    df.iloc[:, col_idx("AY")] = df.iloc[:, col_idx("AL")].values
    df.iloc[:, col_idx("AZ")] = df.iloc[:, col_idx("AG")].values
    log_callback(f"  ✓ Colunas copiadas ({time.perf_counter()-t0:.2f}s)")


def _exportar(df: pd.DataFrame, input_path: str, output_dir: str,
              formato: str, log_callback) -> None:
    """Exporta o resultado tratado."""
    base_name = os.path.splitext(os.path.basename(input_path))[0]
    exportar_resultado(df, output_dir, base_name + "_tratado", formato, log_callback)
