"""
Funções de leitura e escrita de arquivos (Excel / CSV).
Cada função tem uma única responsabilidade.
"""

import os
import time
import pandas as pd


# ============================================================
# LEITURA
# ============================================================

def ler_arquivo(path: str, **kwargs) -> pd.DataFrame:
    """Lê Excel (.xlsx/.xls) ou CSV separado por | (.csv)."""
    if path.lower().endswith(".csv"):
        return _ler_csv(path, **kwargs)
    return _ler_excel(path, **kwargs)


def _ler_csv(path: str, **kwargs) -> pd.DataFrame:
    """Lê arquivo CSV separado por pipe."""
    return pd.read_csv(path, sep="|", encoding="utf-8-sig",
                       dtype=kwargs.pop("dtype", None), **kwargs)


def _ler_excel(path: str, **kwargs) -> pd.DataFrame:
    """Lê arquivo Excel (.xlsx / .xls)."""
    engine = kwargs.pop("engine", "calamine")
    return pd.read_excel(path, engine=engine, **kwargs)


# ============================================================
# CONVERSÃO DE COLUNA
# ============================================================

def col_idx(col: str) -> int:
    """Converte letra de coluna Excel (ex: 'AQ') para índice 0-based."""
    col = col.upper()
    result = 0
    for char in col:
        result = result * 26 + (ord(char) - ord('A') + 1)
    return result - 1


# ============================================================
# EXPORTAÇÃO — RESULTADO TRATADO
# ============================================================

def exportar_resultado(df: pd.DataFrame, output_dir: str, base_name: str,
                       formato: str, log_callback) -> None:
    """Despacha exportação nos formatos selecionados."""
    if formato in ("xlsx", "ambos"):
        path = _caminho_saida(output_dir, base_name, ".xlsx")
        _salvar_xlsx_com_debug(df, path, log_callback)
    if formato in ("csv", "ambos"):
        path = _caminho_saida(output_dir, base_name, ".csv")
        _salvar_csv(df, path, log_callback)


# ============================================================
# EXPORTAÇÃO — BANCO DE DADOS
# ============================================================

def exportar_banco(df: pd.DataFrame, output_dir: str, base_name: str,
                   formato: str, log_callback) -> None:
    """Despacha exportação de banco nos formatos selecionados."""
    log_callback(f"  📊 [EXPORT] DataFrame com {len(df):,} linhas × "
                 f"{len(df.columns)} colunas para exportação")
    if formato in ("xlsx", "ambos"):
        path = _caminho_saida(output_dir, base_name, ".xlsx")
        _salvar_xlsx_simples(df, path, log_callback)
    if formato in ("csv", "ambos"):
        path = _caminho_saida(output_dir, base_name, ".csv")
        _salvar_csv(df, path, log_callback)


# ============================================================
# FUNÇÕES PRIVADAS — CADA UMA FAZ UMA ÚNICA COISA
# ============================================================

def _caminho_saida(output_dir: str, base_name: str, ext: str) -> str:
    """Constrói o caminho completo de saída."""
    return os.path.join(output_dir, base_name + ext)


def _salvar_csv(df: pd.DataFrame, path: str, log_callback) -> None:
    """Salva DataFrame como CSV (pipe-separated)."""
    log_callback(f"📝 Salvando {os.path.basename(path)}...")
    t0 = time.perf_counter()
    df.to_csv(path, index=False, sep="|", encoding="utf-8-sig")
    log_callback(f"  ✓ CSV salvo ({time.perf_counter()-t0:.1f}s): {path}")


def _salvar_xlsx_com_debug(df: pd.DataFrame, path: str, log_callback) -> None:
    """Salva XLSX via Polars com debug e verificação pós-save."""
    log_callback(f"\n📝 Salvando {os.path.basename(path)}...")
    t0 = time.perf_counter()
    try:
        import polars as pl
        df_pl = pl.from_pandas(df)
        _debug_polars_colunas(df, df_pl, log_callback)
        df_pl.write_excel(path)
        log_callback(f"  ✓ XLSX salvo via Polars ({time.perf_counter()-t0:.1f}s): {path}")
        _verificar_pos_save(path, log_callback)
    except PermissionError:
        log_callback("  ⚠️ Arquivo aberto em outro programa! Feche no Excel e tente novamente.")
        raise
    except Exception as e:
        log_callback(f"  ⚠️ Polars falhou ({e}), tentando openpyxl...")
        _salvar_xlsx_openpyxl(df, path, log_callback)


def _salvar_xlsx_simples(df: pd.DataFrame, path: str, log_callback) -> None:
    """Salva XLSX via Polars (sem debug, para banco de dados)."""
    log_callback(f"📝 Salvando {os.path.basename(path)}...")
    t0 = time.perf_counter()
    try:
        import polars as pl
        df_pl = pl.from_pandas(df)
        df_pl.write_excel(path)
        log_callback(f"  ✓ XLSX salvo via Polars ({time.perf_counter()-t0:.1f}s)")
    except PermissionError:
        log_callback("  ⚠️ Arquivo aberto em outro programa! Feche e tente novamente.")
        raise
    except Exception:
        _salvar_xlsx_openpyxl(df, path, log_callback)


def _salvar_xlsx_openpyxl(df: pd.DataFrame, path: str, log_callback) -> None:
    """Salva XLSX como fallback via openpyxl."""
    t0 = time.perf_counter()
    df.to_excel(path, index=False, engine="openpyxl")
    log_callback(f"  ✓ XLSX salvo via openpyxl ({time.perf_counter()-t0:.1f}s): {path}")


def _debug_polars_colunas(df: pd.DataFrame, df_pl, log_callback) -> None:
    """Loga comparação pandas vs polars das últimas 8 colunas."""
    log_callback("  🔍 DEBUG Polars — últimas 8 colunas:")
    for col in df.columns[-8:]:
        pd_nn = int(df[col].notna().sum())
        pl_nn = int(df_pl[col].null_count())
        pl_dtype = df_pl[col].dtype
        pl_sample = (df_pl[col].drop_nulls()[0]
                     if (len(df_pl) - pl_nn) > 0 else 'VAZIO')
        log_callback(
            f"    '{col}': pandas={pd_nn:,} não-nulos | "
            f"polars={len(df_pl)-pl_nn:,} não-nulos (dtype={pl_dtype}) | "
            f"ex: {repr(pl_sample)[:50]}"
        )


def _verificar_pos_save(path: str, log_callback) -> None:
    """Relê o XLSX salvo e verifica se as últimas colunas têm dados."""
    try:
        df_check = pd.read_excel(path, engine='calamine', nrows=5)
        log_callback("  🔎 Verificação pós-save (5 linhas):")
        for col in df_check.columns[-8:]:
            nn = int(df_check[col].notna().sum())
            log_callback(f"    '{col}': {nn}/5 preenchidas")
    except Exception as ve:
        log_callback(f"  ⚠️ Verificação falhou: {ve}")
