"""
Tratamento de Dados — Equador & Argentina
Interface gráfica moderna com CustomTkinter
Otimizado com operações vetorizadas (pandas/numpy)
"""

import customtkinter as ctk
from tkinter import filedialog, messagebox
from tkinterdnd2 import DND_FILES, TkinterDnD
from PIL import Image
import pandas as pd
import numpy as np
import re
import json
import time
import threading
import os
import sys
import base64
import io

# Otimização: PyArrow para operações de string 2-5× mais rápidas
try:
    import pyarrow  # noqa: F401
    _STR_DTYPE = "string[pyarrow]"
except ImportError:
    _STR_DTYPE = "string"

# ============================================================
# PALETA DE CORES
# ============================================================
COLORS = {
    "background":   "#0F172A",
    "surface":      "#1E293B",
    "primary":      "#22487A",
    "secondary":    "#0065B3",
    "error":        "#EF5350",
    "info":         "#38BDF8",
    "success":      "#66BB6A",
    "warning":      "#FFA726",
    "text":         "#E2E8F0",
    "text_dim":     "#94A3B8",
    "card":         "#141F33",
    "card_hover":   "#243750",
    "border":       "#273D55",
}

# ============================================================
# FUNÇÕES DE PROCESSAMENTO (VETORIZADAS)
# ============================================================

def ler_arquivo(path, **kwargs):
    """Lê Excel (.xlsx/.xls) ou CSV separado por | (.csv). Aceita kwargs extras para pandas."""
    if path.lower().endswith(".csv"):
        return pd.read_csv(path, sep="|", encoding="utf-8-sig", dtype=kwargs.pop("dtype", None), **kwargs)
    else:
        engine = kwargs.pop("engine", "calamine")
        return pd.read_excel(path, engine=engine, **kwargs)


def col_idx(col: str) -> int:
    """Converte letra de coluna Excel (ex: 'AQ') para índice 0-based."""
    col = col.upper()
    result = 0
    for char in col:
        result = result * 26 + (ord(char) - ord('A') + 1)
    return result - 1


def processar_equador(input_path, output_dir, formato, log_callback, done_callback):
    """Processa arquivo do Equador — cópia de colunas por posição."""
    try:
        log_callback("📂 Carregando arquivo do Equador...")
        t0 = time.perf_counter()
        df = ler_arquivo(input_path, dtype=str)
        log_callback(f"  ✓ {len(df):,} linhas × {len(df.columns)} colunas ({time.perf_counter()-t0:.1f}s)")

        # Garantir colunas suficientes até AZ — batch O(n) em vez de O(c×n)
        needed = col_idx("AZ") + 1 - len(df.columns)
        if needed > 0:
            extras = pd.DataFrame(
                {f"_extra_{len(df.columns)+i}": pd.array([None]*len(df)) for i in range(needed)}
            )
            df = pd.concat([df, extras], axis=1)

        log_callback("🔄 Copiando dados entre colunas...")
        t0 = time.perf_counter()

        # Cópias vetorizadas diretas via .values
        df.iloc[:, col_idx("AW")] = df.iloc[:, col_idx("AQ")].values
        df.iloc[:, col_idx("AX")] = df.iloc[:, col_idx("AR")].values
        df.iloc[:, col_idx("AY")] = df.iloc[:, col_idx("AL")].values
        df.iloc[:, col_idx("AZ")] = df.iloc[:, col_idx("AG")].values

        log_callback(f"  ✓ Colunas copiadas ({time.perf_counter()-t0:.2f}s)")

        base_name = os.path.splitext(os.path.basename(input_path))[0]
        exportar_resultado(df, output_dir, base_name + "_tratado", formato, log_callback)
        done_callback(True)
    except Exception as e:
        log_callback(f"❌ ERRO: {e}")
        done_callback(False)


def processar_argentina(input_path, output_dir, formato, api_key, ano_cotacao,
                        log_callback, done_callback, cotacoes_callback):
    """Processa arquivo da Argentina — extração vetorizada + conversão moeda via IA."""
    try:
        log_callback("📂 Carregando arquivo da Argentina...")
        t0 = time.perf_counter()
        if input_path.lower().endswith(".csv"):
            df = ler_arquivo(input_path)
        else:
            df = ler_arquivo(input_path, sheet_name="Planilha1")
        log_callback(f"  ✓ {len(df):,} linhas × {len(df.columns)} colunas ({time.perf_counter()-t0:.1f}s)")
        if _STR_DTYPE == "string[pyarrow]":
            log_callback("  ⚡ PyArrow ativo — operações de string otimizadas")

        # --- MARCA (vetorizado com str.extract) ---
        log_callback("\n🏷️  Extraindo MARCA (vetorizado)...")
        t0 = time.perf_counter()
        df["MARCA"] = (
            df["Marca - Sufixos"]
            .astype(_STR_DTYPE)
            .str.extract(r"MARCA:\s*(.+)", expand=False)
            .str.strip()
        )
        marcas_ok = df["MARCA"].notna().sum()
        log_callback(f"  ✓ {marcas_ok:,} marcas extraídas ({time.perf_counter()-t0:.2f}s)")

        # --- PARTNUMBER (vetorizado — último parêntese) ---
        log_callback("🔧 Extraindo PARTNUMBER (vetorizado)...")
        t0 = time.perf_counter()
        df["PARTNUMBER"] = (
            df["Marca ou Descrição"]
            .astype(_STR_DTYPE)
            .str.extract(r".*\(([^)]+)\)", expand=False)
            .str.strip()
        )
        pn_ok = df["PARTNUMBER"].notna().sum()
        log_callback(f"  ✓ {pn_ok:,} partnumbers extraídos ({time.perf_counter()-t0:.2f}s)")

        # --- CANTIDAD (cópia direta) ---
        log_callback("📊 Copiando CANTIDAD...")
        df["CANTIDAD"] = df["Quantidade.1"]
        log_callback(f"  ✓ {df['CANTIDAD'].notna().sum():,} cantidades copiadas")

        # --- FOB (conversão de moeda) ---
        log_callback("\n💰 Preparando conversão FOB...")
        moedas_distintas = df["Moeda"].unique().tolist()
        moedas_outras = [m for m in moedas_distintas
                         if m != "DOLAR USA" and pd.notna(m) and str(m).strip() != "`"]

        log_callback(f"  Moedas encontradas: {len(moedas_distintas)}")
        for m in moedas_outras:
            count = (df["Moeda"] == m).sum()
            log_callback(f"    • {m}: {count:,} linhas")

        taxas_cambio = {}

        # Cotações fallback (médias aproximadas 2025) caso a IA falhe
        FALLBACK_COTACOES = {
            "EURO": 1.08, "YENS": 0.0067, "YUAN": 0.137,
            "CORONAS SUECAS": 0.095, "FRANCOS SUIZOS": 1.13,
            "PESOS": 0.00085, "LIBRA ESTERLINA": 1.27,
            "REAL": 0.175, "CORONAS DANESAS": 0.145,
            "DOLAR AUSTRALIA": 0.645, "DOLAR CANADIENS": 0.73,
            "CORONAS NORUEGA": 0.091, "DOLAR NEOZELAND": 0.59,
            "RAND": 0.054,
        }

        if moedas_outras:
            log_callback("\n🤖 Consultando Gemini AI para cotações...")
            ia_sucesso = False

            try:
                from google import genai
                client = genai.Client(api_key=api_key)
                moedas_lista = ", ".join(moedas_outras)
                prompt = (
                    f"Você é um assistente especializado em dados históricos de câmbio.\n"
                    f"Preciso da cotação MÉDIA ANUAL do ano {ano_cotacao} das seguintes moedas, "
                    f"todas convertidas para USD (dólar americano): {moedas_lista}.\n"
                    f"\n"
                    f"REGRAS OBRIGATÓRIAS:\n"
                    f"1. Use SOMENTE dados históricos reais e verificáveis do ano {ano_cotacao}.\n"
                    f"2. NÃO invente, estime ou suponha valores. Se não tiver certeza de um valor real. "
                    f"3. 'PESOS' deve ser tratado como PESOS ARGENTINOS (ARS).\n"
                    f"4. 'YENS' = JPY, 'YUAN' = CNY, 'REAL' = BRL, 'RAND' = ZAR, "
                    f"'LIBRA ESTERLINA' = GBP, 'FRANCOS SUIZOS' = CHF, "
                    f"'CORONAS SUECAS' = SEK, 'CORONAS DANESAS' = DKK, "
                    f"'CORONAS NORUEGA' = NOK, 'DOLAR AUSTRALIA' = AUD, "
                    f"'DOLAR CANADIENS' = CAD, 'DOLAR NEOZELAND' = NZD, 'EURO' = EUR.\n"
                    f"5. Os valores devem representar quanto 1 unidade da moeda vale em USD.\n"
                    f"6. Retorne APENAS um JSON válido, sem texto adicional, sem markdown, "
                    f"sem explicações. Formato exato: {{\"NOME_MOEDA\": valor_float, ...}}\n"
                    f"7. Use os nomes das moedas EXATAMENTE como fornecidos na lista acima."
                )

                resposta_texto = None
                max_tentativas = 6
                for tentativa in range(max_tentativas):
                    try:
                        response = client.models.generate_content(
                            model="gemini-2.0-flash", contents=prompt)
                        resposta_texto = response.text.strip()
                        break
                    except Exception as retry_err:
                        if "429" in str(retry_err) and tentativa < max_tentativas - 1:
                            wait = min((tentativa + 1) * 15, 90)
                            log_callback(f"  ⏳ Rate limit ({tentativa+1}/{max_tentativas}), aguardando {wait}s...")
                            time.sleep(wait)
                        elif tentativa < max_tentativas - 1:
                            wait = (tentativa + 1) * 5
                            log_callback(f"  ⚠️ Erro ({tentativa+1}/{max_tentativas}): {retry_err}")
                            log_callback(f"  ⏳ Tentando novamente em {wait}s...")
                            time.sleep(wait)
                        else:
                            log_callback(f"  ⚠️ Falha final: {retry_err}")

                if resposta_texto:
                    resposta_texto = re.sub(r"```json\s*", "", resposta_texto)
                    resposta_texto = re.sub(r"```\s*", "", resposta_texto)
                    cotacoes_gemini = json.loads(resposta_texto)

                    for moeda in moedas_outras:
                        if moeda in cotacoes_gemini:
                            taxas_cambio[moeda] = float(cotacoes_gemini[moeda])
                        else:
                            for chave, valor in cotacoes_gemini.items():
                                if chave.upper() in moeda.upper() or moeda.upper() in chave.upper():
                                    taxas_cambio[moeda] = float(valor)
                                    break

                    ia_sucesso = True
                    log_callback("\n📈 Cotações obtidas via IA:")
                    for moeda, taxa in taxas_cambio.items():
                        log_callback(f"    1 {moeda} = {taxa} USD")

            except Exception as e:
                log_callback(f"\n⚠️ Erro na consulta IA: {e}")

            # Preencher moedas faltantes com fallback
            moedas_sem = [m for m in moedas_outras if m not in taxas_cambio]
            if moedas_sem:
                if not ia_sucesso:
                    log_callback("\n⚠️ IA indisponível — usando cotações de referência.")
                    log_callback("   Você poderá editar os valores na tela de revisão.")
                else:
                    log_callback(f"\n⚠️ Moedas sem cotação da IA: {moedas_sem}")

                for moeda in moedas_sem:
                    fallback_val = FALLBACK_COTACOES.get(moeda, 0.0)
                    taxas_cambio[moeda] = fallback_val
                    src = "referência" if fallback_val > 0 else "não encontrada"
                    log_callback(f"    {moeda} = {fallback_val} USD ({src})")

        # Chamar callback para revisão de cotações na GUI
        cotacoes_callback(taxas_cambio, moedas_outras, df, output_dir, formato)

    except Exception as e:
        log_callback(f"❌ ERRO: {e}")
        done_callback(False)


def finalizar_argentina(df, taxas_cambio, output_dir, formato, base_name,
                        log_callback, done_callback):
    """Aplica cotações finais VETORIZADAS e exporta resultado da Argentina."""
    try:
        log_callback("\n💵 Calculando FOB em USD (vetorizado)...")
        t0 = time.perf_counter()

        moeda_col = df["Moeda"].astype(str).str.strip()
        fob_moeda = pd.to_numeric(df["FOB Moeda"], errors="coerce")
        usd_fob = pd.to_numeric(df["U$S FOB"], errors="coerce")

        # Inicializa com NaN
        fob_result = pd.Series(np.nan, index=df.index)

        # Regra 1: Moeda == "DOLAR USA" → usa U$S FOB (se > 0), senão FOB Moeda
        mask_usd = moeda_col == "DOLAR USA"
        fob_result[mask_usd] = np.where(
            usd_fob[mask_usd].fillna(0) > 0,
            usd_fob[mask_usd],
            fob_moeda[mask_usd]
        )

        # Regra 2: Outras moedas → converte com taxa (vetorizado via .map)
        mask_invalido = moeda_col.isin(["`", "", "nan"])
        taxa_series = moeda_col.map(taxas_cambio)
        mask_convert = taxa_series.notna() & ~mask_usd & ~mask_invalido
        fob_result[mask_convert] = (fob_moeda[mask_convert] * taxa_series[mask_convert]).round(2)

        # Regra 3: Backtick ou NaN → já são NaN por padrão (inicialização)

        df["FOB DOLAR"] = fob_result
        log_callback(f"  ✓ {df['FOB DOLAR'].notna().sum():,} valores FOB calculados ({time.perf_counter()-t0:.2f}s)")

        # --- DEBUG Argentina: estado das colunas antes do export ---
        log_callback("\n🔍 DEBUG — Colunas finais Argentina:")
        log_callback(f"  Total: {len(df.columns)} colunas, {len(df):,} linhas")
        notna_counts = df.notna().sum()
        for i, col in enumerate(df.columns):
            letter = ''
            idx = i
            while idx >= 0:
                letter = chr(idx % 26 + ord('A')) + letter
                idx = idx // 26 - 1
            nn = int(notna_counts[col])
            dt = df[col].dtype
            fvi = df[col].first_valid_index()
            sample = df[col].loc[fvi] if fvi is not None else 'VAZIO'
            log_callback(f"  Col {letter} ({i}): [{dt}] '{col}' → {nn:,} vals | ex: {repr(sample)[:50]}")
        log_callback("  --- FIM DEBUG ---")

        exportar_resultado(df, output_dir, base_name + "_tratado", formato, log_callback)
        done_callback(True)
    except Exception as e:
        log_callback(f"❌ ERRO: {e}")
        done_callback(False)

def exportar_resultado(df, output_dir, base_name, formato, log_callback):
    """Exporta DataFrame nos formatos selecionados."""
    if formato in ("xlsx", "ambos"):
        xlsx_path = os.path.join(output_dir, base_name + ".xlsx")
        log_callback(f"\n📝 Salvando {os.path.basename(xlsx_path)}...")
        t0 = time.perf_counter()

        try:
            import polars as pl
            # Converter pandas → polars (lida nativamente com todos os tipos)
            df_pl = pl.from_pandas(df)

            # DEBUG: comparar pandas vs polars para últimas colunas
            log_callback("  🔍 DEBUG Polars — últimas 8 colunas:")
            for col in df.columns[-8:]:
                pd_nn = int(df[col].notna().sum())
                pl_nn = int(df_pl[col].null_count())
                pl_dtype = df_pl[col].dtype
                pl_sample = df_pl[col].drop_nulls()[0] if (len(df_pl) - pl_nn) > 0 else 'VAZIO'
                log_callback(f"    '{col}': pandas={pd_nn:,} não-nulos | polars={len(df_pl)-pl_nn:,} não-nulos (dtype={pl_dtype}) | ex: {repr(pl_sample)[:50]}")

            df_pl.write_excel(xlsx_path)
            log_callback(f"  ✓ XLSX salvo via Polars ({time.perf_counter()-t0:.1f}s): {xlsx_path}")

            # DEBUG: reler e verificar
            try:
                df_check = pd.read_excel(xlsx_path, engine='calamine', nrows=5)
                log_callback("  🔎 Verificação pós-save (5 linhas):")
                for col in df_check.columns[-8:]:
                    nn = int(df_check[col].notna().sum())
                    log_callback(f"    '{col}': {nn}/5 preenchidas")
            except Exception as ve:
                log_callback(f"  ⚠️ Verificação falhou: {ve}")
        except PermissionError:
            log_callback(f"  ⚠️ Arquivo aberto em outro programa! Feche no Excel e tente novamente.")
            raise
        except Exception as e:
            log_callback(f"  ⚠️ Polars falhou ({e}), tentando openpyxl...")
            t0 = time.perf_counter()
            df.to_excel(xlsx_path, index=False, engine="openpyxl")
            log_callback(f"  ✓ XLSX salvo via openpyxl ({time.perf_counter()-t0:.1f}s): {xlsx_path}")

    if formato in ("csv", "ambos"):
        csv_path = os.path.join(output_dir, base_name + ".csv")
        log_callback(f"📝 Salvando {os.path.basename(csv_path)}...")
        t0 = time.perf_counter()
        df.to_csv(csv_path, index=False, sep="|", encoding="utf-8-sig")
        log_callback(f"  ✓ CSV salvo ({time.perf_counter()-t0:.1f}s): {csv_path}")


# ============================================================
# PREPARAÇÃO BANCO DE DADOS
# ============================================================

# Mapeamento de colunas: (coluna_destino, [nomes_possíveis_na_origem])
_DB_COLUMN_MAP = [
    ("NUMERO_DE_FORMULARIO",       ["identificador"]),
    ("RAZON_SOCIAL_IMPORTADOR",    ["importador"]),
    ("CODIGO_LUGAR_INGRESO_MERCA", ["país de origem", "pais de origem"]),
    ("SUBPARTIDA_ARANCELARIA",     ["nandina", "ncm-sim", "ncm_sim", "ncm sim"]),
    ("CANTIDAD_DCMS",              ["cantidad"]),
    ("VALOR_FOB_USD",              ["usd fob", "fob dolar", "fob dólar"]),
    ("DESCRIPCION_MERCANCIA",      ["descrição comercial", "descricao comercial",
                                    "descripcion arancelaria", "descripción arancelaria"]),
    ("FECHA_LEVANTE",              ["data"]),
    ("PARTNUMBERS",                ["partnumber", "partnumbers", "part number", "part_number"]),
    ("MARCA",                      ["marca"]),
]

# Colunas que ficam vazias
_DB_EMPTY_COLS = ["STATUS", "AVG", "DBL_MARKET", "DBL_SEGMENT", "COUNTRY"]

# Ordem final das colunas no arquivo de saída
_DB_OUTPUT_COLUMNS = [
    "NUMERO_DE_FORMULARIO", "RAZON_SOCIAL_IMPORTADOR", "CODIGO_LUGAR_INGRESO_MERCA",
    "SUBPARTIDA_ARANCELARIA", "CANTIDAD_DCMS", "VALOR_FOB_USD", "DESCRIPCION_MERCANCIA",
    "FECHA_LEVANTE", "PARTNUMBERS", "MARCA", "STATUS", "IMPORTADORES",
    "AVG", "DBL_MARKET", "DBL_SEGMENT", "COUNTRY", "CANTIDAD", "VALOR_FOB_USD_2",
]


def _find_column(df_columns_upper, candidates):
    """Encontra a primeira coluna que corresponde aos candidatos (case-insensitive)."""
    for cand in candidates:
        cand_upper = cand.upper().strip()
        for orig, upper in df_columns_upper:
            if upper == cand_upper:
                return orig
    return None


def processar_banco_dados(input_path, output_dir, formato, linhas_por_arquivo,
                          log_callback, done_callback):
    """Prepara arquivo para o banco de dados mapeando colunas para o formato padrão."""
    try:
        log_callback("📂 Carregando arquivo...")
        t0 = time.perf_counter()
        df = ler_arquivo(input_path, dtype=str)
        log_callback(f"  ✓ {len(df):,} linhas × {len(df.columns)} colunas ({time.perf_counter()-t0:.1f}s)")

        # Mapa de colunas (original, UPPER) para busca case-insensitive
        cols_upper = [(c, c.upper().strip()) for c in df.columns]

        log_callback("\n🔄 Mapeando colunas...")
        resultado = pd.DataFrame(index=df.index)
        mapeamentos_ok = 0

        for destino, candidatos in _DB_COLUMN_MAP:
            col_encontrada = _find_column(cols_upper, candidatos)
            if col_encontrada is not None:
                resultado[destino] = df[col_encontrada].values
                log_callback(f"  ✓ '{col_encontrada}' → {destino}")
                mapeamentos_ok += 1
            else:
                resultado[destino] = ""
                nomes = ", ".join(candidatos)
                log_callback(f"  ⚠️ {destino} — nenhuma coluna encontrada ({nomes})")

        # Colunas vazias
        for col_vazia in _DB_EMPTY_COLS:
            resultado[col_vazia] = ""

        # Colunas repetidas
        resultado["IMPORTADORES"] = resultado["RAZON_SOCIAL_IMPORTADOR"].values
        resultado["CANTIDAD"] = resultado["CANTIDAD_DCMS"].values
        resultado["VALOR_FOB_USD_2"] = resultado["VALOR_FOB_USD"].values
        log_callback("\n📋 Colunas repetidas:")
        log_callback("  ✓ RAZON_SOCIAL_IMPORTADOR → IMPORTADORES")
        log_callback("  ✓ CANTIDAD_DCMS → CANTIDAD")
        log_callback("  ✓ VALOR_FOB_USD → VALOR_FOB_USD_2")

        # Reordenar colunas
        resultado = resultado[_DB_OUTPUT_COLUMNS]

        log_callback(f"\n📊 Resumo: {mapeamentos_ok}/{len(_DB_COLUMN_MAP)} colunas mapeadas")
        log_callback(f"  Total de linhas: {len(resultado):,}")
        log_callback(f"  Total de colunas: {len(resultado.columns)}")

        # Divisão em partes
        total_linhas = len(resultado)
        base_name = os.path.splitext(os.path.basename(input_path))[0] + "_banco"

        if linhas_por_arquivo > 0 and total_linhas > linhas_por_arquivo:
            n_partes = (total_linhas + linhas_por_arquivo - 1) // linhas_por_arquivo
            log_callback(f"\n✂️ Dividindo em {n_partes} parte(s) de até {linhas_por_arquivo:,} linhas...")

            for i in range(n_partes):
                inicio = i * linhas_por_arquivo
                fim = min((i + 1) * linhas_por_arquivo, total_linhas)
                parte_df = resultado.iloc[inicio:fim]
                parte_name = f"{base_name}_parte{i+1}"
                log_callback(f"\n📦 Parte {i+1}/{n_partes}: linhas {inicio+1:,} a {fim:,} ({len(parte_df):,} linhas)")
                _exportar_banco(parte_df, output_dir, parte_name, formato, log_callback)
        else:
            _exportar_banco(resultado, output_dir, base_name, formato, log_callback)

        done_callback(True)
    except Exception as e:
        log_callback(f"❌ ERRO: {e}")
        done_callback(False)


def _exportar_banco(df, output_dir, base_name, formato, log_callback):
    """Exporta DataFrame do banco de dados nos formatos selecionados."""
    if formato in ("xlsx", "ambos"):
        xlsx_path = os.path.join(output_dir, base_name + ".xlsx")
        log_callback(f"📝 Salvando {os.path.basename(xlsx_path)}...")
        t0 = time.perf_counter()
        try:
            import polars as pl
            df_pl = pl.from_pandas(df)
            df_pl.write_excel(xlsx_path)
            log_callback(f"  ✓ XLSX salvo via Polars ({time.perf_counter()-t0:.1f}s)")
        except PermissionError:
            log_callback("  ⚠️ Arquivo aberto em outro programa! Feche e tente novamente.")
            raise
        except Exception:
            t0 = time.perf_counter()
            df.to_excel(xlsx_path, index=False, engine="openpyxl")
            log_callback(f"  ✓ XLSX salvo via openpyxl ({time.perf_counter()-t0:.1f}s)")

    if formato in ("csv", "ambos"):
        csv_path = os.path.join(output_dir, base_name + ".csv")
        log_callback(f"📝 Salvando {os.path.basename(csv_path)}...")
        t0 = time.perf_counter()
        df.to_csv(csv_path, index=False, sep="|", encoding="utf-8-sig")
        log_callback(f"  ✓ CSV salvo ({time.perf_counter()-t0:.1f}s)")


# ============================================================
# NORMALIZAÇÃO
# ============================================================

def processar_normalizacao(data_path, regras, output_dir, formato, log_callback, done_callback):
    """Processa normalização de MARCA e PARTNUMBER usando Aho-Corasick."""
    try:
        import ahocorasick

        log_callback("📂 Carregando arquivo de dados...")
        t0 = time.perf_counter()
        df = ler_arquivo(data_path, dtype=str)
        log_callback(f"  ✓ {len(df):,} linhas × {len(df.columns)} colunas ({time.perf_counter()-t0:.1f}s)")

        col_marca = None
        col_pn = None
        for col in df.columns:
            upper = col.upper().strip()
            if upper == "MARCA":
                col_marca = col
            elif upper in ("PARTNUMBER", "PART NUMBER", "PART_NUMBER", "PN"):
                col_pn = col

        if col_marca is None:
            log_callback("❌ Coluna MARCA não encontrada no arquivo de dados!")
            done_callback(False)
            return
        if col_pn is None:
            log_callback("❌ Coluna PARTNUMBER não encontrada no arquivo de dados!")
            done_callback(False)
            return

        log_callback(f"  Coluna MARCA: '{col_marca}'")
        log_callback(f"  Coluna PARTNUMBER: '{col_pn}'")

        # Limpeza em 1 passo por coluna (fillna+upper+strip+NAN→'') fundidos
        for _c in (col_marca, col_pn):
            s = df[_c].fillna("").astype(str).str.upper().str.strip()
            s[s == "NAN"] = ""
            df[_c] = s

        log_callback(f"\n🔄 Aplicando {len(regras)} regra(s) de normalização...")

        pn_rules = [(r["marca"].upper().strip(), r["partnumber"].upper().strip())
                     for r in regras if r.get("partnumber", "").strip()]
        marca_rules = [r["marca"].upper().strip()
                       for r in regras if not r.get("partnumber", "").strip()]

        total_changes = 0

        # --- PARTNUMBER: lookup via dict O(1) por linha + value_counts O(n) ---
        if pn_rules:
            log_callback(f"\n🔧 Aplicando {len(pn_rules)} regra(s) de PARTNUMBER...")
            t0 = time.perf_counter()
            pn_map = {pn: marca for marca, pn in pn_rules}
            nova_marca = df[col_pn].map(pn_map)
            mask = nova_marca.notna()
            changes = int(mask.sum())
            if changes > 0:
                df.loc[mask, col_marca] = nova_marca[mask]
                total_changes += changes
            log_callback(f"  ✓ {changes:,} linha(s) atualizadas via PN ({time.perf_counter()-t0:.2f}s)")
            # Detalhe por regra — O(n) total via value_counts (não O(n×m))
            pn_counts = df.loc[df[col_pn].isin(pn_map), col_pn].value_counts()
            for marca_regra, pn_regra in pn_rules:
                c = int(pn_counts.get(pn_regra, 0))
                if c > 0:
                    log_callback(f"    PN '{pn_regra}' → '{marca_regra}': {c:,}")
                else:
                    log_callback(f"    ⚠ PN '{pn_regra}' não encontrado")

        # --- MARCA: Aho-Corasick multi-pattern matching O(n) ---
        if marca_rules:
            log_callback(f"\n🏷️ Aplicando {len(marca_rules)} regra(s) de MARCA (Aho-Corasick)...")
            t0 = time.perf_counter()

            # Construir autômato Aho-Corasick
            A = ahocorasick.Automaton()
            for idx, marca_regra in enumerate(marca_rules):
                A.add_word(marca_regra, (idx, marca_regra))
            A.make_automaton()

            # Deduplica: corre Aho-Corasick só nos valores ÚNICOS → O(u) vs O(n)
            marcas_series = df[col_marca]
            uniq = marcas_series.unique()  # u valores únicos, u ≪ n
            log_callback(f"  ⚡ Autômato: {len(marca_rules)} padrões × {len(uniq):,} valores únicos (de {len(df):,} linhas)")

            remap = {}  # valor_original → valor_normalizado
            changes_per_rule = {m: 0 for m in marca_rules}

            for valor in uniq:
                if not valor:
                    continue
                best_match = None
                best_len = 0
                for _, (_, matched_marca) in A.iter(valor):
                    if valor == matched_marca:
                        best_match = None
                        break
                    if len(matched_marca) > best_len:
                        best_match = matched_marca
                        best_len = len(matched_marca)
                if best_match is not None:
                    remap[valor] = best_match

            # Aplica remap vetorizado — O(n) via .map()
            if remap:
                mapped = marcas_series.map(remap)
                mask = mapped.notna()
                df.loc[mask, col_marca] = mapped[mask]
                # Contagem por regra — O(n) via value_counts
                vc = mapped.dropna().value_counts()
                for regra_val, cnt in vc.items():
                    changes_per_rule[regra_val] = int(cnt)
                    total_changes += int(cnt)

            elapsed = time.perf_counter() - t0
            log_callback(f"  ✓ Matching concluído ({elapsed:.2f}s)")

            for marca_regra in marca_rules:
                c = changes_per_rule[marca_regra]
                if c > 0:
                    log_callback(f"    '{marca_regra}': {c:,} linha(s) normalizada(s)")
                else:
                    log_callback(f"    ⚠ '{marca_regra}': nenhum match (ou já normalizada)")

        log_callback(f"\n📊 Total de alterações: {total_changes:,}")

        base_name = os.path.splitext(os.path.basename(data_path))[0]
        exportar_resultado(df, output_dir, base_name + "_normalizado", formato, log_callback)
        done_callback(True)
    except Exception as e:
        log_callback(f"❌ ERRO: {e}")
        done_callback(False)


# ============================================================
# CLASSE PRINCIPAL DA GUI
# ============================================================

class TkinterDnDCustom(TkinterDnD.DnDWrapper, ctk.CTk):
    """Combina CustomTkinter com drag-and-drop."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.TkdndVersion = TkinterDnD._require(self)


class App:
    def __init__(self):
        self.root = TkinterDnDCustom()
        self.root.title("Tratamento de Dados — Automata Custom")
        self.root.geometry("960x720")
        self.root.minsize(800, 600)
        self.root.configure(fg_color=COLORS["background"])

        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

        # Estado
        self.input_file = None
        self.pais_selecionado = ctk.StringVar(value="equador")
        self.formato_saida = ctk.StringVar(value="ambos")
        self.api_key_var = ctk.StringVar(value="AIzaSyCUQl6taCkO9mDyipd7XAHFm2j3EcUsJKQ")
        self.api_key_visible = False
        self.ano_cotacao = ctk.StringVar(value=str(time.localtime().tm_year - 1))
        self.output_dir_var = ctk.StringVar(value="")
        self.processando = False

        # Dados temporários Argentina (para revisão de cotações)
        self._arg_df = None
        self._arg_taxas = {}
        self._arg_moedas = []
        self._arg_output_dir = ""
        self._arg_formato = ""
        self._cotacao_entries = {}

        # Estado Normalização
        self.norm_input_file = None
        self.norm_rules_file = None
        self.norm_method = ctk.StringVar(value="excel")
        self.norm_formato = ctk.StringVar(value="xlsx")
        self.norm_output_dir = ctk.StringVar(value="")
        self.norm_regras_manual = []
        self.norm_processando = False

        # Estado Banco de Dados
        self.db_input_file = None
        self.db_formato = ctk.StringVar(value="ambos")
        self.db_output_dir = ctk.StringVar(value="")
        self.db_linhas_var = ctk.StringVar(value="50000")
        self.db_processando = False

        self._build_ui()

    # --------------------------------------------------------
    # CONSTRUÇÃO DA INTERFACE
    # --------------------------------------------------------
    def _build_ui(self):
        main = ctk.CTkFrame(self.root, fg_color=COLORS["background"])
        main.pack(fill="both", expand=True, padx=20, pady=15)

        # --- HEADER (Logo + Título) ---
        header = ctk.CTkFrame(main, fg_color=COLORS["surface"], corner_radius=12,
                              border_width=1, border_color=COLORS["border"])
        header.pack(fill="x", pady=(0, 12))

        header_inner = ctk.CTkFrame(header, fg_color="transparent")
        header_inner.pack(fill="x", padx=20, pady=15)

        # Logo
        self.logo_label = None
        self._logo_image = None
        self._build_logo(header_inner)

        title_frame = ctk.CTkFrame(header_inner, fg_color="transparent")
        title_frame.pack(side="left", fill="x", expand=True)

        ctk.CTkLabel(title_frame, text="Tratamento de Dados",
                     font=ctk.CTkFont(size=22, weight="bold"),
                     text_color=COLORS["text"]).pack(anchor="w")
        ctk.CTkLabel(title_frame, text="Automata Custom",
                     font=ctk.CTkFont(size=13),
                     text_color=COLORS["text_dim"]).pack(anchor="w")

        # --- TABVIEW ---
        self.tabview = ctk.CTkTabview(
            main, fg_color=COLORS["background"],
            segmented_button_fg_color=COLORS["surface"],
            segmented_button_selected_color=COLORS["secondary"],
            segmented_button_unselected_color=COLORS["card"],
            segmented_button_selected_hover_color=COLORS["primary"],
            segmented_button_unselected_hover_color=COLORS["card_hover"],
            text_color=COLORS["text"],
            corner_radius=12,
        )
        self.tabview.pack(fill="both", expand=True)
        self.tabview.add("Tratamento")
        self.tabview.add("Normalização")
        self.tabview.add("Banco de Dados")

        # --- CORPO TRATAMENTO (2 colunas) ---
        body = self.tabview.tab("Tratamento")
        body.grid_columnconfigure(0, weight=1)
        body.grid_columnconfigure(1, weight=2)
        body.grid_rowconfigure(0, weight=1)

        # --- PAINEL ESQUERDO ---
        left = ctk.CTkFrame(body, fg_color=COLORS["surface"], corner_radius=12,
                            border_width=1, border_color=COLORS["border"])
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 8))

        left_inner = ctk.CTkFrame(left, fg_color="transparent")
        left_inner.pack(fill="both", expand=True, padx=16, pady=16)

        # País
        ctk.CTkLabel(left_inner, text="🌍  PAÍS",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", pady=(0, 8))

        for val, label in [("equador", "🇪🇨  Equador"), ("argentina", "🇦🇷  Argentina")]:
            ctk.CTkRadioButton(
                left_inner, text=label, variable=self.pais_selecionado,
                value=val, font=ctk.CTkFont(size=13),
                fg_color=COLORS["secondary"], hover_color=COLORS["primary"],
                border_color=COLORS["border"], text_color=COLORS["text"],
                command=self._on_pais_change
            ).pack(anchor="w", pady=3, padx=8)

        # API Key (só Argentina)
        self.api_frame = ctk.CTkFrame(left_inner, fg_color="transparent")
        self.api_frame.pack(fill="x", pady=(12, 0))

        ctk.CTkLabel(self.api_frame, text="🔑  API Key Gemini",
                     font=ctk.CTkFont(size=13, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", pady=(0, 4))

        # Frame da API Key com entry + botão olhinho
        api_entry_frame = ctk.CTkFrame(self.api_frame, fg_color="transparent")
        api_entry_frame.pack(fill="x")

        self.api_entry = ctk.CTkEntry(api_entry_frame, textvariable=self.api_key_var,
                                       fg_color=COLORS["card"], border_color=COLORS["border"],
                                       text_color=COLORS["text"], font=ctk.CTkFont(size=11),
                                       show="•")
        self.api_entry.pack(side="left", fill="x", expand=True, padx=(0, 4))

        self.eye_btn = ctk.CTkButton(
            api_entry_frame, text="👁", width=36, height=28,
            font=ctk.CTkFont(size=14),
            fg_color=COLORS["card"], hover_color=COLORS["card_hover"],
            border_width=1, border_color=COLORS["border"],
            corner_radius=6, command=self._toggle_api_key_visibility
        )
        self.eye_btn.pack(side="right")

        # Seletor de ano para cotações
        ano_row = ctk.CTkFrame(self.api_frame, fg_color="transparent")
        ano_row.pack(fill="x", pady=(8, 0))

        ctk.CTkLabel(ano_row, text="📅  Ano de referência",
                     font=ctk.CTkFont(size=12), text_color=COLORS["text_dim"]
                     ).pack(side="left", padx=(0, 8))

        anos_disponiveis = [str(a) for a in range(2015, time.localtime().tm_year + 1)][::-1]
        self.ano_menu = ctk.CTkOptionMenu(
            ano_row,
            values=anos_disponiveis,
            variable=self.ano_cotacao,
            fg_color=COLORS["card"],
            button_color=COLORS["primary"],
            button_hover_color=COLORS["secondary"],
            text_color=COLORS["text"],
            font=ctk.CTkFont(size=12),
            width=90,
        )
        self.ano_menu.pack(side="left")

        self.api_frame.pack_forget()  # Oculto no início (Equador)

        # Separador
        ctk.CTkFrame(left_inner, fg_color=COLORS["border"], height=1).pack(fill="x", pady=16)

        # Formato de saída
        ctk.CTkLabel(left_inner, text="📄  FORMATO DE SAÍDA",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", pady=(0, 8))

        for val, label in [("xlsx", "XLSX (Excel)"), ("csv", "CSV (separado por |)"),
                           ("ambos", "Ambos")]:
            ctk.CTkRadioButton(
                left_inner, text=label, variable=self.formato_saida,
                value=val, font=ctk.CTkFont(size=13),
                fg_color=COLORS["secondary"], hover_color=COLORS["primary"],
                border_color=COLORS["border"], text_color=COLORS["text"]
            ).pack(anchor="w", pady=3, padx=8)

        # Separador
        ctk.CTkFrame(left_inner, fg_color=COLORS["border"], height=1).pack(fill="x", pady=16)

        # Pasta de saída
        ctk.CTkLabel(left_inner, text="💾  PASTA DE SAÍDA",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", pady=(0, 6))

        out_row = ctk.CTkFrame(left_inner, fg_color="transparent")
        out_row.pack(fill="x", pady=(0, 4))

        self.out_dir_entry = ctk.CTkEntry(
            out_row, textvariable=self.output_dir_var,
            placeholder_text="Mesma pasta do arquivo de entrada",
            fg_color=COLORS["card"], border_color=COLORS["border"],
            text_color=COLORS["text"], font=ctk.CTkFont(size=11),
        )
        self.out_dir_entry.pack(side="left", fill="x", expand=True, padx=(0, 4))

        ctk.CTkButton(
            out_row, text="📂", width=36, height=28,
            font=ctk.CTkFont(size=14),
            fg_color=COLORS["card"], hover_color=COLORS["card_hover"],
            border_width=1, border_color=COLORS["border"],
            corner_radius=6, command=self._on_browse_output
        ).pack(side="right")

        # Separador
        ctk.CTkFrame(left_inner, fg_color=COLORS["border"], height=1).pack(fill="x", pady=16)

        # Zona de Drop
        ctk.CTkLabel(left_inner, text="📁  ARQUIVO DE ENTRADA",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", pady=(0, 8))

        self.drop_zone = ctk.CTkFrame(left_inner, fg_color=COLORS["card"],
                                       corner_radius=10, height=100,
                                       border_width=2, border_color=COLORS["border"])
        self.drop_zone.pack(fill="x", pady=(0, 8))
        self.drop_zone.pack_propagate(False)

        self.drop_label = ctk.CTkLabel(
            self.drop_zone,
            text="Arraste o arquivo .xlsx / .csv aqui\nou clique para selecionar",
            font=ctk.CTkFont(size=12), text_color=COLORS["text_dim"],
            justify="center"
        )
        self.drop_label.pack(expand=True)

        # Drag & Drop
        self.drop_zone.drop_target_register(DND_FILES)
        self.drop_zone.dnd_bind("<<Drop>>", self._on_drop)
        self.drop_label.drop_target_register(DND_FILES)
        self.drop_label.dnd_bind("<<Drop>>", self._on_drop)

        # Click to browse
        self.drop_zone.bind("<Button-1>", self._on_browse)
        self.drop_label.bind("<Button-1>", self._on_browse)

        # Arquivo selecionado
        self.file_label = ctk.CTkLabel(left_inner, text="", font=ctk.CTkFont(size=11),
                                        text_color=COLORS["success"], wraplength=250)
        self.file_label.pack(anchor="w")

        # Botão processar
        self.btn_processar = ctk.CTkButton(
            left_inner, text="▶  PROCESSAR", font=ctk.CTkFont(size=14, weight="bold"),
            fg_color=COLORS["secondary"], hover_color=COLORS["primary"],
            corner_radius=8, height=44, command=self._on_processar
        )
        self.btn_processar.pack(fill="x", pady=(12, 0))

        # --- PAINEL DIREITO (Log + Cotações) ---
        right = ctk.CTkFrame(body, fg_color=COLORS["surface"], corner_radius=12,
                             border_width=1, border_color=COLORS["border"])
        right.grid(row=0, column=1, sticky="nsew", padx=(8, 0))

        right_inner = ctk.CTkFrame(right, fg_color="transparent")
        right_inner.pack(fill="both", expand=True, padx=16, pady=16)

        ctk.CTkLabel(right_inner, text="📋  LOG DE PROCESSAMENTO",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", pady=(0, 8))

        self.log_text = ctk.CTkTextbox(right_inner, fg_color=COLORS["card"],
                                        text_color=COLORS["text"],
                                        font=ctk.CTkFont(family="Consolas", size=12),
                                        corner_radius=8, border_width=1,
                                        border_color=COLORS["border"],
                                        state="disabled", wrap="word")
        self.log_text.pack(fill="both", expand=True)

        # Frame de cotações
        self.cotacoes_frame = ctk.CTkFrame(right_inner, fg_color=COLORS["card"],
                                            corner_radius=8, border_width=1,
                                            border_color=COLORS["border"])

        # Barra de status
        status_bar = ctk.CTkFrame(main, fg_color=COLORS["surface"], corner_radius=8,
                                   height=36, border_width=1, border_color=COLORS["border"])
        status_bar.pack(fill="x", pady=(12, 0))
        status_bar.pack_propagate(False)

        self.status_label = ctk.CTkLabel(status_bar, text="Pronto",
                                          font=ctk.CTkFont(size=12),
                                          text_color=COLORS["text_dim"])
        self.status_label.pack(side="left", padx=12)

        self.progress = ctk.CTkProgressBar(status_bar, fg_color=COLORS["card"],
                                            progress_color=COLORS["secondary"],
                                            height=6, corner_radius=3)
        self.progress.pack(side="right", padx=12, pady=10, fill="x", expand=True)
        self.progress.set(0)

        # --- TAB NORMALIZAÇÃO ---
        self._build_normalizacao_tab()

        # --- TAB BANCO DE DADOS ---
        self._build_banco_dados_tab()

    # --------------------------------------------------------
    # LOGO
    # --------------------------------------------------------
    def _build_logo(self, parent):
        """Carrega a logo da empresa no header (embutida ou de arquivo)."""
        pil_img = None

        # 1) Tentar logo embutida (funciona dentro do .exe sem arquivo externo)
        try:
            from _logo_data import LOGO_BASE64
            raw = base64.b64decode(LOGO_BASE64)
            pil_img = Image.open(io.BytesIO(raw)).convert("RGBA")
        except Exception:
            pass

        # 2) Fallback: arquivo no disco
        if pil_img is None:
            logo_candidates = [
                "LogoOficial_Branco.png",
                "logo.png", "logo.jpg", "logo.ico",
            ]
            base_dir = self._get_base_dir()
            for name in logo_candidates:
                path = os.path.join(base_dir, name)
                if os.path.exists(path):
                    try:
                        pil_img = Image.open(path).convert("RGBA")
                        break
                    except Exception:
                        pass

        if pil_img is not None:
            max_h = 48
            ratio = max_h / pil_img.height
            new_w = int(pil_img.width * ratio)
            new_h = max_h

            self._logo_image = ctk.CTkImage(
                light_image=pil_img, dark_image=pil_img,
                size=(new_w, new_h)
            )
            self.logo_label = ctk.CTkLabel(
                parent, image=self._logo_image, text=""
            )
            self.logo_label.pack(side="left", padx=(0, 15))
            return

        # Fallback: ícone com letra
        fallback = ctk.CTkFrame(parent, fg_color=COLORS["primary"],
                                corner_radius=8, width=50, height=50)
        fallback.pack(side="left", padx=(0, 15))
        fallback.pack_propagate(False)
        ctk.CTkLabel(fallback, text="A",
                     font=ctk.CTkFont(size=24, weight="bold"),
                     text_color="white").pack(expand=True)

    @staticmethod
    def _get_base_dir():
        if getattr(sys, 'frozen', False):
            return os.path.dirname(sys.executable)
        return os.path.dirname(os.path.abspath(__file__))

    # --------------------------------------------------------
    # TOGGLE VISIBILIDADE API KEY (olhinho)
    # --------------------------------------------------------
    def _toggle_api_key_visibility(self):
        self.api_key_visible = not self.api_key_visible
        if self.api_key_visible:
            self.api_entry.configure(show="")
            self.eye_btn.configure(text="🙈")
        else:
            self.api_entry.configure(show="•")
            self.eye_btn.configure(text="👁")

    # --------------------------------------------------------
    # EVENTOS
    # --------------------------------------------------------
    def _on_pais_change(self):
        if self.pais_selecionado.get() == "argentina":
            self.api_frame.pack(fill="x", pady=(12, 0))
        else:
            self.api_frame.pack_forget()

    def _on_drop(self, event):
        path = event.data.strip()
        if path.startswith("{"):
            path = path[1:]
        if path.endswith("}"):
            path = path[:-1]
        path = path.strip('"').strip("'")
        self._set_file(path)

    def _on_browse_output(self):
        path = filedialog.askdirectory(title="Selecionar pasta de saída")
        if path:
            self.output_dir_var.set(path)
            self._log(f"💾 Pasta de saída: {path}")

    def _on_browse(self, _event=None):
        path = filedialog.askopenfilename(
            title="Selecionar arquivo Excel ou CSV",
            filetypes=[("Excel/CSV", "*.xlsx *.xls *.csv"), ("Excel", "*.xlsx *.xls"), ("CSV", "*.csv"), ("Todos", "*.*")]
        )
        if path:
            self._set_file(path)

    def _set_file(self, path):
        if not path.lower().endswith((".xlsx", ".xls", ".csv")):
            messagebox.showwarning("Arquivo inválido", "Selecione um arquivo .xlsx, .xls ou .csv")
            return
        self.input_file = path
        nome = os.path.basename(path)
        self.file_label.configure(text=f"✓ {nome}")
        self.drop_label.configure(text=f"📄 {nome}")
        self.drop_zone.configure(border_color=COLORS["success"])
        self._log(f"📄 Arquivo selecionado: {nome}")

    # --------------------------------------------------------
    # LOG
    # --------------------------------------------------------
    def _log(self, msg):
        def _update():
            self.log_text.configure(state="normal")
            self.log_text.insert("end", msg + "\n")
            self.log_text.see("end")
            self.log_text.configure(state="disabled")
        self.root.after(0, _update)

    def _clear_log(self):
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.configure(state="disabled")

    # --------------------------------------------------------
    # STATUS
    # --------------------------------------------------------
    def _set_status(self, text, progresso=None):
        def _update():
            self.status_label.configure(text=text)
            if progresso is not None:
                self.progress.set(progresso)
        self.root.after(0, _update)

    def _set_processing(self, active):
        self.processando = active
        state = "disabled" if active else "normal"
        self.root.after(0, lambda: self.btn_processar.configure(state=state))
        if active:
            self.root.after(0, lambda: self.progress.configure(mode="indeterminate"))
            self.root.after(0, lambda: self.progress.start())
        else:
            self.root.after(0, lambda: self.progress.stop())
            self.root.after(0, lambda: self.progress.configure(mode="determinate"))
            self.root.after(0, lambda: self.progress.set(1.0))

    # --------------------------------------------------------
    # PROCESSAMENTO
    # --------------------------------------------------------
    def _on_processar(self):
        if self.processando:
            return
        if not self.input_file or not os.path.exists(self.input_file):
            messagebox.showwarning("Atenção", "Selecione um arquivo válido primeiro.")
            return

        self._clear_log()
        self._hide_cotacoes()
        output_dir = self.output_dir_var.get().strip() or os.path.dirname(self.input_file)
        if not os.path.isdir(output_dir):
            messagebox.showwarning("Pasta inválida", f"A pasta de saída não existe:\n{output_dir}")
            return
        formato = self.formato_saida.get()
        pais = self.pais_selecionado.get()

        self._set_processing(True)
        self._set_status(f"Processando {pais.capitalize()}...")

        if pais == "equador":
            t = threading.Thread(target=processar_equador, daemon=True,
                                 args=(self.input_file, output_dir, formato,
                                       self._log, self._done))
            t.start()
        else:
            api_key = self.api_key_var.get().strip()
            if not api_key:
                messagebox.showwarning("Atenção", "Insira a API Key do Gemini.")
                self._set_processing(False)
                self._set_status("Pronto")
                return
            ano = self.ano_cotacao.get()
            t = threading.Thread(target=processar_argentina, daemon=True,
                                 args=(self.input_file, output_dir, formato, api_key,
                                       ano, self._log, self._done, self._show_cotacoes))
            t.start()

    def _done(self, success):
        self._set_processing(False)
        if success:
            self._set_status("✅ Concluído com sucesso!", 1.0)
            self._log("\n✅ PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
            self.root.after(0, lambda: messagebox.showinfo(
                "Sucesso", "Arquivo processado e salvo com sucesso!"))
        else:
            self._set_status("❌ Erro no processamento", 0)

    # --------------------------------------------------------
    # COTAÇÕES (Argentina)
    # --------------------------------------------------------
    def _show_cotacoes(self, taxas, moedas_outras, df, output_dir, formato):
        self._arg_df = df
        self._arg_taxas = dict(taxas)
        self._arg_moedas = moedas_outras
        self._arg_output_dir = output_dir
        self._arg_formato = formato

        def _build():
            self._set_processing(False)
            self._set_status("Aguardando revisão de cotações...")

            for w in self.cotacoes_frame.winfo_children():
                w.destroy()
            self._cotacao_entries = {}

            self.cotacoes_frame.pack(fill="x", pady=(12, 0))

            ctk.CTkLabel(self.cotacoes_frame, text="💱  REVISÃO DE COTAÇÕES",
                         font=ctk.CTkFont(size=14, weight="bold"),
                         text_color=COLORS["warning"]).pack(anchor="w", padx=12, pady=(12, 4))

            ctk.CTkLabel(self.cotacoes_frame,
                         text="Edite os valores abaixo se necessário e clique em Confirmar.",
                         font=ctk.CTkFont(size=11), text_color=COLORS["text_dim"]
                         ).pack(anchor="w", padx=12, pady=(0, 8))

            for moeda in moedas_outras:
                row = ctk.CTkFrame(self.cotacoes_frame, fg_color="transparent")
                row.pack(fill="x", padx=12, pady=3)

                ctk.CTkLabel(row, text=f"1 {moeda} =",
                             font=ctk.CTkFont(size=12),
                             text_color=COLORS["text"], width=200).pack(side="left")

                entry = ctk.CTkEntry(row, fg_color=COLORS["surface"],
                                      border_color=COLORS["border"],
                                      text_color=COLORS["text"],
                                      font=ctk.CTkFont(size=12), width=140)
                entry.pack(side="left", padx=6)
                valor_atual = taxas.get(moeda, 0.0)
                entry.insert(0, str(valor_atual))
                self._cotacao_entries[moeda] = entry

                ctk.CTkLabel(row, text="USD", font=ctk.CTkFont(size=12),
                             text_color=COLORS["text_dim"]).pack(side="left")

            btn_frame = ctk.CTkFrame(self.cotacoes_frame, fg_color="transparent")
            btn_frame.pack(fill="x", padx=12, pady=(8, 12))

            ctk.CTkButton(btn_frame, text="✓  Confirmar e Gerar Arquivo",
                          font=ctk.CTkFont(size=13, weight="bold"),
                          fg_color=COLORS["success"], hover_color="#4CAF50",
                          corner_radius=8, height=38,
                          command=self._confirmar_cotacoes).pack(fill="x")

        self.root.after(0, _build)

    def _hide_cotacoes(self):
        self.cotacoes_frame.pack_forget()

    def _confirmar_cotacoes(self):
        taxas_finais = {}
        for moeda, entry in self._cotacao_entries.items():
            try:
                val = float(entry.get().strip().replace(",", "."))
                taxas_finais[moeda] = val
            except ValueError:
                messagebox.showwarning("Valor inválido",
                                       f"Valor inválido para {moeda}. Use número decimal.")
                return

        self._log("\n📈 Cotações finais confirmadas:")
        for moeda, taxa in taxas_finais.items():
            self._log(f"    1 {moeda} = {taxa} USD")

        self._hide_cotacoes()
        self._set_processing(True)
        self._set_status("Finalizando Argentina...")

        base_name = os.path.splitext(os.path.basename(self.input_file))[0]
        t = threading.Thread(target=finalizar_argentina, daemon=True,
                             args=(self._arg_df, taxas_finais, self._arg_output_dir,
                                   self._arg_formato, base_name,
                                   self._log, self._done))
        t.start()

    # --------------------------------------------------------
    # NORMALIZAÇÃO — UI
    # --------------------------------------------------------
    def _build_normalizacao_tab(self):
        tab = self.tabview.tab("Normalização")
        tab.grid_columnconfigure(0, weight=1)
        tab.grid_columnconfigure(1, weight=2)
        tab.grid_rowconfigure(0, weight=1)

        # --- PAINEL ESQUERDO (scrollable) ---
        left = ctk.CTkScrollableFrame(tab, fg_color=COLORS["surface"], corner_radius=12,
                                       border_width=1, border_color=COLORS["border"])
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 8))

        # Método de entrada
        ctk.CTkLabel(left, text="📋  MÉTODO DE ENTRADA",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", padx=16, pady=(16, 8))

        for val, label in [("excel", "📂  Via Excel (arquivo de regras)"),
                           ("app", "✏️  Via APP (entrada manual)")]:
            ctk.CTkRadioButton(
                left, text=label, variable=self.norm_method,
                value=val, font=ctk.CTkFont(size=13),
                fg_color=COLORS["secondary"], hover_color=COLORS["primary"],
                border_color=COLORS["border"], text_color=COLORS["text"],
                command=self._on_norm_method_change
            ).pack(anchor="w", pady=3, padx=24)

        ctk.CTkFrame(left, fg_color=COLORS["border"], height=1).pack(fill="x", padx=16, pady=12)

        # Arquivo de dados
        ctk.CTkLabel(left, text="📁  ARQUIVO DE DADOS",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", padx=16, pady=(0, 4))
        ctk.CTkLabel(left, text="Excel ou CSV (sep. |) com colunas MARCA e PARTNUMBER",
                     font=ctk.CTkFont(size=11), text_color=COLORS["text_dim"]
                     ).pack(anchor="w", padx=16, pady=(0, 8))

        self.norm_data_drop = ctk.CTkFrame(left, fg_color=COLORS["card"],
                                            corner_radius=10, height=70,
                                            border_width=2, border_color=COLORS["border"])
        self.norm_data_drop.pack(fill="x", padx=16, pady=(0, 4))
        self.norm_data_drop.pack_propagate(False)

        self.norm_data_label = ctk.CTkLabel(
            self.norm_data_drop, text="Arraste ou clique para selecionar",
            font=ctk.CTkFont(size=11), text_color=COLORS["text_dim"], justify="center"
        )
        self.norm_data_label.pack(expand=True)

        self.norm_data_drop.drop_target_register(DND_FILES)
        self.norm_data_drop.dnd_bind("<<Drop>>", self._on_norm_data_drop)
        self.norm_data_label.drop_target_register(DND_FILES)
        self.norm_data_label.dnd_bind("<<Drop>>", self._on_norm_data_drop)
        self.norm_data_drop.bind("<Button-1>", self._on_norm_data_browse)
        self.norm_data_label.bind("<Button-1>", self._on_norm_data_browse)

        self.norm_data_file_label = ctk.CTkLabel(left, text="", font=ctk.CTkFont(size=11),
                                                  text_color=COLORS["success"], wraplength=250)
        self.norm_data_file_label.pack(anchor="w", padx=16)

        ctk.CTkFrame(left, fg_color=COLORS["border"], height=1).pack(fill="x", padx=16, pady=12)

        # Container para conteúdo de método (mantém posição no layout)
        self.norm_method_container = ctk.CTkFrame(left, fg_color="transparent")
        self.norm_method_container.pack(fill="x")

        # --- Via Excel ---
        self.norm_excel_frame = ctk.CTkFrame(self.norm_method_container, fg_color="transparent")
        self.norm_excel_frame.pack(fill="x")

        ctk.CTkLabel(self.norm_excel_frame, text="📁  ARQUIVO DE REGRAS",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", padx=16, pady=(0, 4))
        ctk.CTkLabel(self.norm_excel_frame, text="Excel com colunas MARCA e PARTNUMBER",
                     font=ctk.CTkFont(size=11), text_color=COLORS["text_dim"]
                     ).pack(anchor="w", padx=16, pady=(0, 8))

        self.norm_rules_drop = ctk.CTkFrame(self.norm_excel_frame, fg_color=COLORS["card"],
                                             corner_radius=10, height=70,
                                             border_width=2, border_color=COLORS["border"])
        self.norm_rules_drop.pack(fill="x", padx=16, pady=(0, 4))
        self.norm_rules_drop.pack_propagate(False)

        self.norm_rules_label = ctk.CTkLabel(
            self.norm_rules_drop, text="Arraste ou clique para selecionar",
            font=ctk.CTkFont(size=11), text_color=COLORS["text_dim"], justify="center"
        )
        self.norm_rules_label.pack(expand=True)

        self.norm_rules_drop.drop_target_register(DND_FILES)
        self.norm_rules_drop.dnd_bind("<<Drop>>", self._on_norm_rules_drop)
        self.norm_rules_label.drop_target_register(DND_FILES)
        self.norm_rules_label.dnd_bind("<<Drop>>", self._on_norm_rules_drop)
        self.norm_rules_drop.bind("<Button-1>", self._on_norm_rules_browse)
        self.norm_rules_label.bind("<Button-1>", self._on_norm_rules_browse)

        self.norm_rules_file_label = ctk.CTkLabel(self.norm_excel_frame, text="",
                                                   font=ctk.CTkFont(size=11),
                                                   text_color=COLORS["success"], wraplength=250)
        self.norm_rules_file_label.pack(anchor="w", padx=16)

        # --- Via APP ---
        self.norm_app_frame = ctk.CTkFrame(self.norm_method_container, fg_color="transparent")
        # Inicialmente oculto

        ctk.CTkLabel(self.norm_app_frame, text="✏️  ENTRADA MANUAL",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", padx=16, pady=(0, 8))

        ctk.CTkLabel(self.norm_app_frame, text="MARCA:",
                     font=ctk.CTkFont(size=12), text_color=COLORS["text"]
                     ).pack(anchor="w", padx=16, pady=(0, 2))
        self.norm_marca_entry = ctk.CTkEntry(
            self.norm_app_frame, placeholder_text="Ex: HELLA",
            fg_color=COLORS["card"], border_color=COLORS["border"],
            text_color=COLORS["text"], font=ctk.CTkFont(size=12)
        )
        self.norm_marca_entry.pack(fill="x", padx=16, pady=(0, 8))

        ctk.CTkLabel(self.norm_app_frame, text="PARTNUMBER (opcional):",
                     font=ctk.CTkFont(size=12), text_color=COLORS["text"]
                     ).pack(anchor="w", padx=16, pady=(0, 2))
        self.norm_pn_entry = ctk.CTkEntry(
            self.norm_app_frame, placeholder_text="Ex: P559000 (vazio = só marca)",
            fg_color=COLORS["card"], border_color=COLORS["border"],
            text_color=COLORS["text"], font=ctk.CTkFont(size=12)
        )
        self.norm_pn_entry.pack(fill="x", padx=16, pady=(0, 8))

        ctk.CTkButton(
            self.norm_app_frame, text="+  Adicionar Regra",
            font=ctk.CTkFont(size=12), fg_color=COLORS["primary"],
            hover_color=COLORS["secondary"], corner_radius=6, height=32,
            command=self._add_norm_rule
        ).pack(fill="x", padx=16, pady=(0, 8))

        self.norm_rules_list_frame = ctk.CTkFrame(self.norm_app_frame, fg_color=COLORS["card"],
                                                   corner_radius=8, border_width=1,
                                                   border_color=COLORS["border"])
        self.norm_rules_list_frame.pack(fill="x", padx=16, pady=(0, 4))

        ctk.CTkLabel(
            self.norm_rules_list_frame, text="Nenhuma regra adicionada",
            font=ctk.CTkFont(size=11), text_color=COLORS["text_dim"]
        ).pack(padx=12, pady=8)

        ctk.CTkFrame(left, fg_color=COLORS["border"], height=1).pack(fill="x", padx=16, pady=12)

        # Baixar Modelo (sempre visível)
        ctk.CTkButton(
            left, text="📥  Baixar Modelo Base",
            font=ctk.CTkFont(size=12), fg_color=COLORS["primary"],
            hover_color=COLORS["secondary"], corner_radius=6, height=32,
            command=self._download_normalizacao_template
        ).pack(fill="x", padx=16, pady=(0, 4))

        ctk.CTkFrame(left, fg_color=COLORS["border"], height=1).pack(fill="x", padx=16, pady=12)

        # Formato de saída
        ctk.CTkLabel(left, text="📄  FORMATO DE SAÍDA",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", padx=16, pady=(0, 8))

        for val, label in [("xlsx", "XLSX (Excel)"), ("csv", "CSV (separado por |)"),
                           ("ambos", "Ambos")]:
            ctk.CTkRadioButton(
                left, text=label, variable=self.norm_formato,
                value=val, font=ctk.CTkFont(size=13),
                fg_color=COLORS["secondary"], hover_color=COLORS["primary"],
                border_color=COLORS["border"], text_color=COLORS["text"]
            ).pack(anchor="w", pady=3, padx=24)

        ctk.CTkFrame(left, fg_color=COLORS["border"], height=1).pack(fill="x", padx=16, pady=12)

        # Pasta de saída
        ctk.CTkLabel(left, text="💾  PASTA DE SAÍDA",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", padx=16, pady=(0, 6))

        norm_out_row = ctk.CTkFrame(left, fg_color="transparent")
        norm_out_row.pack(fill="x", padx=16, pady=(0, 4))

        self.norm_out_entry = ctk.CTkEntry(
            norm_out_row, textvariable=self.norm_output_dir,
            placeholder_text="Mesma pasta do arquivo de dados",
            fg_color=COLORS["card"], border_color=COLORS["border"],
            text_color=COLORS["text"], font=ctk.CTkFont(size=11),
        )
        self.norm_out_entry.pack(side="left", fill="x", expand=True, padx=(0, 4))

        ctk.CTkButton(
            norm_out_row, text="📂", width=36, height=28,
            font=ctk.CTkFont(size=14),
            fg_color=COLORS["card"], hover_color=COLORS["card_hover"],
            border_width=1, border_color=COLORS["border"],
            corner_radius=6, command=self._on_norm_browse_output
        ).pack(side="right")

        # Botão processar
        self.norm_btn_processar = ctk.CTkButton(
            left, text="▶  NORMALIZAR", font=ctk.CTkFont(size=14, weight="bold"),
            fg_color=COLORS["secondary"], hover_color=COLORS["primary"],
            corner_radius=8, height=44, command=self._on_normalizar
        )
        self.norm_btn_processar.pack(fill="x", padx=16, pady=(12, 16))

        # --- PAINEL DIREITO (Log) ---
        right = ctk.CTkFrame(tab, fg_color=COLORS["surface"], corner_radius=12,
                              border_width=1, border_color=COLORS["border"])
        right.grid(row=0, column=1, sticky="nsew", padx=(8, 0))

        right_inner = ctk.CTkFrame(right, fg_color="transparent")
        right_inner.pack(fill="both", expand=True, padx=16, pady=16)

        ctk.CTkLabel(right_inner, text="📋  LOG DE NORMALIZAÇÃO",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", pady=(0, 8))

        self.norm_log_text = ctk.CTkTextbox(right_inner, fg_color=COLORS["card"],
                                             text_color=COLORS["text"],
                                             font=ctk.CTkFont(family="Consolas", size=12),
                                             corner_radius=8, border_width=1,
                                             border_color=COLORS["border"],
                                             state="disabled", wrap="word")
        self.norm_log_text.pack(fill="both", expand=True)

    # --------------------------------------------------------
    # NORMALIZAÇÃO — EVENTOS
    # --------------------------------------------------------
    def _on_norm_method_change(self):
        if self.norm_method.get() == "excel":
            self.norm_app_frame.pack_forget()
            self.norm_excel_frame.pack(fill="x")
        else:
            self.norm_excel_frame.pack_forget()
            self.norm_app_frame.pack(fill="x")

    def _on_norm_data_drop(self, event):
        path = event.data.strip()
        if path.startswith("{"):
            path = path[1:]
        if path.endswith("}"):
            path = path[:-1]
        path = path.strip('"').strip("'")
        self._set_norm_data_file(path)

    def _on_norm_data_browse(self, _event=None):
        path = filedialog.askopenfilename(
            title="Selecionar arquivo de dados",
            filetypes=[("Excel/CSV", "*.xlsx *.xls *.csv"), ("Excel", "*.xlsx *.xls"), ("CSV", "*.csv"), ("Todos", "*.*")]
        )
        if path:
            self._set_norm_data_file(path)

    def _set_norm_data_file(self, path):
        if not path.lower().endswith((".xlsx", ".xls", ".csv")):
            messagebox.showwarning("Arquivo inválido", "Selecione um arquivo .xlsx, .xls ou .csv")
            return
        self.norm_input_file = path
        nome = os.path.basename(path)
        self.norm_data_file_label.configure(text=f"✓ {nome}")
        self.norm_data_label.configure(text=f"📄 {nome}")
        self.norm_data_drop.configure(border_color=COLORS["success"])
        self._norm_log(f"📄 Arquivo de dados: {nome}")

    def _on_norm_rules_drop(self, event):
        path = event.data.strip()
        if path.startswith("{"):
            path = path[1:]
        if path.endswith("}"):
            path = path[:-1]
        path = path.strip('"').strip("'")
        self._set_norm_rules_file(path)

    def _on_norm_rules_browse(self, _event=None):
        path = filedialog.askopenfilename(
            title="Selecionar arquivo de regras",
            filetypes=[("Excel/CSV", "*.xlsx *.xls *.csv"), ("Excel", "*.xlsx *.xls"), ("CSV", "*.csv"), ("Todos", "*.*")]
        )
        if path:
            self._set_norm_rules_file(path)

    def _set_norm_rules_file(self, path):
        if not path.lower().endswith((".xlsx", ".xls", ".csv")):
            messagebox.showwarning("Arquivo inválido", "Selecione um arquivo .xlsx, .xls ou .csv")
            return
        self.norm_rules_file = path
        nome = os.path.basename(path)
        self.norm_rules_file_label.configure(text=f"✓ {nome}")
        self.norm_rules_label.configure(text=f"📄 {nome}")
        self.norm_rules_drop.configure(border_color=COLORS["success"])
        self._norm_log(f"📋 Arquivo de regras: {nome}")

    def _on_norm_browse_output(self):
        path = filedialog.askdirectory(title="Selecionar pasta de saída")
        if path:
            self.norm_output_dir.set(path)
            self._norm_log(f"💾 Pasta de saída: {path}")

    def _add_norm_rule(self):
        marca = self.norm_marca_entry.get().strip()
        if not marca:
            messagebox.showwarning("Atenção", "Informe a MARCA.")
            return
        pn = self.norm_pn_entry.get().strip()
        self.norm_regras_manual.append({"marca": marca.upper(), "partnumber": pn.upper()})
        self.norm_marca_entry.delete(0, "end")
        self.norm_pn_entry.delete(0, "end")
        self._refresh_norm_rules_list()

    def _remove_norm_rule(self, idx):
        if 0 <= idx < len(self.norm_regras_manual):
            self.norm_regras_manual.pop(idx)
            self._refresh_norm_rules_list()

    def _refresh_norm_rules_list(self):
        for w in self.norm_rules_list_frame.winfo_children():
            w.destroy()
        if not self.norm_regras_manual:
            ctk.CTkLabel(
                self.norm_rules_list_frame, text="Nenhuma regra adicionada",
                font=ctk.CTkFont(size=11), text_color=COLORS["text_dim"]
            ).pack(padx=12, pady=8)
            return
        for i, regra in enumerate(self.norm_regras_manual):
            row = ctk.CTkFrame(self.norm_rules_list_frame, fg_color="transparent")
            row.pack(fill="x", padx=8, pady=2)
            if regra["partnumber"]:
                texto = f"🔧 {regra['marca']} / {regra['partnumber']}"
            else:
                texto = f"🏷️ {regra['marca']}"
            ctk.CTkLabel(row, text=texto, font=ctk.CTkFont(size=11),
                         text_color=COLORS["text"]).pack(side="left", padx=4)
            ctk.CTkButton(
                row, text="🗑", width=28, height=24, font=ctk.CTkFont(size=11),
                fg_color=COLORS["error"], hover_color="#D32F2F",
                corner_radius=4, command=lambda idx=i: self._remove_norm_rule(idx)
            ).pack(side="right", padx=4)

    def _download_normalizacao_template(self):
        path = filedialog.asksaveasfilename(
            title="Salvar Modelo Base",
            defaultextension=".xlsx",
            filetypes=[("Excel", "*.xlsx")],
            initialfile="modelo_normalizacao.xlsx"
        )
        if not path:
            return
        try:
            template_df = pd.DataFrame({
                "MARCA": ["HELLA", "DONALDSON", "BOSCH"],
                "PARTNUMBER": ["", "P559000", ""]
            })
            template_df.to_excel(path, index=False, engine="openpyxl")
            self._norm_log(f"📥 Modelo salvo: {path}")
            messagebox.showinfo("Modelo Salvo",
                                f"Modelo base salvo em:\n{path}\n\n"
                                f"• Linha com só MARCA: normaliza marcas que contêm o texto\n"
                                f"• Linha com MARCA + PARTNUMBER: se o PN existir no arquivo,\n"
                                f"  a marca será alterada para a MARCA da regra")
        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao salvar modelo:\n{e}")

    # --------------------------------------------------------
    # NORMALIZAÇÃO — LOG & PROCESSAMENTO
    # --------------------------------------------------------
    def _norm_log(self, msg):
        def _update():
            self.norm_log_text.configure(state="normal")
            self.norm_log_text.insert("end", msg + "\n")
            self.norm_log_text.see("end")
            self.norm_log_text.configure(state="disabled")
        self.root.after(0, _update)

    def _norm_clear_log(self):
        self.norm_log_text.configure(state="normal")
        self.norm_log_text.delete("1.0", "end")
        self.norm_log_text.configure(state="disabled")

    def _norm_set_processing(self, active):
        self.norm_processando = active
        state = "disabled" if active else "normal"
        self.root.after(0, lambda: self.norm_btn_processar.configure(state=state))
        if active:
            self.root.after(0, lambda: self.progress.configure(mode="indeterminate"))
            self.root.after(0, lambda: self.progress.start())
        else:
            self.root.after(0, lambda: self.progress.stop())
            self.root.after(0, lambda: self.progress.configure(mode="determinate"))
            self.root.after(0, lambda: self.progress.set(1.0))

    def _on_normalizar(self):
        if self.norm_processando:
            return
        if not self.norm_input_file or not os.path.exists(self.norm_input_file):
            messagebox.showwarning("Atenção", "Selecione o arquivo de dados primeiro.")
            return

        regras = []
        method = self.norm_method.get()

        if method == "excel":
            if not self.norm_rules_file or not os.path.exists(self.norm_rules_file):
                messagebox.showwarning("Atenção", "Selecione o arquivo de regras.")
                return
            try:
                df_regras = ler_arquivo(self.norm_rules_file, dtype=str)
                col_marca = None
                col_pn = None
                for c in df_regras.columns:
                    upper = c.upper().strip()
                    if upper == "MARCA":
                        col_marca = c
                    elif upper in ("PARTNUMBER", "PART NUMBER", "PART_NUMBER", "PN"):
                        col_pn = c
                if col_marca is None:
                    messagebox.showwarning("Erro",
                                           "Coluna MARCA não encontrada no arquivo de regras.")
                    return
                for _, row in df_regras.iterrows():
                    marca = str(row.get(col_marca, "")).strip()
                    pn = str(row.get(col_pn, "")).strip() if col_pn else ""
                    if marca and marca.upper() != "NAN":
                        pn_clean = pn.upper() if pn.upper() != "NAN" else ""
                        regras.append({"marca": marca.upper(), "partnumber": pn_clean})
            except Exception as e:
                messagebox.showerror("Erro", f"Erro ao ler arquivo de regras:\n{e}")
                return
        else:
            if not self.norm_regras_manual:
                messagebox.showwarning("Atenção", "Adicione pelo menos uma regra.")
                return
            regras = list(self.norm_regras_manual)

        if not regras:
            messagebox.showwarning("Atenção", "Nenhuma regra válida encontrada.")
            return

        self._norm_clear_log()
        output_dir = self.norm_output_dir.get().strip() or os.path.dirname(self.norm_input_file)
        if not os.path.isdir(output_dir):
            messagebox.showwarning("Pasta inválida",
                                   f"A pasta de saída não existe:\n{output_dir}")
            return
        formato = self.norm_formato.get()

        self._norm_set_processing(True)
        self._set_status("Normalizando dados...")

        t = threading.Thread(target=processar_normalizacao, daemon=True,
                             args=(self.norm_input_file, regras, output_dir, formato,
                                   self._norm_log, self._norm_done))
        t.start()

    def _norm_done(self, success):
        self._norm_set_processing(False)
        if success:
            self._set_status("✅ Normalização concluída!", 1.0)
            self._norm_log("\n✅ NORMALIZAÇÃO CONCLUÍDA COM SUCESSO!")
            self.root.after(0, lambda: messagebox.showinfo(
                "Sucesso", "Dados normalizados e salvos com sucesso!"))
        else:
            self._set_status("❌ Erro na normalização", 0)

    # --------------------------------------------------------
    # BANCO DE DADOS — UI
    # --------------------------------------------------------
    def _build_banco_dados_tab(self):
        tab = self.tabview.tab("Banco de Dados")
        tab.grid_columnconfigure(0, weight=1)
        tab.grid_columnconfigure(1, weight=2)
        tab.grid_rowconfigure(0, weight=1)

        # --- PAINEL ESQUERDO (scrollable) ---
        left = ctk.CTkScrollableFrame(tab, fg_color=COLORS["surface"], corner_radius=12,
                                       border_width=1, border_color=COLORS["border"])
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 8))

        # Título
        ctk.CTkLabel(left, text="🗄️  PREPARAR PARA BANCO DE DADOS",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", padx=16, pady=(16, 4))
        ctk.CTkLabel(left, text="Mapeia as colunas do arquivo tratado\npara o formato padrão do banco de dados.",
                     font=ctk.CTkFont(size=11), text_color=COLORS["text_dim"],
                     justify="left").pack(anchor="w", padx=16, pady=(0, 8))

        ctk.CTkFrame(left, fg_color=COLORS["border"], height=1).pack(fill="x", padx=16, pady=8)

        # Arquivo de entrada
        ctk.CTkLabel(left, text="📁  ARQUIVO DE ENTRADA",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", padx=16, pady=(0, 4))
        ctk.CTkLabel(left, text="Excel ou CSV (sep. |) já tratado (Equador ou Argentina)",
                     font=ctk.CTkFont(size=11), text_color=COLORS["text_dim"]
                     ).pack(anchor="w", padx=16, pady=(0, 8))

        self.db_data_drop = ctk.CTkFrame(left, fg_color=COLORS["card"],
                                          corner_radius=10, height=80,
                                          border_width=2, border_color=COLORS["border"])
        self.db_data_drop.pack(fill="x", padx=16, pady=(0, 4))
        self.db_data_drop.pack_propagate(False)

        self.db_data_label = ctk.CTkLabel(
            self.db_data_drop, text="Arraste o arquivo .xlsx / .csv aqui\nou clique para selecionar",
            font=ctk.CTkFont(size=11), text_color=COLORS["text_dim"], justify="center"
        )
        self.db_data_label.pack(expand=True)

        self.db_data_drop.drop_target_register(DND_FILES)
        self.db_data_drop.dnd_bind("<<Drop>>", self._on_db_data_drop)
        self.db_data_label.drop_target_register(DND_FILES)
        self.db_data_label.dnd_bind("<<Drop>>", self._on_db_data_drop)
        self.db_data_drop.bind("<Button-1>", self._on_db_data_browse)
        self.db_data_label.bind("<Button-1>", self._on_db_data_browse)

        self.db_data_file_label = ctk.CTkLabel(left, text="", font=ctk.CTkFont(size=11),
                                                text_color=COLORS["success"], wraplength=250)
        self.db_data_file_label.pack(anchor="w", padx=16)

        ctk.CTkFrame(left, fg_color=COLORS["border"], height=1).pack(fill="x", padx=16, pady=12)

        # Linhas por arquivo
        ctk.CTkLabel(left, text="✂️  LINHAS POR ARQUIVO",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", padx=16, pady=(0, 4))
        ctk.CTkLabel(left, text="Divide o resultado em partes (0 = sem divisão)",
                     font=ctk.CTkFont(size=11), text_color=COLORS["text_dim"]
                     ).pack(anchor="w", padx=16, pady=(0, 8))

        linhas_row = ctk.CTkFrame(left, fg_color="transparent")
        linhas_row.pack(fill="x", padx=16, pady=(0, 4))

        self.db_linhas_entry = ctk.CTkEntry(
            linhas_row, textvariable=self.db_linhas_var,
            fg_color=COLORS["card"], border_color=COLORS["border"],
            text_color=COLORS["text"], font=ctk.CTkFont(size=12), width=120,
        )
        self.db_linhas_entry.pack(side="left", padx=(0, 8))

        ctk.CTkLabel(linhas_row, text="linhas",
                     font=ctk.CTkFont(size=12), text_color=COLORS["text_dim"]
                     ).pack(side="left")

        # Atalhos rápidos
        atalhos_row = ctk.CTkFrame(left, fg_color="transparent")
        atalhos_row.pack(fill="x", padx=16, pady=(4, 4))

        for valor, label in [("10000", "10K"), ("50000", "50K"), ("100000", "100K"), ("0", "Tudo")]:
            ctk.CTkButton(
                atalhos_row, text=label, width=55, height=26,
                font=ctk.CTkFont(size=11),
                fg_color=COLORS["card"], hover_color=COLORS["card_hover"],
                border_width=1, border_color=COLORS["border"],
                corner_radius=6,
                command=lambda v=valor: self.db_linhas_var.set(v)
            ).pack(side="left", padx=2)

        ctk.CTkFrame(left, fg_color=COLORS["border"], height=1).pack(fill="x", padx=16, pady=12)

        # Formato de saída
        ctk.CTkLabel(left, text="📄  FORMATO DE SAÍDA",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", padx=16, pady=(0, 8))

        for val, label in [("xlsx", "XLSX (Excel)"), ("csv", "CSV (separado por |)"),
                           ("ambos", "Ambos")]:
            ctk.CTkRadioButton(
                left, text=label, variable=self.db_formato,
                value=val, font=ctk.CTkFont(size=13),
                fg_color=COLORS["secondary"], hover_color=COLORS["primary"],
                border_color=COLORS["border"], text_color=COLORS["text"]
            ).pack(anchor="w", pady=3, padx=24)

        ctk.CTkFrame(left, fg_color=COLORS["border"], height=1).pack(fill="x", padx=16, pady=12)

        # Pasta de saída
        ctk.CTkLabel(left, text="💾  PASTA DE SAÍDA",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", padx=16, pady=(0, 6))

        db_out_row = ctk.CTkFrame(left, fg_color="transparent")
        db_out_row.pack(fill="x", padx=16, pady=(0, 4))

        self.db_out_entry = ctk.CTkEntry(
            db_out_row, textvariable=self.db_output_dir,
            placeholder_text="Mesma pasta do arquivo de entrada",
            fg_color=COLORS["card"], border_color=COLORS["border"],
            text_color=COLORS["text"], font=ctk.CTkFont(size=11),
        )
        self.db_out_entry.pack(side="left", fill="x", expand=True, padx=(0, 4))

        ctk.CTkButton(
            db_out_row, text="📂", width=36, height=28,
            font=ctk.CTkFont(size=14),
            fg_color=COLORS["card"], hover_color=COLORS["card_hover"],
            border_width=1, border_color=COLORS["border"],
            corner_radius=6, command=self._on_db_browse_output
        ).pack(side="right")

        # Botão processar
        self.db_btn_processar = ctk.CTkButton(
            left, text="▶  PREPARAR BANCO", font=ctk.CTkFont(size=14, weight="bold"),
            fg_color=COLORS["secondary"], hover_color=COLORS["primary"],
            corner_radius=8, height=44, command=self._on_preparar_banco
        )
        self.db_btn_processar.pack(fill="x", padx=16, pady=(12, 16))

        # --- PAINEL DIREITO (Log) ---
        right = ctk.CTkFrame(tab, fg_color=COLORS["surface"], corner_radius=12,
                              border_width=1, border_color=COLORS["border"])
        right.grid(row=0, column=1, sticky="nsew", padx=(8, 0))

        right_inner = ctk.CTkFrame(right, fg_color="transparent")
        right_inner.pack(fill="both", expand=True, padx=16, pady=16)

        ctk.CTkLabel(right_inner, text="📋  LOG — BANCO DE DADOS",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", pady=(0, 8))

        self.db_log_text = ctk.CTkTextbox(right_inner, fg_color=COLORS["card"],
                                           text_color=COLORS["text"],
                                           font=ctk.CTkFont(family="Consolas", size=12),
                                           corner_radius=8, border_width=1,
                                           border_color=COLORS["border"],
                                           state="disabled", wrap="word")
        self.db_log_text.pack(fill="both", expand=True)

        # Info do formato esperado
        info_frame = ctk.CTkFrame(right_inner, fg_color=COLORS["card"], corner_radius=8,
                                   border_width=1, border_color=COLORS["border"])
        info_frame.pack(fill="x", pady=(8, 0))

        ctk.CTkLabel(info_frame, text="📌  Mapeamento de Colunas",
                     font=ctk.CTkFont(size=12, weight="bold"),
                     text_color=COLORS["warning"]).pack(anchor="w", padx=12, pady=(8, 4))

        mapeamento_texto = (
            "identificador → NUMERO_DE_FORMULARIO\n"
            "importador → RAZON_SOCIAL_IMPORTADOR + IMPORTADORES\n"
            "País de origem → CODIGO_LUGAR_INGRESO_MERCA\n"
            "NANDINA / NCM-SIM → SUBPARTIDA_ARANCELARIA\n"
            "CANTIDAD → CANTIDAD_DCMS + CANTIDAD\n"
            "USD FOB / FOB DOLAR → VALOR_FOB_USD + VALOR_FOB_USD_2\n"
            "Descrição Comercial → DESCRIPCION_MERCANCIA\n"
            "Data → FECHA_LEVANTE  |  Partnumber → PARTNUMBERS\n"
            "MARCA → MARCA"
        )
        ctk.CTkLabel(info_frame, text=mapeamento_texto,
                     font=ctk.CTkFont(family="Consolas", size=10),
                     text_color=COLORS["text_dim"], justify="left"
                     ).pack(anchor="w", padx=12, pady=(0, 8))

    # --------------------------------------------------------
    # BANCO DE DADOS — EVENTOS
    # --------------------------------------------------------
    def _on_db_data_drop(self, event):
        path = event.data.strip()
        if path.startswith("{"):
            path = path[1:]
        if path.endswith("}"):
            path = path[:-1]
        path = path.strip('"').strip("'")
        self._set_db_data_file(path)

    def _on_db_data_browse(self, _event=None):
        path = filedialog.askopenfilename(
            title="Selecionar arquivo de dados",
            filetypes=[("Excel/CSV", "*.xlsx *.xls *.csv"), ("Excel", "*.xlsx *.xls"), ("CSV", "*.csv"), ("Todos", "*.*")]
        )
        if path:
            self._set_db_data_file(path)

    def _set_db_data_file(self, path):
        if not path.lower().endswith((".xlsx", ".xls", ".csv")):
            messagebox.showwarning("Arquivo inválido", "Selecione um arquivo .xlsx, .xls ou .csv")
            return
        self.db_input_file = path
        nome = os.path.basename(path)
        self.db_data_file_label.configure(text=f"✓ {nome}")
        self.db_data_label.configure(text=f"📄 {nome}")
        self.db_data_drop.configure(border_color=COLORS["success"])
        self._db_log(f"📄 Arquivo selecionado: {nome}")

    def _on_db_browse_output(self):
        path = filedialog.askdirectory(title="Selecionar pasta de saída")
        if path:
            self.db_output_dir.set(path)
            self._db_log(f"💾 Pasta de saída: {path}")

    # --------------------------------------------------------
    # BANCO DE DADOS — LOG & PROCESSAMENTO
    # --------------------------------------------------------
    def _db_log(self, msg):
        def _update():
            self.db_log_text.configure(state="normal")
            self.db_log_text.insert("end", msg + "\n")
            self.db_log_text.see("end")
            self.db_log_text.configure(state="disabled")
        self.root.after(0, _update)

    def _db_clear_log(self):
        self.db_log_text.configure(state="normal")
        self.db_log_text.delete("1.0", "end")
        self.db_log_text.configure(state="disabled")

    def _db_set_processing(self, active):
        self.db_processando = active
        state = "disabled" if active else "normal"
        self.root.after(0, lambda: self.db_btn_processar.configure(state=state))
        if active:
            self.root.after(0, lambda: self.progress.configure(mode="indeterminate"))
            self.root.after(0, lambda: self.progress.start())
        else:
            self.root.after(0, lambda: self.progress.stop())
            self.root.after(0, lambda: self.progress.configure(mode="determinate"))
            self.root.after(0, lambda: self.progress.set(1.0))

    def _on_preparar_banco(self):
        if self.db_processando:
            return
        if not self.db_input_file or not os.path.exists(self.db_input_file):
            messagebox.showwarning("Atenção", "Selecione o arquivo de dados primeiro.")
            return

        # Validar linhas por arquivo
        try:
            linhas = int(self.db_linhas_var.get().strip())
            if linhas < 0:
                raise ValueError
        except ValueError:
            messagebox.showwarning("Valor inválido",
                                   "Informe um número inteiro positivo para linhas por arquivo.\n"
                                   "Use 0 para não dividir.")
            return

        self._db_clear_log()
        output_dir = self.db_output_dir.get().strip() or os.path.dirname(self.db_input_file)
        if not os.path.isdir(output_dir):
            messagebox.showwarning("Pasta inválida",
                                   f"A pasta de saída não existe:\n{output_dir}")
            return
        formato = self.db_formato.get()

        self._db_set_processing(True)
        self._set_status("Preparando para banco de dados...")

        t = threading.Thread(target=processar_banco_dados, daemon=True,
                             args=(self.db_input_file, output_dir, formato, linhas,
                                   self._db_log, self._db_done))
        t.start()

    def _db_done(self, success):
        self._db_set_processing(False)
        if success:
            self._set_status("✅ Banco de dados preparado!", 1.0)
            self._db_log("\n✅ PREPARAÇÃO CONCLUÍDA COM SUCESSO!")
            self.root.after(0, lambda: messagebox.showinfo(
                "Sucesso", "Arquivo preparado para o banco de dados com sucesso!"))
        else:
            self._set_status("❌ Erro na preparação", 0)

    # --------------------------------------------------------
    # RUN
    # --------------------------------------------------------
    def run(self):
        self.root.mainloop()


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    app = App()
    app.run()
