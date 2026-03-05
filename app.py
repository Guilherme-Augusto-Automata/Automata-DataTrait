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
        df = pd.read_excel(input_path, engine="calamine", dtype=str)
        log_callback(f"  ✓ {len(df):,} linhas × {len(df.columns)} colunas ({time.perf_counter()-t0:.1f}s)")

        # Garantir colunas suficientes até AZ
        while len(df.columns) <= col_idx("AZ"):
            df[f"_extra_{len(df.columns)}"] = None

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
        df = pd.read_excel(input_path, sheet_name="Planilha1", engine="calamine")
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
        for i, col in enumerate(df.columns):
            letter = ''
            idx = i
            while idx >= 0:
                letter = chr(idx % 26 + ord('A')) + letter
                idx = idx // 26 - 1
            nn = df[col].notna().sum()
            dt = df[col].dtype
            sample = df[col].dropna().iloc[0] if nn > 0 else 'VAZIO'
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

        # --- CORPO (2 colunas) ---
        body = ctk.CTkFrame(main, fg_color="transparent")
        body.pack(fill="both", expand=True)
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
            text="Arraste o arquivo .xlsx aqui\nou clique para selecionar",
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

    # --------------------------------------------------------
    # LOGO
    # --------------------------------------------------------
    def _build_logo(self, parent):
        """Carrega a logo da empresa no header."""
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
                    # Calcula proporção para caber na altura do header
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
                except Exception:
                    pass

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
            title="Selecionar arquivo Excel",
            filetypes=[("Excel", "*.xlsx *.xls"), ("Todos", "*.*")]
        )
        if path:
            self._set_file(path)

    def _set_file(self, path):
        if not path.lower().endswith((".xlsx", ".xls")):
            messagebox.showwarning("Arquivo inválido", "Selecione um arquivo .xlsx ou .xls")
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
