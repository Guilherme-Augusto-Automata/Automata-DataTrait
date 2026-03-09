"""
Tratamento de dados — Chile.
Extração de MARCA, PARTNUMBER, NCM, USD FOB, CANTIDAD, DESCRICAO,
IMPORTADOR (PROCV), IDENTIFICADOR e DATA.
Usa Gemini AI para partnumbers SIN-CODIGO e descrições.
"""

import os
import re
import json
import time

import numpy as np
import pandas as pd

from config.settings import STR_DTYPE
from infrastructure.file_io import ler_arquivo, exportar_resultado, col_idx


# ============================================================
# ORQUESTRADOR
# ============================================================

def processar_chile(input_path: str, secondary_path: str,
                    output_dir: str, formato: str, api_key: str,
                    log_callback, done_callback) -> None:
    """Orquestra o processamento do Chile."""
    try:
        df = _carregar_arquivo(input_path, log_callback)
        df_sec = _carregar_arquivo_secundario(secondary_path, log_callback)

        _extrair_ncm(df, log_callback)
        _extrair_marca(df, log_callback)
        _copiar_cantidad(df, log_callback)
        _calcular_fob_usd(df, log_callback)
        _extrair_identificador(df, log_callback)
        _extrair_data(df, log_callback)
        _extrair_importador(df, df_sec, log_callback)
        _extrair_partnumber_e_descricao(df, api_key, log_callback)

        _debug_colunas_finais(df, log_callback)

        base_name = os.path.splitext(os.path.basename(input_path))[0]
        exportar_resultado(df, output_dir, base_name + "_tratado", formato, log_callback)
        done_callback(True)
    except Exception as e:
        log_callback(f"❌ ERRO: {e}")
        import traceback
        log_callback(traceback.format_exc())
        done_callback(False)


# ============================================================
# CARREGAMENTO
# ============================================================

def _carregar_arquivo(input_path: str, log_callback) -> pd.DataFrame:
    """Carrega o arquivo principal do Chile (CSV | ou XLSX)."""
    log_callback("📂 Carregando arquivo principal do Chile...")
    t0 = time.perf_counter()
    df = ler_arquivo(input_path, dtype=str)
    log_callback(f"  ✓ {len(df):,} linhas × {len(df.columns)} colunas "
                 f"({time.perf_counter()-t0:.1f}s)")
    log_callback(f"  Colunas: {list(df.columns[:10])}...")
    return df


def _carregar_arquivo_secundario(secondary_path: str, log_callback) -> pd.DataFrame:
    """Carrega o arquivo secundário para PROCV do importador."""
    log_callback("📂 Carregando arquivo secundário (PROCV importador)...")
    t0 = time.perf_counter()
    df = ler_arquivo(secondary_path, dtype=str)
    log_callback(f"  ✓ {len(df):,} linhas × {len(df.columns)} colunas "
                 f"({time.perf_counter()-t0:.1f}s)")
    return df


# ============================================================
# EXTRAÇÃO DE CAMPOS
# ============================================================

def _extrair_ncm(df: pd.DataFrame, log_callback) -> None:
    """Copia coluna ARANC-NAC para nova coluna NCM."""
    log_callback("\n📋 Extraindo NCM...")
    t0 = time.perf_counter()

    # Tenta por nome de coluna
    ncm_col = _encontrar_coluna(df, ["ARANC-NAC", "ARANC_NAC", "ARANCELARIA"])
    if ncm_col is not None:
        df["NCM"] = df[ncm_col].astype(str).str.strip()
        log_callback(f"  ✓ NCM extraído da coluna '{ncm_col}' — "
                     f"{df['NCM'].notna().sum():,} valores ({time.perf_counter()-t0:.2f}s)")
    else:
        log_callback("  ⚠️ Coluna ARANC-NAC não encontrada por nome. "
                     "Verifique o arquivo de entrada.")
        df["NCM"] = ""


def _extrair_marca(df: pd.DataFrame, log_callback) -> None:
    """
    Extrai MARCA da coluna EE (DMARCA).
    - Retira todo texto após '~'
    - Substitui '-F' por vazio
    """
    log_callback("🏷️  Extraindo MARCA (coluna EE / DMARCA)...")
    t0 = time.perf_counter()

    idx_ee = col_idx("EE")
    if idx_ee < len(df.columns):
        col_name = df.columns[idx_ee]
        marca_raw = df.iloc[:, idx_ee].astype(str).str.strip()
        # Retirar tudo após '~'
        marca = marca_raw.str.split("~").str[0].str.strip()
        # Substituir '-F' por vazio
        marca = marca.str.replace("-F", "", regex=False).str.strip()
        # Limpar valores nulos
        marca = marca.replace({"nan": "", "None": "", "NaN": ""})
        df["MARCA"] = marca
        marcas_ok = (df["MARCA"] != "").sum()
        log_callback(f"  ✓ {marcas_ok:,} marcas extraídas da coluna '{col_name}' "
                     f"({time.perf_counter()-t0:.2f}s)")
    else:
        log_callback(f"  ⚠️ Coluna EE (índice {idx_ee}) não existe. "
                     f"Total de colunas: {len(df.columns)}")
        df["MARCA"] = ""


def _copiar_cantidad(df: pd.DataFrame, log_callback) -> None:
    """Copia coluna EM para nova coluna CANTIDAD."""
    log_callback("📊 Copiando CANTIDAD (coluna EM)...")
    idx_em = col_idx("EM")
    if idx_em < len(df.columns):
        col_name = df.columns[idx_em]
        df["CANTIDAD"] = df.iloc[:, idx_em]
        log_callback(f"  ✓ {df['CANTIDAD'].notna().sum():,} valores copiados "
                     f"da coluna '{col_name}'")
    else:
        log_callback(f"  ⚠️ Coluna EM (índice {idx_em}) não existe.")
        df["CANTIDAD"] = ""


def _calcular_fob_usd(df: pd.DataFrame, log_callback) -> None:
    """Calcula USD FOB = EM × EP."""
    log_callback("💵 Calculando USD FOB (EM × EP)...")
    t0 = time.perf_counter()

    idx_em = col_idx("EM")
    idx_ep = col_idx("EP")

    if idx_em < len(df.columns) and idx_ep < len(df.columns):
        col_em_name = df.columns[idx_em]
        col_ep_name = df.columns[idx_ep]
        em_vals = pd.to_numeric(df.iloc[:, idx_em], errors="coerce")
        ep_vals = pd.to_numeric(df.iloc[:, idx_ep], errors="coerce")
        df["USD FOB"] = (em_vals * ep_vals).round(2)
        fob_ok = df["USD FOB"].notna().sum()
        log_callback(f"  ✓ {fob_ok:,} valores FOB calculados "
                     f"({col_em_name} × {col_ep_name}) "
                     f"({time.perf_counter()-t0:.2f}s)")
    else:
        log_callback(f"  ⚠️ Colunas EM ({idx_em}) ou EP ({idx_ep}) não existem. "
                     f"Total: {len(df.columns)}")
        df["USD FOB"] = np.nan


def _extrair_identificador(df: pd.DataFrame, log_callback) -> None:
    """Copia coluna A para nova coluna IDENTIFICADOR."""
    log_callback("🆔 Extraindo IDENTIFICADOR (coluna A)...")
    idx_a = col_idx("A")
    if idx_a < len(df.columns):
        df["IDENTIFICADOR"] = df.iloc[:, idx_a].astype(str).str.strip()
        log_callback(f"  ✓ {df['IDENTIFICADOR'].notna().sum():,} identificadores copiados")
    else:
        log_callback("  ⚠️ Coluna A não encontrada.")
        df["IDENTIFICADOR"] = ""


def _extrair_data(df: pd.DataFrame, log_callback) -> None:
    """Copia coluna AM (FECTRA) para nova coluna DATA."""
    log_callback("📅 Extraindo DATA (coluna AM / FECTRA)...")
    idx_am = col_idx("AM")
    if idx_am < len(df.columns):
        col_name = df.columns[idx_am]
        df["DATA"] = df.iloc[:, idx_am].astype(str).str.strip()
        df["DATA"] = df["DATA"].replace({"nan": "", "None": "", "NaN": ""})
        log_callback(f"  ✓ {(df['DATA'] != '').sum():,} datas extraídas "
                     f"da coluna '{col_name}'")
    else:
        log_callback(f"  ⚠️ Coluna AM (índice {idx_am}) não encontrada.")
        df["DATA"] = ""


def _extrair_importador(df: pd.DataFrame, df_sec: pd.DataFrame,
                        log_callback) -> None:
    """
    PROCV: Procura coluna A (planilha principal) na coluna AN (planilha secundária).
    Quando encontra, pega a primeira linha correspondente e retorna a coluna E (importador).
    """
    log_callback("🏢 Extraindo IMPORTADOR via PROCV...")
    t0 = time.perf_counter()

    idx_a = col_idx("A")
    idx_an = col_idx("AN")
    idx_e = col_idx("E")

    if idx_a >= len(df.columns):
        log_callback("  ⚠️ Coluna A não existe na planilha principal.")
        df["IMPORTADOR"] = ""
        return

    if idx_an >= len(df_sec.columns) or idx_e >= len(df_sec.columns):
        log_callback(f"  ⚠️ Planilha secundária não tem colunas AN ({idx_an}) e/ou E ({idx_e}). "
                     f"Total: {len(df_sec.columns)}")
        df["IMPORTADOR"] = ""
        return

    # Criar mapeamento: valor coluna AN → valor coluna E (primeira ocorrência)
    sec_an = df_sec.iloc[:, idx_an].astype(str).str.strip()
    sec_e = df_sec.iloc[:, idx_e].astype(str).str.strip()

    # drop_duplicates mantém a primeira ocorrência
    lookup_df = pd.DataFrame({"chave": sec_an, "importador": sec_e})
    lookup_df = lookup_df.drop_duplicates(subset="chave", keep="first")
    lookup_map = dict(zip(lookup_df["chave"], lookup_df["importador"]))

    # Aplicar o PROCV
    chaves_principal = df.iloc[:, idx_a].astype(str).str.strip()
    df["IMPORTADOR"] = chaves_principal.map(lookup_map).fillna("")

    encontrados = (df["IMPORTADOR"] != "").sum()
    total = len(df)
    log_callback(f"  ✓ {encontrados:,}/{total:,} importadores encontrados "
                 f"({time.perf_counter()-t0:.2f}s)")


# ============================================================
# PARTNUMBER + DESCRICAO (com IA Gemini)
# ============================================================

def _extrair_partnumber_e_descricao(df: pd.DataFrame, api_key: str,
                                     log_callback) -> None:
    """
    Extrai PARTNUMBER da coluna ED (DNOMBRE) — texto antes de '~'.
    Para linhas com 'SIN-CODIGO', usa Gemini AI com colunas EF+EG+EH
    para inferir partnumber e descrição.
    Para linhas normais, a descrição também vem das colunas EF+EG+EH concatenadas.
    """
    log_callback("\n🔧 Extraindo PARTNUMBER (coluna ED / DNOMBRE)...")
    t0 = time.perf_counter()

    idx_ed = col_idx("ED")
    idx_ef = col_idx("EF")
    idx_eg = col_idx("EG")
    idx_eh = col_idx("EH")

    if idx_ed >= len(df.columns):
        log_callback(f"  ⚠️ Coluna ED (índice {idx_ed}) não existe.")
        df["PARTNUMBER"] = ""
        df["DESCRICAO"] = ""
        return

    # Extrair código antes de '~'
    dnombre_raw = df.iloc[:, idx_ed].astype(str).str.strip()
    pn_extraido = dnombre_raw.str.split("~").str[0].str.strip()
    pn_extraido = pn_extraido.replace({"nan": "", "None": "", "NaN": ""})

    # Construir descrição base das colunas EF, EG, EH
    desc_parts = []
    for idx_col in [idx_ef, idx_eg, idx_eh]:
        if idx_col < len(df.columns):
            desc_parts.append(df.iloc[:, idx_col].astype(str).str.strip()
                              .replace({"nan": "", "None": "", "NaN": ""}))
        else:
            desc_parts.append(pd.Series([""] * len(df), index=df.index))

    descricao_concat = (desc_parts[0] + " " + desc_parts[1] + " " + desc_parts[2]).str.strip()

    # Identificar linhas com SIN-CODIGO
    mask_sin = pn_extraido.str.upper().str.contains("SIN-CODIGO|SIN CODIGO|SINCODIGO",
                                                      regex=True, na=False)

    pn_ok = (~mask_sin & (pn_extraido != "")).sum()
    sin_count = mask_sin.sum()
    log_callback(f"  ✓ {pn_ok:,} partnumbers extraídos diretamente")
    log_callback(f"  ⚠️ {sin_count:,} linhas com SIN-CODIGO — serão consultadas via IA")

    # Inicializar colunas
    df["PARTNUMBER"] = pn_extraido
    df["DESCRICAO"] = descricao_concat

    # Para linhas SIN-CODIGO: consultar IA
    if sin_count > 0 and api_key:
        _resolver_sin_codigo_via_ia(df, mask_sin, descricao_concat, api_key, log_callback)
    elif sin_count > 0 and not api_key:
        log_callback("  ⚠️ Sem API Key Gemini. Linhas SIN-CODIGO ficarão sem partnumber.")

    total_pn = (df["PARTNUMBER"].astype(str).str.strip() != "").sum()
    log_callback(f"  ✓ Total final: {total_pn:,} partnumbers "
                 f"({time.perf_counter()-t0:.2f}s)")


def _resolver_sin_codigo_via_ia(df: pd.DataFrame, mask_sin: pd.Series,
                                 descricao_concat: pd.Series,
                                 api_key: str, log_callback) -> None:
    """Consulta Gemini AI em lotes para resolver SIN-CODIGO."""
    from google import genai

    log_callback("\n🤖 Consultando Gemini AI para SIN-CODIGO...")

    indices_sin = df.index[mask_sin].tolist()
    descricoes = descricao_concat.loc[mask_sin].tolist()

    # Processar em lotes de 50
    BATCH_SIZE = 50
    total_batches = (len(indices_sin) + BATCH_SIZE - 1) // BATCH_SIZE
    client = genai.Client(api_key=api_key)

    resolvidos_pn = 0
    resolvidos_desc = 0

    for batch_idx in range(total_batches):
        start = batch_idx * BATCH_SIZE
        end = min(start + BATCH_SIZE, len(indices_sin))
        batch_indices = indices_sin[start:end]
        batch_descs = descricoes[start:end]

        log_callback(f"  📦 Lote {batch_idx+1}/{total_batches} "
                     f"({len(batch_indices)} itens)...")

        prompt = _construir_prompt_partnumber(batch_descs)
        resposta = _enviar_com_retries(client, prompt, log_callback)

        # Abortar se erro fatal (API key inválida/vazada)
        if resposta == _FATAL_ERROR:
            log_callback(f"  ⚠️ Pulando {total_batches - batch_idx - 1} lotes restantes.")
            break

        if resposta:
            resultados = _parsear_resposta_partnumber(resposta, len(batch_indices),
                                                      log_callback)
            for i, idx_row in enumerate(batch_indices):
                if i < len(resultados):
                    pn_ia = resultados[i].get("partnumber", "").strip()
                    desc_ia = resultados[i].get("descricao", "").strip()
                    if pn_ia and pn_ia.upper() not in ("", "N/A", "DESCONHECIDO",
                                                        "SIN-CODIGO"):
                        df.at[idx_row, "PARTNUMBER"] = pn_ia
                        resolvidos_pn += 1
                    if desc_ia:
                        df.at[idx_row, "DESCRICAO"] = desc_ia
                        resolvidos_desc += 1

        # Rate limiting
        if batch_idx < total_batches - 1:
            time.sleep(2)

    log_callback(f"  ✓ IA resolveu {resolvidos_pn:,} partnumbers e "
                 f"{resolvidos_desc:,} descrições")


def _construir_prompt_partnumber(descricoes: list) -> str:
    """Constrói prompt para o Gemini identificar partnumbers e descrições."""
    itens_texto = "\n".join(
        f"{i+1}. \"{desc}\"" for i, desc in enumerate(descricoes)
    )
    return (
        "Você é um especialista em peças e componentes industriais/automotivos.\n"
        "Abaixo há descrições de produtos importados que não possuem código (partnumber).\n"
        "Para cada item, identifique:\n"
        "1. O PARTNUMBER mais provável (código do fabricante/modelo)\n"
        "2. Uma DESCRIÇÃO padronizada e limpa do produto\n\n"
        "REGRAS:\n"
        "- Se conseguir identificar o partnumber real, retorne-o.\n"
        "- Se não conseguir identificar com certeza, retorne string vazia para partnumber.\n"
        "- A descrição deve ser limpa, objetiva e em português.\n"
        "- Retorne APENAS um JSON válido, sem markdown, sem texto adicional.\n"
        "- Formato: [{\"partnumber\": \"XXX\", \"descricao\": \"YYY\"}, ...]\n"
        "- A lista deve ter exatamente o mesmo número de itens da entrada.\n\n"
        f"ITENS:\n{itens_texto}"
    )


# ============================================================
# CONSULTA IA (GEMINI)
# ============================================================

# Sentinel para abortar lotes restantes quando há erro fatal
_FATAL_ERROR = "__FATAL__"


def _enviar_com_retries(client, prompt: str, log_callback,
                        max_tentativas: int = 6) -> str | None:
    """Envia prompt ao Gemini com retries em caso de erro.
    Retorna _FATAL_ERROR se o erro for irrecuperável (ex: 403 PERMISSION_DENIED)."""
    for tentativa in range(max_tentativas):
        try:
            response = client.models.generate_content(
                model="gemini-2.0-flash", contents=prompt)
            return response.text.strip()
        except Exception as retry_err:
            err_str = str(retry_err)
            # Erros fatais — não adianta tentar de novo
            if any(k in err_str for k in ("403", "PERMISSION_DENIED", "leaked",
                                           "401", "UNAUTHENTICATED", "invalid")):
                log_callback(f"  ❌ Erro fatal de autenticação: {retry_err}")
                log_callback("  ❌ Abortando consultas IA — verifique sua API Key.")
                return _FATAL_ERROR
            if tentativa >= max_tentativas - 1:
                log_callback(f"  ⚠️ Falha final: {retry_err}")
                return None
            wait = _calcular_wait(tentativa, retry_err)
            log_callback(f"  ⏳ Tentativa {tentativa+1}/{max_tentativas} falhou, "
                         f"aguardando {wait}s...")
            time.sleep(wait)
    return None


def _calcular_wait(tentativa: int, erro: Exception) -> int:
    """Calcula o tempo de espera entre retries."""
    if "429" in str(erro):
        return min((tentativa + 1) * 15, 90)
    return (tentativa + 1) * 5


def _parsear_resposta_partnumber(resposta_texto: str, expected_count: int,
                                  log_callback) -> list:
    """Extrai lista de partnumbers/descrições do JSON retornado pela IA."""
    try:
        # Limpar markdown
        resposta_texto = re.sub(r"```json\s*", "", resposta_texto)
        resposta_texto = re.sub(r"```\s*", "", resposta_texto)
        resposta_texto = resposta_texto.strip()

        resultados = json.loads(resposta_texto)
        if isinstance(resultados, list):
            return resultados
        else:
            log_callback(f"  ⚠️ Resposta IA não é uma lista: {type(resultados)}")
            return []
    except json.JSONDecodeError as e:
        log_callback(f"  ⚠️ Erro ao parsear JSON da IA: {e}")
        log_callback(f"     Resposta: {resposta_texto[:200]}...")
        return []


# ============================================================
# UTILIDADES
# ============================================================

def _encontrar_coluna(df: pd.DataFrame, nomes_possiveis: list) -> str | None:
    """Busca uma coluna pelo nome (case-insensitive)."""
    colunas_lower = {c.lower().strip(): c for c in df.columns}
    for nome in nomes_possiveis:
        if nome.lower().strip() in colunas_lower:
            return colunas_lower[nome.lower().strip()]
    return None


# ============================================================
# DEBUG
# ============================================================

def _debug_colunas_finais(df: pd.DataFrame, log_callback) -> None:
    """Loga o estado das novas colunas adicionadas."""
    log_callback("\n🔍 DEBUG — Novas colunas Chile:")
    novas = ["NCM", "MARCA", "PARTNUMBER", "USD FOB", "CANTIDAD",
             "DESCRICAO", "IMPORTADOR", "IDENTIFICADOR", "DATA"]
    for col in novas:
        if col in df.columns:
            nn = int(df[col].notna().sum())
            fvi = df[col].first_valid_index()
            sample = df[col].loc[fvi] if fvi is not None else "VAZIO"
            log_callback(f"  {col}: {nn:,} vals | ex: {repr(sample)[:60]}")
        else:
            log_callback(f"  {col}: NÃO ENCONTRADA")
    log_callback(f"  Total: {len(df.columns)} colunas, {len(df):,} linhas")
    log_callback("  --- FIM DEBUG ---")
