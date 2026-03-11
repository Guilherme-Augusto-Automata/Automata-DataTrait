"""
Tratamento de dados — Argentina.
Extração vetorizada de marca/PN/cantidad + conversão de moeda via IA (Gemini).
Cada função faz uma única coisa.
"""

import os
import re
import json
import time

import numpy as np
import pandas as pd

from config.settings import STR_DTYPE, FALLBACK_COTACOES
from infrastructure.file_io import ler_arquivo, exportar_resultado


# ============================================================
# ORQUESTRADOR
# ============================================================

def processar_argentina(input_path: str, output_dir: str, formato: str,
                        api_key: str, ano_cotacao: str,
                        log_callback, done_callback, cotacoes_callback) -> None:
    """Orquestra o processamento da Argentina."""
    try:
        df = _carregar_arquivo(input_path, log_callback)
        _extrair_marca(df, log_callback)
        _extrair_partnumber(df, log_callback)
        _copiar_cantidad(df, log_callback)
        moedas_outras = _listar_moedas_nao_usd(df, log_callback)
        taxas_cambio = _obter_cotacoes(api_key, ano_cotacao, moedas_outras, log_callback)
        cotacoes_callback(taxas_cambio, moedas_outras, df, output_dir, formato)
    except Exception as e:
        log_callback(f"❌ ERRO: {e}")
        done_callback(False)


def finalizar_argentina(df: pd.DataFrame, taxas_cambio: dict,
                        output_dir: str, formato: str, base_name: str,
                        log_callback, done_callback) -> None:
    """Orquestra o cálculo FOB final e exportação."""
    try:
        _calcular_fob_usd(df, taxas_cambio, log_callback)
        _debug_colunas_finais(df, log_callback)
        exportar_resultado(df, output_dir, base_name + "_tratado", formato, log_callback)
        done_callback(True)
    except Exception as e:
        log_callback(f"❌ ERRO: {e}")
        done_callback(False)


# ============================================================
# CARREGAMENTO
# ============================================================

def _carregar_arquivo(input_path: str, log_callback) -> pd.DataFrame:
    """Carrega o arquivo da Argentina."""
    log_callback("📂 Carregando arquivo da Argentina...")
    t0 = time.perf_counter()
    if input_path.lower().endswith(".csv"):
        df = ler_arquivo(input_path)
    else:
        df = ler_arquivo(input_path, sheet_name="Planilha1")
    log_callback(f"  ✓ {len(df):,} linhas × {len(df.columns)} colunas "
                 f"({time.perf_counter()-t0:.1f}s)")
    if STR_DTYPE == "string[pyarrow]":
        log_callback("  ⚡ PyArrow ativo — operações de string otimizadas")
    return df


# ============================================================
# EXTRAÇÃO DE CAMPOS
# ============================================================

def _extrair_marca(df: pd.DataFrame, log_callback) -> None:
    """Extrai coluna MARCA via regex vetorizado."""
    log_callback("\n🏷️  Extraindo MARCA (vetorizado)...")
    t0 = time.perf_counter()
    df["MARCA"] = (
        df["Marca - Sufixos"]
        .astype(STR_DTYPE)
        .str.extract(r"MARCA:\s*(.+)", expand=False)
        .str.strip()
    )
    marcas_ok = df["MARCA"].notna().sum()
    log_callback(f"  ✓ {marcas_ok:,} marcas extraídas ({time.perf_counter()-t0:.2f}s)")


def _extrair_partnumber(df: pd.DataFrame, log_callback) -> None:
    """Extrai coluna PARTNUMBER via regex vetorizado (último parêntese)."""
    log_callback("🔧 Extraindo PARTNUMBER (vetorizado)...")
    t0 = time.perf_counter()
    df["PARTNUMBER"] = (
        df["Marca ou Descrição"]
        .astype(STR_DTYPE)
        .str.extract(r".*\(([^)]+)\)", expand=False)
        .str.strip()
    )
    pn_ok = df["PARTNUMBER"].notna().sum()
    log_callback(f"  ✓ {pn_ok:,} partnumbers extraídos ({time.perf_counter()-t0:.2f}s)")


def _copiar_cantidad(df: pd.DataFrame, log_callback) -> None:
    """Copia coluna Quantidade.1 para CANTIDAD."""
    log_callback("📊 Copiando CANTIDAD...")
    df["CANTIDAD"] = df["Quantidade.1"]
    log_callback(f"  ✓ {df['CANTIDAD'].notna().sum():,} cantidades copiadas")


# ============================================================
# MOEDAS
# ============================================================

def _listar_moedas_nao_usd(df: pd.DataFrame, log_callback) -> list:
    """Identifica moedas distintas que não são DOLAR USA."""
    log_callback("\n💰 Preparando conversão FOB...")
    moedas_distintas = df["Moeda"].unique().tolist()
    moedas_outras = [m for m in moedas_distintas
                     if m != "DOLAR USA" and pd.notna(m) and str(m).strip() != "`"]
    log_callback(f"  Moedas encontradas: {len(moedas_distintas)}")
    for m in moedas_outras:
        count = (df["Moeda"] == m).sum()
        log_callback(f"    • {m}: {count:,} linhas")
    return moedas_outras


def _obter_cotacoes(api_key: str, ano_cotacao: str,
                    moedas_outras: list, log_callback) -> dict:
    """Obtém cotações para as moedas (IA + fallback)."""
    if not moedas_outras:
        return {}
    taxas = _chamar_gemini(api_key, ano_cotacao, moedas_outras, log_callback)
    _aplicar_fallback(taxas, moedas_outras, log_callback)
    return taxas


# ============================================================
# CONSULTA IA (GEMINI)
# ============================================================

def _chamar_gemini(api_key: str, ano_cotacao: str,
                   moedas: list, log_callback) -> dict:
    """Consulta Gemini AI para cotações de câmbio."""
    log_callback("\n🤖 Consultando Gemini AI para cotações...")
    taxas = {}

    try:
        from google import genai
        client = genai.Client(api_key=api_key)
        prompt = _construir_prompt(ano_cotacao, moedas)
        resposta_texto = _enviar_com_retries(client, prompt, log_callback)

        if resposta_texto:
            taxas = _parsear_resposta(resposta_texto, moedas, log_callback)

    except Exception as e:
        log_callback(f"\n⚠️ Erro na consulta IA: {e}")

    return taxas


def _construir_prompt(ano_cotacao: str, moedas: list) -> str:
    """Constrói o prompt para a API do Gemini."""
    moedas_lista = ", ".join(moedas)
    return (
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


def _enviar_com_retries(client, prompt: str, log_callback,
                        max_tentativas: int = 6) -> str | None:
    """Envia prompt ao Gemini com retries em caso de erro."""
    for tentativa in range(max_tentativas):
        try:
            response = client.models.generate_content(
                model="gemini-2.0-flash", contents=prompt)
            return response.text.strip()
        except Exception as retry_err:
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


def _parsear_resposta(resposta_texto: str, moedas: list,
                      log_callback) -> dict:
    """Extrai cotações do JSON retornado pela IA."""
    resposta_texto = re.sub(r"```json\s*", "", resposta_texto)
    resposta_texto = re.sub(r"```\s*", "", resposta_texto)
    cotacoes_gemini = json.loads(resposta_texto)

    taxas = {}
    for moeda in moedas:
        valor = _encontrar_moeda_no_json(cotacoes_gemini, moeda)
        if valor is not None:
            taxas[moeda] = valor

    log_callback("\n📈 Cotações obtidas via IA:")
    for moeda, taxa in taxas.items():
        log_callback(f"    1 {moeda} = {taxa} USD")

    return taxas


def _encontrar_moeda_no_json(cotacoes: dict, moeda: str) -> float | None:
    """Busca o valor da moeda no JSON (match exato ou parcial)."""
    if moeda in cotacoes:
        return float(cotacoes[moeda])
    for chave, valor in cotacoes.items():
        if chave.upper() in moeda.upper() or moeda.upper() in chave.upper():
            return float(valor)
    return None


def _aplicar_fallback(taxas: dict, moedas: list, log_callback) -> None:
    """Preenche moedas faltantes com cotações de referência."""
    moedas_sem = [m for m in moedas if m not in taxas]
    if not moedas_sem:
        return

    ia_teve_resultado = bool(taxas)
    if not ia_teve_resultado:
        log_callback("\n⚠️ IA indisponível — usando cotações de referência.")
        log_callback("   Você poderá editar os valores na tela de revisão.")
    else:
        log_callback(f"\n⚠️ Moedas sem cotação da IA: {moedas_sem}")

    for moeda in moedas_sem:
        fallback_val = FALLBACK_COTACOES.get(moeda, 0.0)
        taxas[moeda] = fallback_val
        src = "referência" if fallback_val > 0 else "não encontrada"
        log_callback(f"    {moeda} = {fallback_val} USD ({src})")


# ============================================================
# CÁLCULO FOB
# ============================================================

def _calcular_fob_usd(df: pd.DataFrame, taxas_cambio: dict,
                      log_callback) -> None:
    """Calcula FOB em USD usando cotações vetorizadas."""
    log_callback("\n💵 Calculando FOB em USD (vetorizado)...")
    t0 = time.perf_counter()

    moeda_col = df["Moeda"].astype(str).str.strip()
    fob_moeda = pd.to_numeric(df["FOB Moeda"].astype(str).str.replace(",", "", regex=False), errors="coerce")
    usd_fob = pd.to_numeric(df["U$S FOB"].astype(str).str.replace(",", "", regex=False), errors="coerce")

    fob_result = pd.Series(np.nan, index=df.index)
    _preencher_dolar_usa(fob_result, moeda_col, usd_fob, fob_moeda)
    _converter_outras_moedas(fob_result, moeda_col, fob_moeda, taxas_cambio)

    df["FOB DOLAR"] = fob_result
    log_callback(f"  ✓ {df['FOB DOLAR'].notna().sum():,} valores FOB calculados "
                 f"({time.perf_counter()-t0:.2f}s)")


def _preencher_dolar_usa(fob_result: pd.Series, moeda_col: pd.Series,
                         usd_fob: pd.Series, fob_moeda: pd.Series) -> None:
    """Preenche FOB para linhas em DOLAR USA."""
    mask_usd = moeda_col == "DOLAR USA"
    fob_result[mask_usd] = np.where(
        usd_fob[mask_usd].fillna(0) > 0,
        usd_fob[mask_usd],
        fob_moeda[mask_usd]
    )


def _converter_outras_moedas(fob_result: pd.Series, moeda_col: pd.Series,
                             fob_moeda: pd.Series, taxas_cambio: dict) -> None:
    """Converte FOB de outras moedas para USD via taxa de câmbio."""
    mask_usd = moeda_col == "DOLAR USA"
    mask_invalido = moeda_col.isin(["`", "", "nan"])
    taxa_series = moeda_col.map(taxas_cambio)
    mask_convert = taxa_series.notna() & ~mask_usd & ~mask_invalido
    fob_result[mask_convert] = (
        fob_moeda[mask_convert] * taxa_series[mask_convert]
    ).round(2)


# ============================================================
# DEBUG
# ============================================================

def _debug_colunas_finais(df: pd.DataFrame, log_callback) -> None:
    """Loga o estado de todas as colunas antes do export."""
    log_callback("\n🔍 DEBUG — Colunas finais Argentina:")
    log_callback(f"  Total: {len(df.columns)} colunas, {len(df):,} linhas")
    notna_counts = df.notna().sum()
    for i, col in enumerate(df.columns):
        letter = _indice_para_letra(i)
        nn = int(notna_counts[col])
        dt = df[col].dtype
        fvi = df[col].first_valid_index()
        sample = df[col].loc[fvi] if fvi is not None else 'VAZIO'
        log_callback(f"  Col {letter} ({i}): [{dt}] '{col}' → "
                     f"{nn:,} vals | ex: {repr(sample)[:50]}")
    log_callback("  --- FIM DEBUG ---")


def _indice_para_letra(i: int) -> str:
    """Converte índice 0-based para letra de coluna Excel (A, B, ..., AA, AB)."""
    letter = ''
    idx = i
    while idx >= 0:
        letter = chr(idx % 26 + ord('A')) + letter
        idx = idx // 26 - 1
    return letter
