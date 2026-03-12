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
import threading

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
    """Copia coluna ARANC-NAC para nova coluna NANDINA."""
    log_callback("\n📋 Extraindo NANDINA...")
    t0 = time.perf_counter()

    # Tenta por nome de coluna
    ncm_col = _encontrar_coluna(df, ["ARANC-NAC", "ARANC_NAC", "ARANCELARIA"])

    # Debug: listar nomes de colunas próximas para diagnóstico
    log_callback(f"  🔍 Total de colunas: {len(df.columns)}")
    colunas_com_aranc = [c for c in df.columns if "ARANC" in c.upper() or "NCM" in c.upper() or "NANDINA" in c.upper()]
    log_callback(f"  🔍 Colunas com 'ARANC/NCM/NANDINA' no nome: {colunas_com_aranc}")

    if ncm_col is not None:
        raw = df[ncm_col].astype(str).str.strip()
        raw = raw.replace({"nan": "", "None": "", "NaN": ""})
        df["NANDINA"] = raw
        nao_vazio = (df["NANDINA"] != "").sum()
        log_callback(f"  ✓ NANDINA extraído da coluna '{ncm_col}' — "
                     f"{nao_vazio:,} valores não-vazios ({time.perf_counter()-t0:.2f}s)")
        # Amostras
        amostras = df["NANDINA"][df["NANDINA"] != ""].head(5).tolist()
        log_callback(f"  📊 Amostras NANDINA: {amostras}")
        vazios = (df["NANDINA"] == "").sum()
        if vazios > 0:
            log_callback(f"  ⚠️ {vazios:,} linhas com NANDINA vazio")
    else:
        log_callback("  ⚠️ Coluna ARANC-NAC não encontrada por nome. "
                     "Verifique o arquivo de entrada.")
        df["NANDINA"] = ""


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
        em_vals = pd.to_numeric(df.iloc[:, idx_em].astype(str).str.replace(",", "", regex=False), errors="coerce")
        ep_vals = pd.to_numeric(df.iloc[:, idx_ep].astype(str).str.replace(",", "", regex=False), errors="coerce")
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

    log_callback(f"  🔍 Índices: A={idx_a}, AN={idx_an}, E={idx_e}")
    log_callback(f"  🔍 Planilha principal: {len(df.columns)} colunas, {len(df):,} linhas")
    log_callback(f"  🔍 Planilha secundária: {len(df_sec.columns)} colunas, {len(df_sec):,} linhas")

    # Listar nomes de todas as colunas da secundária para diagnóstico
    log_callback(f"  🔍 Colunas secundária (primeiras 10): {list(df_sec.columns[:10])}")
    log_callback(f"  🔍 Colunas secundária (últimas 10): {list(df_sec.columns[-10:])}")

    if idx_a >= len(df.columns):
        log_callback(f"  ⚠️ Coluna A (idx {idx_a}) não existe na planilha principal ({len(df.columns)} cols).")
        df["IMPORTADOR"] = ""
        return

    if idx_an >= len(df_sec.columns) or idx_e >= len(df_sec.columns):
        log_callback(f"  ⚠️ Planilha secundária não tem colunas AN ({idx_an}) e/ou E ({idx_e}). "
                     f"Total: {len(df_sec.columns)}")
        df["IMPORTADOR"] = ""
        return

    # ── DEBUG: info das colunas usadas ──
    col_a_name = df.columns[idx_a]
    col_an_name = df_sec.columns[idx_an]
    col_e_name = df_sec.columns[idx_e]
    log_callback(f"  🔍 Coluna chave principal: A ('{col_a_name}', índice {idx_a})")
    log_callback(f"  🔍 Coluna lookup secundária: AN ('{col_an_name}', índice {idx_an})")
    log_callback(f"  🔍 Coluna importador: E ('{col_e_name}', índice {idx_e})")

    # Criar mapeamento: valor coluna AN → valor coluna E (primeira ocorrência)
    # Normalizar removendo zeros à esquerda para match
    sec_an = df_sec.iloc[:, idx_an].astype(str).str.strip().str.lstrip("0")
    sec_e = df_sec.iloc[:, idx_e].astype(str).str.strip()

    # drop_duplicates mantém a primeira ocorrência
    lookup_df = pd.DataFrame({"chave": sec_an, "importador": sec_e})
    lookup_df = lookup_df.drop_duplicates(subset="chave", keep="first")
    lookup_map = dict(zip(lookup_df["chave"], lookup_df["importador"]))

    # ── DEBUG: amostras das chaves ──
    log_callback(f"  📊 Lookup: {len(lookup_map):,} chaves únicas na secundária")
    sample_sec_keys = list(lookup_map.keys())[:5]
    log_callback(f"  📊 Amostras chave secundária (AN, sem zeros): {sample_sec_keys}")
    sample_sec_vals = [lookup_map[k] for k in sample_sec_keys]
    log_callback(f"  📊 Amostras importador (E):      {sample_sec_vals}")

    # Aplicar o PROCV — também normalizar chaves da principal
    chaves_principal = df.iloc[:, idx_a].astype(str).str.strip().str.lstrip("0")

    # ── DEBUG: amostras das chaves da principal ──
    sample_pri_keys = chaves_principal.head(5).tolist()
    log_callback(f"  📊 Amostras chave principal (A, sem zeros):  {sample_pri_keys}")

    # Verificar se há interseção
    set_pri = set(chaves_principal.unique())
    set_sec = set(lookup_map.keys())
    intersecao = set_pri & set_sec
    log_callback(f"  📊 Chaves principal: {len(set_pri):,} únicas")
    log_callback(f"  📊 Chaves secundária: {len(set_sec):,} únicas")
    log_callback(f"  📊 Interseção: {len(intersecao):,} chaves em comum")
    if len(intersecao) == 0 and len(set_pri) > 0 and len(set_sec) > 0:
        log_callback("  ⚠️ NENHUMA chave em comum mesmo sem zeros à esquerda!")
        log_callback(f"     - Principal[0]: {repr(sample_pri_keys[0]) if sample_pri_keys else 'VAZIO'}")
        log_callback(f"     - Secundária[0]: {repr(sample_sec_keys[0]) if sample_sec_keys else 'VAZIO'}")

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

    # Limpar PNs: separar prefixo descritivo de código real
    pn_extraido = pn_extraido.apply(_limpar_pn_chile)

    # Validar: rejeitar texto descritivo que não é partnumber real
    mask_nao_sin = ~pn_extraido.str.upper().str.contains(
        "SIN-CODIGO|SIN CODIGO|SINCODIGO", regex=True, na=False
    )
    mask_invalido = (pn_extraido != "") & mask_nao_sin & ~pn_extraido.apply(
        lambda x: _validar_pn_chile(x) if x else True
    )
    invalidos_count = mask_invalido.sum()
    if invalidos_count > 0:
        amostras_inv = pn_extraido[mask_invalido].drop_duplicates().head(15).tolist()
        log_callback(f"  🧹 {invalidos_count:,} PNs descartados (texto descritivo)")
        log_callback(f"  📊 Amostras descartadas: {amostras_inv}")
        pn_extraido = pn_extraido.where(~mask_invalido, other="")

    # Construir descrição base das colunas EF, EG, EH
    desc_parts = []
    for idx_col in [idx_ef, idx_eg, idx_eh]:
        if idx_col < len(df.columns):
            desc_parts.append(df.iloc[:, idx_col].astype(str).str.strip()
                              .replace({"nan": "", "None": "", "NaN": ""}))
        else:
            desc_parts.append(pd.Series([""] * len(df), index=df.index))

    descricao_concat = (desc_parts[0] + " " + desc_parts[1] + " " + desc_parts[2]).str.strip()

    # Identificar linhas sem partnumber válido (SIN-CODIGO + descartados + vazios)
    mask_sin = pn_extraido.str.upper().str.contains("SIN-CODIGO|SIN CODIGO|SINCODIGO",
                                                      regex=True, na=False)
    mask_sem_pn = mask_sin | (pn_extraido == "")

    pn_ok = (~mask_sem_pn).sum()
    sin_count = mask_sin.sum()
    log_callback(f"  ✓ {pn_ok:,} partnumbers extraídos diretamente")
    log_callback(f"  ⚠️ {mask_sem_pn.sum():,} linhas sem PN "
                 f"(SIN-CODIGO: {sin_count:,}, descartados: {invalidos_count:,})")

    # Inicializar colunas
    df["PARTNUMBER"] = pn_extraido
    df["DESCRICAO"] = descricao_concat

    # ── ETAPA 1: extrair PNs com regex das colunas EF, EG, EH ──
    if mask_sem_pn.sum() > 0:
        _resolver_sin_codigo_via_regex(df, mask_sem_pn, desc_parts, log_callback)

    # ── ETAPA 2: para os que ainda não têm PN, usar IA ──
    mask_sin_restante = (
        df["PARTNUMBER"].str.upper().str.contains(
            "SIN-CODIGO|SIN CODIGO|SINCODIGO", regex=True, na=False)
        | (mask_sem_pn & (df["PARTNUMBER"].astype(str).str.strip() == ""))
    )
    sin_restante = mask_sin_restante.sum()
    if sin_restante > 0 and api_key:
        log_callback(f"  ⚠️ {sin_restante:,} linhas SIN-CODIGO restantes → consultando IA")
        _resolver_sin_codigo_via_ia(df, mask_sin_restante, descricao_concat,
                                     api_key, log_callback)
    elif sin_restante > 0 and not api_key:
        log_callback(f"  ⚠️ {sin_restante:,} linhas SIN-CODIGO restantes. "
                     "Sem API Key Gemini — ficarão sem partnumber.")

    total_pn = (df["PARTNUMBER"].astype(str).str.strip() != "").sum()
    total_sin_restante = df["PARTNUMBER"].str.upper().str.contains(
        "SIN-CODIGO|SIN CODIGO|SINCODIGO", regex=True, na=False).sum()
    # Limpar linhas que ainda ficaram com SIN-CODIGO → vazio
    if total_sin_restante > 0:
        mask_limpar = df["PARTNUMBER"].str.upper().str.contains(
            "SIN-CODIGO|SIN CODIGO|SINCODIGO", regex=True, na=False)
        df.loc[mask_limpar, "PARTNUMBER"] = ""
        log_callback(f"  🧹 {total_sin_restante:,} linhas SIN-CODIGO limpas → vazio")
    total_pn = (df["PARTNUMBER"].astype(str).str.strip() != "").sum()
    log_callback(f"  ✓ Total final: {total_pn:,} partnumbers "
                 f"({time.perf_counter()-t0:.2f}s)")


# ============================================================
# RESOLUÇÃO VIA REGEX (colunas EF, EG, EH)
# ============================================================

# Regex para códigos alfanuméricos mistos (letras+números)
_REGEX_PN_CHILE = re.compile(
    r"(?:^|\s|[,;|])"
    r"("
    # Formato com traço: VMHPL24-30-SMO, 08CLA-P99-0F0A8
    r"(?:[A-Za-z0-9]{2,10}[\-][A-Za-z0-9][\-A-Za-z0-9]{1,30})"
    r"|"
    # Alfanumérico misto (letras+números juntos), 5-20 chars
    r"(?:(?=[A-Za-z0-9]*[A-Za-z])(?=[A-Za-z0-9]*[0-9])[A-Za-z0-9]{5,20})"
    r"|"
    # Números longos seguidos de letra(s): 61071101715S001A
    r"(?:[0-9]{1,12}[A-Za-z][A-Za-z0-9]{0,30})"
    r"|"
    # Letras seguidas de números: AF9633MB
    r"(?:[A-Za-z]{1,5}[0-9][A-Za-z0-9]{2,30})"
    r"|"
    # Formato com barras: AF6310/FH
    r"(?:[A-Za-z0-9]{2,6}/[A-Za-z0-9/]{2,20})"
    r"|"
    # Formato com pontos numéricos: 510.730.001
    r"(?:[0-9]{1,5}[.][0-9]{2,6}[.\-][0-9]{2,6}(?:\.[0-9]{1,4})?)"
    r")"
    r"(?=\s|$|[,;|])",
    re.IGNORECASE
)

# Marcadores explícitos de PN
_REGEX_MARCADORES_CHILE = re.compile(
    r"(?:"
    r"P/?N[.:]?\s*"
    r"|"
    r"PART\s*(?:NO|NUMBER|#)[.:#]?\s*"
    r"|"
    r"(?:PARTE\s*#|#\s*PARTE?)\s*[:#]?\s*"
    r"|"
    r"(?:NRO\.?\s*(?:AUTO)?PARTE|N[°º]\s*PARTE|PARTE\s*N[°º])\s*[:#]?\s*"
    r"|"
    r"C[OÓ]DIGO\s*[:#]?\s*"
    r"|"
    r"COD[.:]?\s*"
    r")"
    r"([A-Za-z0-9][A-Za-z0-9\-/. ]{2,40}?)"
    r"(?=\s+[A-ZÁÉÍÓÚÑ]{4,}|\s*$|\s*[,;|])",
    re.IGNORECASE
)

# Palavras que não são partnumbers
_PALAVRAS_EXCLUIDAS_CHILE = {
    "S/M", "SM", "S.M", "S/N", "SN", "S.N", "SIN MODELO", "SIN MARCA",
    "SIN-CODIGO", "SINCODIGO",
    "BULTO", "BULTOS", "LIQUIDO", "MOTOR", "PARA", "USO",
    "AUTOMOTRIZ", "UNIDADES", "UNIDAD", "REPUESTO", "REPUESTOS",
    "INDUSTRIAL", "COMERCIAL", "MATERIAL",
    "SELLO", "SELLOS", "ANILLO", "ANILLOS", "EMPAQUE", "JUNTA", "JUNTAS",
    "GASKET", "SEAL", "RING", "KIT", "SET",
    "LITRO", "LITROS", "GALON", "GALONES", "CILINDRO", "TAMBOR",
    "ACEITE", "LUBRICANTE", "FLUIDO", "ADITIVO",
    "FILTRO", "FILTROS", "CORREA", "CORREAS", "MANGUERA", "MANGUERAS",
    "BOMBA", "VALVULA", "VALVULAS",
    "PESO", "NETO", "BRUTO", "TOTAL",
    "VITON", "NITRILO", "EPDM", "ACERO", "METAL",
    "ORIGINAL", "ORIGINALES", "GENUINO",
    "COOLANT", "ANTIFREEZE", "PREMEZCLA",
    "PREMIX", "PREMIXED", "RTU",
    "GALLON", "GALLONS", "DRUM", "PAIL",
    "ULGADAS", "PULGADAS",
}


def _limpar_pn_chile(pn: str) -> str:
    """Limpa PN do Chile: separa prefixo descritivo de código alfanumérico.
    Ex: 'COMPUERTA 115S-FS4/3-RP-NC' → '115S-FS4/3-RP-NC'
    """
    if not pn or pn in ("", "nan", "None", "NaN"):
        return ""
    pn = pn.strip()

    # Se contém espaço, tentar separar prefixo descritivo do código real
    if " " in pn:
        partes = pn.split(None, 1)
        if len(partes) == 2:
            primeira, segunda = partes
            p1_alfa = primeira.replace("-", "").replace(".", "").replace("_", "").isalpha()
            p2_alfa = segunda.replace("-", "").replace(".", "").replace("_", "").isalpha()
            p1_digit = bool(re.search(r'\d', primeira))
            p2_digit = bool(re.search(r'\d', segunda))

            # Primeira é all-alpha (descritiva) e segunda tem dígitos (código)
            # Só retorna se o código extraído tiver 4+ chars (senão é curto demais)
            if p1_alfa and p2_digit and len(segunda.strip()) >= 4:
                return segunda.strip()

            # Segunda é all-alpha (descritiva) e primeira tem dígitos (código)
            if p2_alfa and p1_digit and len(primeira.strip()) >= 4:
                return primeira.strip()

    return pn


def _validar_pn_chile(pn: str) -> bool:
    """Valida se o candidato a PN é realmente um partnumber válido.
    Usa regras lógicas (não hardcoded) para filtrar texto descritivo."""
    if not pn or len(pn) < 3:
        return False
    pn_upper = pn.strip().upper()

    if pn_upper in _PALAVRAS_EXCLUIDAS_CHILE:
        return False

    # ── Regra 1: texto puramente alfabético (inclui separadores comuns) ──
    # Limpa hífens, barras, pontos, espaços e underscores antes de checar
    pn_clean = re.sub(r'[\-/. _]', '', pn_upper)
    if pn_clean.isalpha():
        return False

    # ── Regra 2: número(s) + espaço + palavra(s) alfabética(s) ──
    # Ex: "2 PULGADAS", "40 CONEXION", "16 CONEXION"
    if re.match(r'^\d+\s+[A-Za-z][A-Za-z\s]*$', pn_upper):
        return False

    # ── Regra 3: prefixo alfa curto (1-3 chars) + dígito + espaço + alfa ──
    # Ex: "DE8 PULGADAS"
    if re.match(r'^[A-Za-z]{1,3}\d+\s+[A-Za-z][A-Za-z\s]*$', pn_upper):
        return False

    # ── Regra 4: múltiplas palavras sem nenhum token misto (letra+dígito) ──
    # Se tem espaço, cada token deve ser ou puro-alfa ou puro-numérico
    # → não é código; é texto descritivo
    if ' ' in pn_upper:
        tokens = pn_upper.split()
        tem_token_misto = any(
            bool(re.search(r'[A-Za-z]', t)) and bool(re.search(r'\d', t))
            for t in tokens
        )
        if not tem_token_misto:
            return False

    # ── Regra 5: strings com 4+ palavras são descrições ──
    if len(pn_upper.split()) >= 4:
        return False

    # ── Regra 6: formatos de medida/embalagem ──
    if re.match(r'^\d+\s*[xX]\s*\d+', pn_upper):
        return False
    if re.match(r'^\d+\s*(L|LT|GL|LTR|ML|MM|KG|GAL|GA|GLS|OZ|CC)S?$', pn_upper):
        return False

    # ── Regra 7: números muito curtos (menos de 4 dígitos sozinhos) ──
    if pn_clean.isdigit() and len(pn_clean) < 4:
        return False

    # ── Regra 8: compostos só-alfa com hífens ──
    partes_hifen = pn_upper.split('-')
    if len(partes_hifen) >= 2 and all(p.strip().isalpha() for p in partes_hifen if p.strip()):
        return False

    # ── Regra 9: compostos só-alfa com underscore ──
    partes_under = pn_upper.split('_')
    if len(partes_under) >= 2 and all(p.strip().isalpha() for p in partes_under if p.strip()):
        return False

    return True


def _extrair_pn_de_texto_chile(texto: str) -> str:
    """Extrai partnumber de um texto chileno usando regex."""
    if not texto or texto in ("nan", "None", "NaN", ""):
        return ""

    # Prioridade 1: Marcadores explícitos
    m = _REGEX_MARCADORES_CHILE.search(texto)
    if m:
        pn = m.group(1).strip().rstrip(",. ")
        if _validar_pn_chile(pn):
            return pn

    # Prioridade 2: Código alfanumérico misto
    m = _REGEX_PN_CHILE.search(texto)
    if m:
        pn = m.group(1).strip().rstrip(",. ")
        if _validar_pn_chile(pn):
            return pn

    return ""


def _resolver_sin_codigo_via_regex(df: pd.DataFrame, mask_sin: pd.Series,
                                    desc_parts: list, log_callback) -> None:
    """Extrai partnumber via regex das colunas EF, EG, EH antes de recorrer à IA."""
    log_callback("\n🔍 Extraindo PN via regex das colunas EF, EG, EH (SIN-CODIGO)...")
    t0 = time.perf_counter()

    indices_sin = df.index[mask_sin].tolist()
    total_sin = len(indices_sin)
    resolvidos = 0
    fonte_stats: dict[str, int] = {}

    for idx_row in indices_sin:
        pn_encontrado = ""
        fonte = ""

        # Tentar cada coluna: EF → EG → EH
        for i, letra in enumerate(["EF", "EG", "EH"]):
            texto = str(desc_parts[i].iloc[idx_row]).strip()
            if not texto or texto in ("nan", "None", "NaN", ""):
                continue
            pn = _extrair_pn_de_texto_chile(texto)
            if pn:
                pn_encontrado = pn
                fonte = letra
                break

        # Se não encontrou em colunas individuais, tentar no texto concatenado
        if not pn_encontrado:
            texto_full = " ".join(
                str(desc_parts[i].iloc[idx_row]).strip()
                for i in range(3)
            ).strip()
            if texto_full:
                pn = _extrair_pn_de_texto_chile(texto_full)
                if pn:
                    pn_encontrado = pn
                    fonte = "CONCAT"

        if pn_encontrado:
            df.at[idx_row, "PARTNUMBER"] = pn_encontrado
            resolvidos += 1
            fonte_stats[fonte] = fonte_stats.get(fonte, 0) + 1

    log_callback(f"  ✓ Regex resolveu {resolvidos:,} de {total_sin:,} "
                 f"SIN-CODIGO ({resolvidos/max(total_sin,1)*100:.1f}%)")
    for fonte_nome, qtd in sorted(fonte_stats.items(), key=lambda x: -x[1]):
        log_callback(f"     Coluna {fonte_nome}: {qtd:,}")

    restantes = total_sin - resolvidos
    log_callback(f"  📊 Restantes sem PN (para IA): {restantes:,} "
                 f"({time.perf_counter()-t0:.2f}s)")

    # Amostras de PNs encontrados
    mask_resolvido = mask_sin & ~df["PARTNUMBER"].str.upper().str.contains(
        "SIN-CODIGO|SIN CODIGO|SINCODIGO", regex=True, na=False)
    amostras = df.loc[mask_resolvido, "PARTNUMBER"].drop_duplicates().head(15).tolist()
    if amostras:
        log_callback(f"  📊 Amostras PN regex: {amostras}")


def _resolver_sin_codigo_via_ia(df: pd.DataFrame, mask_sin: pd.Series,
                                 descricao_concat: pd.Series,
                                 api_key: str, log_callback) -> None:
    """Consulta Gemini AI para resolver SIN-CODIGO."""
    from google import genai

    log_callback("\n🤖 Consultando Gemini AI para SIN-CODIGO...")

    desc_to_indices = _deduplicar_descricoes(mask_sin, descricao_concat, df, log_callback)
    unique_descs = list(desc_to_indices.keys())

    client = genai.Client(api_key=api_key)
    cache_resultados = _enviar_lotes_com_retry(
        unique_descs, client, _construir_prompt_partnumber, log_callback)

    resolvidos_pn, resolvidos_desc = _distribuir_resultados(
        df, desc_to_indices, cache_resultados)

    log_callback(f"  ✓ IA resolveu {resolvidos_pn:,} partnumbers e "
                 f"{resolvidos_desc:,} descrições")


def _deduplicar_descricoes(mask: pd.Series, descricao_concat: pd.Series,
                            df: pd.DataFrame, log_callback) -> dict[str, list[int]]:
    """Agrupa linhas com mesma descrição para enviar apenas 1x à IA."""
    indices = df.index[mask].tolist()
    descricoes = descricao_concat.loc[mask].tolist()

    desc_to_indices: dict[str, list[int]] = {}
    for idx_row, desc in zip(indices, descricoes):
        key = str(desc).strip()
        if key and key not in ("", "nan", "None"):
            desc_to_indices.setdefault(key, []).append(idx_row)

    unique_count = len(desc_to_indices)
    economia = 100 - unique_count / max(len(indices), 1) * 100
    log_callback(f"  📊 {len(indices):,} linhas → "
                 f"{unique_count:,} descrições únicas "
                 f"({economia:.0f}% economia)")
    return desc_to_indices


def _enviar_lotes_com_retry(unique_descs: list, client, construir_prompt,
                             log_callback,
                             batch_size: int = 100,
                             max_retries_truncado: int = 6,
                             max_workers: int = 2) -> dict[str, dict]:
    """Envia descrições em lotes ao Gemini com processamento paralelo."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    total_batches = (len(unique_descs) + batch_size - 1) // batch_size
    cache: dict[str, dict] = {}
    log_lock = threading.Lock()

    def safe_log(msg):
        with log_lock:
            log_callback(msg)

    batches = []
    for i in range(total_batches):
        start = i * batch_size
        end = min(start + batch_size, len(unique_descs))
        batches.append((i, unique_descs[start:end]))

    abort_flag = False

    for group_start in range(0, len(batches), max_workers):
        if abort_flag:
            break
        group = batches[group_start:group_start + max_workers]

        with ThreadPoolExecutor(max_workers=len(group)) as executor:
            futures: dict = {}
            for batch_idx, batch_descs in group:
                safe_log(f"  📦 Lote {batch_idx + 1}/{total_batches} "
                         f"({len(batch_descs)} descrições únicas)...")
                local_cache: dict = {}
                future = executor.submit(
                    _processar_lote_com_retry,
                    batch_descs, client, construir_prompt, local_cache,
                    safe_log, max_retries_truncado)
                futures[future] = local_cache
                time.sleep(1)

            for future in as_completed(futures):
                cache.update(futures[future])
                if future.result():
                    abort_flag = True

        if not abort_flag and group_start + max_workers < len(batches):
            time.sleep(2)

    return cache


def _processar_lote_com_retry(pendentes: list, client, construir_prompt,
                               cache: dict, log_callback,
                               max_retries: int) -> bool:
    """Processa um lote, re-enviando itens perdidos por truncamento.
    Retorna True se deve abortar os lotes restantes (erro fatal)."""
    pendentes = list(pendentes)
    tentativa = 0

    while pendentes and tentativa <= max_retries:
        prompt = construir_prompt(pendentes)
        resposta = _enviar_com_retries(client, prompt, log_callback)

        if resposta == _FATAL_ERROR:
            log_callback("  ⚠️ Pulando lotes restantes.")
            return True

        if not resposta:
            break

        resultados = _parsear_resposta_sem_padding(resposta, len(pendentes),
                                                    log_callback)
        for i, desc_key in enumerate(pendentes[:len(resultados)]):
            cache[desc_key] = resultados[i]

        if len(resultados) < len(pendentes):
            perdidos = pendentes[len(resultados):]
            tentativa += 1
            log_callback(f"  🔄 Truncamento: {len(resultados)}/{len(pendentes)} "
                         f"recebidos. Re-enviando {len(perdidos)} "
                         f"(tentativa {tentativa}/{max_retries})...")
            pendentes = perdidos
            time.sleep(1)
        else:
            break

    return False


def _distribuir_resultados(df: pd.DataFrame, desc_to_indices: dict,
                            cache_resultados: dict) -> tuple[int, int]:
    """Distribui resultados da IA para todas as linhas do DataFrame."""
    resolvidos_pn = 0
    resolvidos_desc = 0

    for desc_key, row_indices in desc_to_indices.items():
        resultado = cache_resultados.get(desc_key)
        if not resultado:
            continue
        pn_ia = (resultado.get("partnumber") or "").strip()
        desc_ia = (resultado.get("descricao") or "").strip()
        for idx_row in row_indices:
            if pn_ia and pn_ia.upper() not in ("", "N/A", "DESCONHECIDO", "SIN-CODIGO"):
                df.at[idx_row, "PARTNUMBER"] = pn_ia
                resolvidos_pn += 1
            if desc_ia:
                df.at[idx_row, "DESCRICAO"] = desc_ia
                resolvidos_desc += 1

    return resolvidos_pn, resolvidos_desc


def _construir_prompt_partnumber(descricoes: list) -> str:
    """Prompt compacto para minimizar tokens de entrada e saída."""
    itens = "|".join(desc.replace("|", " ") for desc in descricoes)
    return (
        "Identifique partnumber e descrição de cada item separado por |.\n"
        "Responda JSON puro: [{\"p\":\"PARTNUMBER\",\"d\":\"DESCRIÇÃO\"},...]\n"
        "Se não identificar PN, p=\"\". Descrição limpa em português.\n"
        "Mesma quantidade de itens na saída.\n\n"
        f"{itens}"
    )


# ============================================================
# CONSULTA IA (GEMINI)
# ============================================================

# Sentinel para abortar lotes restantes quando há erro fatal
_FATAL_ERROR = "__FATAL__"


def _enviar_com_retries(client, prompt: str, log_callback,
                        max_tentativas: int = 6) -> str | None:
    """Envia prompt ao Gemini com retries.
    Após esgotar tentativas em erro 429, pergunta ao usuário se deseja
    tentar mais 10 vezes ou prosseguir sem resultado.
    Retorna _FATAL_ERROR se o erro for irrecuperável."""
    resultado = _tentar_envio(client, prompt, log_callback, max_tentativas)
    if resultado is not None:
        return resultado

    # Esgorou tentativas — perguntar ao usuário
    if _perguntar_retry_usuario(log_callback):
        log_callback("  🔄 Usuário optou por tentar novamente (mais 10 tentativas)...")
        return _tentar_envio(client, prompt, log_callback, 10)

    log_callback("  ⏩ Usuário optou por prosseguir sem este lote.")
    return None


def _tentar_envio(client, prompt: str, log_callback,
                  max_tentativas: int) -> str | None:
    """Executa até max_tentativas de envio ao Gemini."""
    for tentativa in range(max_tentativas):
        try:
            response = client.models.generate_content(
                model="gemini-2.0-flash", contents=prompt)
            return response.text.strip()
        except Exception as retry_err:
            err_str = str(retry_err)
            if any(k in err_str for k in ("403", "PERMISSION_DENIED", "leaked",
                                           "401", "UNAUTHENTICATED", "invalid")):
                log_callback(f"  ❌ Erro fatal de autenticação: {retry_err}")
                log_callback("  ❌ Abortando consultas IA — verifique sua API Key.")
                return _FATAL_ERROR
            if tentativa >= max_tentativas - 1:
                log_callback(f"  ⚠️ Falha após {max_tentativas} tentativas: {retry_err}")
                return None
            wait = _calcular_wait(tentativa, retry_err)
            log_callback(f"  ⏳ Tentativa {tentativa+1}/{max_tentativas} falhou, "
                         f"aguardando {wait}s...")
            time.sleep(wait)
    return None


def _perguntar_retry_usuario(log_callback) -> bool:
    """Pergunta ao usuário se deseja tentar novamente via messagebox."""
    from tkinter import messagebox
    log_callback("  ⚠️ Todas as tentativas falharam (rate limit). "
                 "Aguardando decisão do usuário...")
    return messagebox.askretrycancel(
        "Rate Limit Excedido (429)",
        "A API do Gemini está com limite de requisições excedido.\n\n"
        "Tentar novamente? (mais 10 tentativas com backoff)\n"
        "Cancelar = prosseguir sem este lote."
    )


def _calcular_wait(tentativa: int, erro: Exception) -> int:
    """Calcula o tempo de espera entre retries."""
    if "429" in str(erro):
        return min((tentativa + 1) * 15, 90)
    return min((tentativa + 1) * 5, 30)


def _parsear_resposta_sem_padding(resposta_texto: str, expected_count: int,
                                   log_callback) -> list:
    """Extrai itens do JSON da IA sem preencher faltantes com vazio."""
    resposta_texto = _limpar_markdown(resposta_texto)
    resultados = _tentar_parse_json(resposta_texto, log_callback)

    if resultados is None:
        log_callback(f"  ⚠️ Resposta IA inválida (não é lista)")
        log_callback(f"     Resposta: {resposta_texto[:200]}...")
        return []

    normalizados = _normalizar_itens(resultados)

    if len(normalizados) < expected_count:
        log_callback(f"  ⚠️ Truncamento: {len(normalizados)}/{expected_count} "
                     f"itens recuperados")
    return normalizados


def _limpar_markdown(texto: str) -> str:
    """Remove delimitadores de bloco markdown da resposta."""
    texto = re.sub(r"```json\s*", "", texto)
    texto = re.sub(r"```\s*", "", texto)
    return texto.strip()


def _tentar_parse_json(texto: str, log_callback) -> list | None:
    """Tenta parsear JSON; se truncado, recupera itens válidos."""
    try:
        resultado = json.loads(texto)
        if isinstance(resultado, list):
            return resultado
    except json.JSONDecodeError:
        pass
    return _recuperar_json_truncado(texto, log_callback)


def _normalizar_itens(resultados: list) -> list:
    """Normaliza chaves compactas (p/d) para (partnumber/descricao)."""
    normalizados = []
    for item in resultados:
        if not isinstance(item, dict):
            normalizados.append({"partnumber": "", "descricao": ""})
            continue
        normalizados.append({
            "partnumber": item.get("partnumber") or item.get("p") or "",
            "descricao": item.get("descricao") or item.get("d") or "",
        })
    return normalizados


def _recuperar_json_truncado(texto: str, log_callback) -> list | None:
    """Recupera itens válidos de um JSON array truncado."""
    inicio = texto.find("[")
    if inicio == -1:
        return None

    texto = texto[inicio:]
    posicoes = [i for i, c in enumerate(texto) if c == "}"]
    if not posicoes:
        return None

    for pos in reversed(posicoes):
        tentativa = texto[:pos + 1] + "]"
        try:
            resultado = json.loads(tentativa)
            if isinstance(resultado, list) and len(resultado) > 0:
                log_callback(f"  🔧 JSON truncado recuperado: "
                             f"{len(resultado)} itens salvos")
                return resultado
        except json.JSONDecodeError:
            continue
    return None


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
    novas = ["NANDINA", "MARCA", "PARTNUMBER", "USD FOB", "CANTIDAD",
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