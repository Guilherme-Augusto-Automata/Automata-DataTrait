"""
Tratamento de dados — Peru.
Extração de NANDINA, IDENTIFICADOR, CANTIDAD, USD FOB, DESCRIÇÃO COMERCIAL,
MARCA, PARTNUMBER (com IA Gemini), IMPORTADOR e DATA.
"""

import os
import re
import json
import time
from collections import Counter

import numpy as np
import pandas as pd

from config.settings import STR_DTYPE
from infrastructure.file_io import ler_arquivo, exportar_resultado, col_idx


# ============================================================
# ORQUESTRADOR
# ============================================================

def processar_peru(input_path: str, output_dir: str, formato: str,
                   api_key: str, log_callback, done_callback) -> None:
    """Orquestra o processamento do Peru."""
    try:
        df = _carregar_arquivo(input_path, log_callback)

        _extrair_nandina(df, log_callback)
        _extrair_identificador(df, log_callback)
        _copiar_cantidad(df, log_callback)
        _extrair_fob_usd(df, log_callback)
        _extrair_importador(df, log_callback)
        _extrair_descricao_marca_partnumber(df, api_key, log_callback)
        _extrair_data(df, log_callback)

        _debug_colunas_finais(df, log_callback)

        base_name = os.path.splitext(os.path.basename(input_path))[0]
        exportar_resultado(df, output_dir, base_name + "_tratado", formato,
                           log_callback)
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
    """Carrega o arquivo principal do Peru (CSV | ou XLSX)."""
    log_callback("📂 Carregando arquivo do Peru...")
    t0 = time.perf_counter()
    df = ler_arquivo(input_path, dtype=str)
    log_callback(f"  ✓ {len(df):,} linhas × {len(df.columns)} colunas "
                 f"({time.perf_counter()-t0:.1f}s)")
    log_callback(f"  Colunas: {list(df.columns[:10])}...")
    return df


# ============================================================
# UTILIDADES
# ============================================================

def _encontrar_coluna(df: pd.DataFrame, nomes_possiveis: list) -> str | None:
    """Busca uma coluna pelo nome (case-insensitive, trim)."""
    colunas_lower = {c.lower().strip(): c for c in df.columns}
    for nome in nomes_possiveis:
        if nome.lower().strip() in colunas_lower:
            return colunas_lower[nome.lower().strip()]
    return None


# ============================================================
# EXTRAÇÃO DE CAMPOS
# ============================================================

def _extrair_nandina(df: pd.DataFrame, log_callback) -> None:
    """Extrai NANDINA da coluna 'Partida', removendo todos os pontos."""
    log_callback("\n📋 Extraindo NANDINA...")
    t0 = time.perf_counter()

    col = _encontrar_coluna(df, ["Partida", "PARTIDA", "partida",
                                  "Sub Partida", "SubPartida"])
    if col is not None:
        raw = df[col].astype(str).str.strip()
        # Remove todos os pontos, mantém apenas dígitos
        nandina = raw.str.replace(".", "", regex=False)
        nandina = nandina.replace({"nan": "", "None": "", "NaN": ""})
        df["NANDINA"] = nandina

        nao_vazio = (df["NANDINA"] != "").sum()
        amostras = df["NANDINA"][df["NANDINA"] != ""].head(5).tolist()
        log_callback(f"  ✓ NANDINA extraído da coluna '{col}' — "
                     f"{nao_vazio:,} valores não-vazios "
                     f"({time.perf_counter()-t0:.2f}s)")
        log_callback(f"  📊 Amostras NANDINA: {amostras}")
    else:
        log_callback("  ⚠️ Coluna 'Partida' não encontrada.")
        colunas_possiveis = [c for c in df.columns
                             if "PART" in c.upper() or "ARANC" in c.upper()
                             or "NANDINA" in c.upper()]
        log_callback(f"  🔍 Colunas parecidas: {colunas_possiveis}")
        df["NANDINA"] = ""


def _extrair_identificador(df: pd.DataFrame, log_callback) -> None:
    """Extrai IDENTIFICADOR da coluna 'Declaracion' removendo traços.
    Ex: 118-2025-000986 → 1182025000986"""
    log_callback("\n🆔 Extraindo IDENTIFICADOR...")
    t0 = time.perf_counter()

    col = _encontrar_coluna(df, ["Declaracion", "DECLARACION", "declaracion",
                                  "Declaración", "DECLARACIÓN"])
    if col is None:
        # Fallback: primeira coluna
        col = df.columns[0]
        log_callback(f"  ⚠️ Coluna 'Declaracion' não encontrada, "
                     f"usando primeira coluna '{col}'")

    raw = df[col].astype(str).str.strip()
    # Remove todos os traços
    identificador = raw.str.replace("-", "", regex=False)
    identificador = identificador.replace({"nan": "", "None": "", "NaN": ""})
    df["IDENTIFICADOR"] = identificador

    nao_vazio = (df["IDENTIFICADOR"] != "").sum()
    amostras_antes = raw[~raw.isin(["nan", "None", ""])].head(3).tolist()
    amostras_depois = df["IDENTIFICADOR"][df["IDENTIFICADOR"] != ""].head(3).tolist()
    log_callback(f"  ✓ {nao_vazio:,} identificadores extraídos da coluna "
                 f"'{col}' ({time.perf_counter()-t0:.2f}s)")
    log_callback(f"  📊 Antes: {amostras_antes}")
    log_callback(f"  📊 Depois: {amostras_depois}")


def _copiar_cantidad(df: pd.DataFrame, log_callback) -> None:
    """Copia coluna T (Cantidad) para CANTIDAD."""
    log_callback("\n📊 Copiando CANTIDAD (coluna T)...")

    col = _encontrar_coluna(df, ["Cantidad", "CANTIDAD", "cantidad"])
    if col is None:
        idx_t = col_idx("T")
        if idx_t < len(df.columns):
            col = df.columns[idx_t]
            log_callback(f"  🔍 'Cantidad' não encontrada por nome, "
                         f"usando coluna T: '{col}'")
        else:
            log_callback("  ⚠️ Coluna T não existe.")
            df["CANTIDAD"] = ""
            return

    df["CANTIDAD"] = df[col]
    nao_vazio = df["CANTIDAD"].notna().sum()
    log_callback(f"  ✓ {nao_vazio:,} valores copiados da coluna '{col}'")


def _extrair_fob_usd(df: pd.DataFrame, log_callback) -> None:
    """Copia coluna Y (Fob) para USD FOB."""
    log_callback("\n💵 Extraindo USD FOB (coluna Y / Fob)...")
    t0 = time.perf_counter()

    col = _encontrar_coluna(df, ["Fob", "FOB", "fob", "FOB USD",
                                  "Fob USD", "Fob Dolar"])
    if col is None:
        idx_y = col_idx("Y")
        if idx_y < len(df.columns):
            col = df.columns[idx_y]
            log_callback(f"  🔍 'Fob' não encontrada por nome, "
                         f"usando coluna Y: '{col}'")
        else:
            log_callback("  ⚠️ Coluna Y não existe.")
            df["USD FOB"] = np.nan
            return

    df["USD FOB"] = pd.to_numeric(df[col], errors="coerce")
    fob_ok = df["USD FOB"].notna().sum()
    log_callback(f"  ✓ {fob_ok:,} valores FOB extraídos da coluna '{col}' "
                 f"({time.perf_counter()-t0:.2f}s)")


def _extrair_importador(df: pd.DataFrame, log_callback) -> None:
    """Extrai IMPORTADOR da coluna B removendo números iniciais.
    Ex: '4-20348687191 WURTH PERU S.A.C.' → 'WURTH PERU S.A.C.'"""
    log_callback("\n🏢 Extraindo IMPORTADOR (coluna B)...")
    t0 = time.perf_counter()

    idx_b = col_idx("B")
    if idx_b >= len(df.columns):
        log_callback("  ⚠️ Coluna B não existe.")
        df["IMPORTADOR"] = ""
        return

    col_name = df.columns[idx_b]
    raw = df.iloc[:, idx_b].astype(str).str.strip()
    raw = raw.replace({"nan": "", "None": "", "NaN": ""})

    # Remove o padrão de números iniciais: "4-20348687191 " → ""
    # Padrão: dígitos, possível traço, mais dígitos, seguido de espaço(s)
    importador = raw.str.replace(r"^[\d][\d\-]*\s+", "", regex=True).str.strip()

    df["IMPORTADOR"] = importador
    nao_vazio = (df["IMPORTADOR"] != "").sum()
    amostras = (df["IMPORTADOR"][df["IMPORTADOR"] != ""]
                .drop_duplicates().head(5).tolist())
    log_callback(f"  ✓ {nao_vazio:,} importadores extraídos da coluna "
                 f"'{col_name}' ({time.perf_counter()-t0:.2f}s)")
    log_callback(f"  📊 Amostras: {amostras}")


def _extrair_data(df: pd.DataFrame, log_callback) -> None:
    """Extrai DATA buscando coluna de data por nome."""
    log_callback("\n📅 Extraindo DATA...")

    col = _encontrar_coluna(df, [
        "Fecha", "FECHA", "fecha",
        "Fecha Numeracion", "Fecha_Num", "FechaNumeracion",
        "Fecha de Numeración", "Fecha Llegada", "F.Llegada",
        "Fecha Ingreso", "Data", "DATE",
    ])

    if col is not None:
        raw = df[col].astype(str).str.strip()
        raw = raw.replace({"nan": "", "None": "", "NaN": ""})
        df["DATA"] = raw
        nao_vazio = (df["DATA"] != "").sum()
        amostras = df["DATA"][df["DATA"] != ""].head(5).tolist()
        log_callback(f"  ✓ {nao_vazio:,} datas extraídas da coluna '{col}'")
        log_callback(f"  📊 Amostras: {amostras}")
    else:
        log_callback("  ⚠️ Nenhuma coluna de data encontrada.")
        colunas_data = [c for c in df.columns
                        if "FECHA" in c.upper() or "DATA" in c.upper()
                        or "DATE" in c.upper()]
        log_callback(f"  🔍 Colunas com 'FECHA/DATA/DATE': {colunas_data}")
        df["DATA"] = ""


# ============================================================
# DESCRIÇÃO + MARCA + PARTNUMBER (coluna O)
# ============================================================

def _extrair_descricao_marca_partnumber(df: pd.DataFrame, api_key: str,
                                        log_callback) -> None:
    """
    Extrai DESCRIÇÃO COMERCIAL, MARCA e PARTNUMBER da coluna O.

    Formato esperado na coluna O:
        "DESCRICAO,MARCA,PARTNUMBER"
        Ex: "LIQUIDO ANTICONGELANTE,PRESTONE,AF9900M8"

    - DESCRIÇÃO COMERCIAL = texto antes da primeira vírgula
    - MARCA = texto entre primeira e segunda vírgula
    - PARTNUMBER = texto após a segunda vírgula

    Se não houver PN na coluna O, concatena O+P+Q+R+S e usa IA Gemini.
    """
    log_callback("\n🔧 Extraindo DESCRIÇÃO, MARCA e PARTNUMBER (coluna O)...")
    t0 = time.perf_counter()

    idx_o = col_idx("O")  # 14
    if idx_o >= len(df.columns):
        log_callback(f"  ⚠️ Coluna O (índice {idx_o}) não existe. "
                     f"Total: {len(df.columns)}")
        df["DESCRICAO"] = ""
        df["MARCA"] = ""
        df["PARTNUMBER"] = ""
        return

    col_name = df.columns[idx_o]
    raw = df.iloc[:, idx_o].astype(str).str.strip()
    raw = raw.replace({"nan": "", "None": "", "NaN": ""})

    log_callback(f"  🔍 Usando coluna '{col_name}' (índice {idx_o})")
    amostras_raw = raw[raw != ""].head(5).tolist()
    log_callback(f"  📊 Amostras coluna O:")
    for i, a in enumerate(amostras_raw):
        log_callback(f"     [{i}] {a}")

    # ── Split por vírgula (max 3 partes) ──
    partes = raw.str.split(",", n=2)

    # DESCRIÇÃO COMERCIAL = primeira parte (antes da 1ª vírgula)
    descricao = partes.str[0].fillna("").str.strip()
    df["DESCRICAO"] = descricao

    # MARCA = segunda parte (entre 1ª e 2ª vírgula)
    marca = partes.str[1].fillna("").str.strip()
    marca = marca.replace({"nan": "", "None": "", "NaN": ""})
    df["MARCA"] = marca

    # PARTNUMBER = terceira parte (após a 2ª vírgula)
    pn_direto = partes.str[2].fillna("").str.strip()
    pn_direto = pn_direto.replace({"nan": "", "None": "", "NaN": ""})

    # Limpar PNs: remover vírgulas no final (ex: "AF12050M," → "AF12050M")
    pn_direto = pn_direto.str.rstrip(",").str.strip()

    # Tratar "S/M" (Sin Modelo) como vazio — não é partnumber
    pn_direto = pn_direto.where(
        ~pn_direto.str.upper().isin(["S/M", "S.M", "SM", "S / M", "SIN MODELO",
                                      "SIN MARCA", "S/N", "S.N", "SN"]),
        other=""
    )

    # ── Estatísticas ──
    has_desc = (descricao != "").sum()
    has_marca = (marca != "").sum()
    has_pn = (pn_direto != "").sum()
    no_pn = (pn_direto == "").sum()

    log_callback(f"\n  ✓ {has_desc:,} descrições extraídas")
    log_callback(f"  ✓ {has_marca:,} marcas extraídas")
    log_callback(f"  ✓ {has_pn:,} partnumbers extraídos diretamente da coluna O")
    log_callback(f"  ⚠️ {no_pn:,} linhas SEM partnumber — tentando regex + IA")

    # Amostras de PNs diretos
    pn_amostras = pn_direto[pn_direto != ""].head(10).tolist()
    log_callback(f"  📊 Amostras PN direto: {pn_amostras}")

    # Amostras de marcas
    marca_amostras = marca[marca != ""].drop_duplicates().head(10).tolist()
    log_callback(f"  📊 Amostras MARCA: {marca_amostras}")

    # Inicializar PARTNUMBER com os extraídos diretamente
    df["PARTNUMBER"] = pn_direto

    # ── ETAPA 1: extrair PNs com regex das colunas P, Q, R, S ──
    mask_sem_pn = (df["PARTNUMBER"] == "")
    if mask_sem_pn.sum() > 0:
        _resolver_pn_via_regex(df, mask_sem_pn, log_callback)

    # ── ETAPA 2: para os que ainda não têm PN, usar IA ──
    mask_sem_pn = (df["PARTNUMBER"] == "")
    if mask_sem_pn.sum() > 0 and api_key:
        _resolver_partnumber_via_ia(df, mask_sem_pn, api_key, log_callback)
    elif mask_sem_pn.sum() > 0 and not api_key:
        log_callback("  ⚠️ Sem API Key Gemini. Linhas sem PN ficarão vazias.")

    total_pn = (df["PARTNUMBER"].astype(str).str.strip() != "").sum()
    log_callback(f"\n  ✓ Total final: {total_pn:,} partnumbers "
                 f"({time.perf_counter()-t0:.2f}s)")


# ============================================================
# RESOLUÇÃO VIA REGEX (colunas O, P, Q, R, S)
# ============================================================

# Palavras que NÃO são partnumbers — falsos positivos comuns
_PALAVRAS_EXCLUIDAS = {
    # Marcadores genéricos
    "S/M", "SM", "S.M", "S/N", "SN", "S.N", "SIN MODELO", "SIN MARCA",
    # Palavras espanholas comuns
    "BULTO", "BULTOS", "LIQUIDO", "MOTOR", "REFRIGERANTE", "PARA", "USO",
    "AUTOMOTRIZ", "UNIDADES", "UNIDAD", "CAUCHO", "JEBE", "REPUESTO",
    "REPUESTOS", "INDUSTRIAL", "COMERCIAL", "VEHICULO", "VEHICULOS",
    "MATERIAL", "CONTENIDO", "PRESENTACION", "PREPARACION", "PREPARACIÓN",
    "ANTICONGELANTE", "CONCENTRADO", "SELLO", "SELLOS", "ANILLO", "ANILLOS",
    "EMPAQUE", "EMPAQUES", "EMPAQUETADURA", "EMPAQUETADURAS", "JUNTA",
    "JUNTAS", "ORING", "RETEN", "RETENES", "GASKET", "SEAL", "RING",
    "PART", "PARTE", "PARTES", "NUMERO", "CODIGO", "CÓDIGO", "MODELO",
    "MARCA", "TIPO", "COLOR", "ROJO", "VERDE", "AZUL", "AMARILLO",
    "NEGRO", "NARANJA", "BLANCO", "GRIS",
    "LITRO", "LITROS", "GALON", "GALONES", "CILINDRO", "TAMBOR",
    "BALDE", "CAJA", "DRUM", "PAIL", "BARREL",
    "ACEITE", "LUBRICANTE", "LUBRICANTES", "FLUIDO", "ADITIVO",
    "DIESEL", "GASOLINA", "COMBUSTIBLE",
    "AUTO", "CAMION", "CAMIONES", "MOTO", "MOTOCICLETA",
    "DELANTERO", "TRASERO", "DERECHO", "IZQUIERDO", "SUPERIOR", "INFERIOR",
    "ORIGINAL", "ORIGINALES", "GENUINO", "GENUINOS",
    "SISTEMA", "COMPONENTE", "COMPONENTES", "ACCESORIO", "ACCESORIOS",
    "MAQUINARIA", "MAQUINARIAS", "EQUIPO", "EQUIPOS",
    "PESO", "NETO", "BRUTO", "TOTAL",
    "OTROS", "OTRO", "OTRAS", "OTRA",
    "VITON", "NITRILO", "EPDM", "ACERO", "METAL", "FIERRO",
    "BOMBA", "VALVULA", "VALVULAS", "FILTRO", "FILTROS",
    "CORREA", "CORREAS", "MANGUERA", "MANGUERAS",
    "CUBIERTA", "TAPA", "CARTER", "CABEZA", "PISTON",
    "RADIADOR", "COMPRESOR", "COMPRESORES",
    "CREDITO", "PLAZO", "DIAS", "DIA",
    "PRS", "MAX", "ASN", "HTP",
    "COOLANT", "ANTIFREEZE", "COOLELF", "FREEZE", "PREMEZCLA",
    "SIZE", "DRUM", "TOTE", "TOTES", "ENVASE", "ENVASES",
    # Inglés descriptivo (falsos positivos de la IA)
    "GASKET", "SEAL", "FOAM", "RING", "KIT", "SET",
    "BACKUP", "BUFFER", "DUST", "FRONT", "REAR", "COVER",
    "WATER", "PUMP", "VALVE",
    # Productos/materiales químicos
    "ETILENGLICOL", "MONOETILENGLICOL", "GLICOLETILENO",
    "MONOETILENO", "GLICOL", "PROPILENGLICOL",
    "SILICONA", "INHIBIDORES", "ANTICORROSIVA",
    # Formatos de embalagem
    "GALLON", "GALLONS", "GALON", "GALONES",
    "LITERS", "LITROS", "LITRO",
    "BOTELLA", "BOTELLAS", "BIDON", "BIDONES",
    # Palabras descriptivas adicionales
    "MEZCLA", "PREPARADA", "PREPARADO",
    "ESPECIALIDAD", "DIFERIDO", "CONCENTRADO",
    "PROTECCION", "PROTECCIÓN",
    "PREMIX", "PREMIXED", "RTU",
}

# Regex para detectar marcadores explícitos de PN seguidos del código
_REGEX_MARCADORES = re.compile(
    r"(?:"
    # P/N. / P/N: / P/N / PN: / PN.
    r"P/?N[.:]?\s*"
    r"|"
    # PART NO. / PART NO: / PART NUMBER : / PART #
    r"PART\s*(?:NO|NUMBER|#)[.:#]?\s*"
    r"|"
    # PARTE # / #PARTE: / #PART
    r"(?:PARTE\s*#|#\s*PARTE?)\s*[:#]?\s*"
    r"|"
    # NRO. AUTOPARTE: / NRO PARTE: / N° PARTE:
    r"(?:NRO\.?\s*(?:AUTO)?PARTE|N[°º]\s*PARTE)\s*[:#]?\s*"
    r"|"
    # CODIGO SEGUN FACTURA / CODIGO: / CÓDIGO: / COD: / COD.
    r"C[OÓ]DIGO\s*(?:SEG[UÚ]N\s+FACTURA)?\s*[:#]?\s*"
    r"|"
    r"COD[.:]?\s*(?:Producto\s*[:#]?\s*)?"
    r")"
    # Captura: código alfanumérico (mín 3 chars) com -, /, . opcionais
    r"([A-Za-z0-9][A-Za-z0-9\-/. ]{2,40}?)"
    # Termina em: fim de string, espaço seguido de palavra longa, ou delimitadores
    r"(?=\s+[A-ZÁÉÍÓÚÑ]{4,}|\s*$|\s*[,;|]|\s+(?:PARA|USO|DE|EN|REP|LIQ|PRE|ANT|REF|LUB))",
    re.IGNORECASE
)

# Códigos alfanuméricos standalone no FINAL do texto
# Captura: código com letras+números (ou só números 5+ dígitos) no final
_REGEX_CODIGO_FINAL = re.compile(
    r"(?:^|\s)"
    # Grupo 1: alfanumérico misto (letras+números, com -, /, . opcionais)
    r"("
    # Opção A: começa com letra(s) seguida de números  (ex: AF9633MB, BG00814782)
    r"(?:[A-Za-z]{1,5}[0-9][A-Za-z0-9\-/\.]{2,30})"
    r"|"
    # Opção B: começa com números seguidos de letra(s)  (ex: 76367251BR, 9W6688)
    r"(?:[0-9]{1,5}[A-Za-z][A-Za-z0-9\-/\.]{1,30})"
    r"|"
    # Opção C: formato NUM-NUM ou NUM-ALFANUM  (ex: 07000-F2130, 413-10484-511-0)
    r"(?:[0-9]{2,6}[\-][A-Za-z0-9\-/]{3,30})"
    r"|"
    # Opção D: número puro 5+ dígitos (ex: 334887100, 2292028)
    r"(?:[0-9]{5,13})"
    r"|"
    # Opção E: formato com pontos (ex: 00105.3158, 18307.003.01)
    r"(?:[0-9]{2,6}\.[0-9]{2,6}(?:\.[0-9]{2,4})?)"
    r"|"
    # Opção F: formato com barras (ex: BB3Z/6051/B/, VC/13/G/)
    r"(?:[A-Za-z0-9]{2,6}/[A-Za-z0-9/]{3,20})"
    r")"
    r"\s*$",
    re.IGNORECASE
)

# Códigos alfanuméricos standalone em QUALQUER posição
# Mais restritivo: exige mix de letras+números
_REGEX_CODIGO_MISTO = re.compile(
    r"(?:^|\s)"
    r"("
    # Mix alfanumérico com traço (ex: 08CLA-P99-0F0A8, AS3209-220, 11394-K2K-D01)
    r"(?:[A-Za-z0-9]{2,6}[\-][A-Za-z0-9][\-A-Za-z0-9]{2,25})"
    r"|"
    # Letras+números juntos mín 5 chars (ex: AF9633MB, MZ341017EX, BG00814782)
    r"(?:(?=[A-Za-z0-9]*[A-Za-z])(?=[A-Za-z0-9]*[0-9])[A-Za-z0-9]{5,15})"
    r"|"
    # Formato com barras alfanuméricas (ex: AF6310/FH, AFC11100/1F, VC/13/G/)
    r"(?:[A-Za-z]{2,5}/[A-Za-z0-9/]{2,20})"
    r"|"
    # Formato com barras começando com letras+dígito (ex: AF6310/FH)
    r"(?:[A-Za-z]{2,5}[0-9][A-Za-z0-9]*/[A-Za-z0-9/]{1,15})"
    r"|"
    # Formato com pontos numéricos (ex: 6.363-006.0, 510.730.001)
    r"(?:[0-9]{1,5}[.][0-9]{2,6}[.\-][0-9]{2,6}(?:\.[0-9]{1,4})?)"
    r")"
    r"(?=\s|$|[,;|\-])",
    re.IGNORECASE
)


def _validar_pn(pn: str) -> bool:
    """Valida se o candidato a PN é realmente um partnumber válido."""
    if not pn or len(pn) < 3:
        return False
    pn_upper = pn.strip().upper()

    # Rejeitar palavras da lista de exclusão
    if pn_upper in _PALAVRAS_EXCLUIDAS:
        return False

    # Rejeitar se é apenas letras comuns (palavras)
    pn_clean = pn_upper.replace("-", "").replace("/", "").replace(".", "").replace(" ", "")
    if pn_clean.isalpha() and len(pn_clean) <= 12:
        # Letras puras são geralmente palavras, não PNs
        return False

    # Rejeitar padrões descritivos ingleses (falsos positivos da IA)
    # ex: "GASKET,WTR TRF CON", "SEAL,O RING", "FOAM,6.35 X ..."
    if "," in pn and any(w in pn_upper for w in ["GASKET", "SEAL", "FOAM", "RING"]):
        return False

    # ── Rejeitar padrões de embalagem/medida ──
    # Formatos NxN (ex: 3x1gl, 12X1/4GL, 1x5GL, 6X1GA)
    if re.match(r"^\d+\s*[xX]\s*\d+", pn_upper):
        return False

    # Medidas com unidades completas (ex: 4Liters, 5Gallons, 20LTR)
    if re.match(r"^\d+\s*(?:LITERS?|LITROS?|GALLONS?|GALONES?)$", pn_upper):
        return False

    # Medidas curtas (ex: "50%", "1L", "5GL", "20 LTR", "208L")
    if re.match(r"^\d+[.,]?\d*\s*(%|L|GL|LTR|ML|KG|GAL|GA|GLS|GR|OZ|CC)S?$",
                pn_upper):
        return False

    # Formatos de apresentação (ex: "5 GALLONS", "1 GALLON X 4", "BALDE 19 LITROS")
    if re.match(r"^\d+\s+(?:GALLONS?|GALONES?|LITR[OA]S?|LITRES?|UNITS?)$",
                pn_upper):
        return False

    # Rejeitar strings com "Premix", "Coolant", "RTU" que são nomes de produto
    if re.match(r".*(?:PREMIX|COOLANT|RTU|PREMEZCLA|ANTIFREEZE).*$", pn_upper):
        return False

    # Rejeitar formatos "N GA" ou "N GL" com espaço (ex: "5 GA", "55 GA")
    if re.match(r"^\d+\s+(?:GA|GL|GLS|GAL|LT|LTR|L)S?$", pn_upper):
        return False

    # Rejeitar números muito curtos (menos de 4 dígitos sozinhos)
    if pn_clean.isdigit() and len(pn_clean) < 4:
        return False

    # Rejeitar palavras comuns que passaram (verificação extra)
    for palavra in ["MATERIAL", "SILICONA", "ETILENGLICOL", "MONOETILENGLICOL",
                    "GLICOLETILENO", "ANTICONGELA", "ANTICONGELANTES",
                    "MEZCLA", "PREPARADA", "ESPECIALIDAD", "DIFERIDO",
                    "INHIBIDORES"]:
        if pn_upper == palavra:
            return False

    return True


def _extrair_pn_de_texto(texto: str) -> str:
    """Extrai partnumber de um texto usando hierarquia de regex.
    Retorna o PN encontrado ou string vazia.
    """
    if not texto or texto in ("nan", "None", "NaN", ""):
        return ""

    # ── PRIORIDADE 1: Marcadores explícitos (altíssima confiança) ──
    m = _REGEX_MARCADORES.search(texto)
    if m:
        pn = m.group(1).strip().rstrip(",. ")
        if _validar_pn(pn):
            return pn

    # ── PRIORIDADE 2: Código alfanumérico no FINAL do texto ──
    m = _REGEX_CODIGO_FINAL.search(texto)
    if m:
        pn = m.group(1).strip().rstrip(",. ")
        if _validar_pn(pn):
            return pn

    # ── PRIORIDADE 3: Código alfanumérico misto em qualquer posição ──
    m = _REGEX_CODIGO_MISTO.search(texto)
    if m:
        pn = m.group(1).strip().rstrip(",. ")
        if _validar_pn(pn):
            return pn

    return ""


def _resolver_pn_via_regex(df: pd.DataFrame, mask_sem_pn: pd.Series,
                            log_callback) -> None:
    """Extrai partnumber via regex multi-padrão antes de recorrer à IA.

    Estratégia baseada na análise de 10,679 PNs detectados pela IA:
    - Prioridade 1: Marcadores explícitos (P/N, PART NO, CODIGO, COD, etc.)
    - Prioridade 2: Código alfanumérico no final do texto
    - Prioridade 3: Código alfanumérico misto em qualquer posição

    Processa cada coluna (S, R, Q, P, O) individualmente, priorizando col S
    onde a maioria dos PNs foi encontrada pela IA.
    """
    log_callback("\n🔍 Extraindo PN via regex multi-padrão (colunas O, P, Q, R, S)...")
    t0 = time.perf_counter()

    idx_o = col_idx("O")
    idx_p = col_idx("P")
    idx_q = col_idx("Q")
    idx_r = col_idx("R")
    idx_s = col_idx("S")

    # Preparar textos de cada coluna
    colunas_texto: dict[str, pd.Series] = {}
    col_nomes = []
    for idx_col, letra in [(idx_s, "S"), (idx_r, "R"), (idx_q, "Q"),
                            (idx_p, "P"), (idx_o, "O")]:
        if idx_col < len(df.columns):
            col_nomes.append(f"{letra}='{df.columns[idx_col]}'")
            s = df.iloc[:, idx_col].astype(str).str.strip()
            s = s.replace({"nan": "", "None": "", "NaN": ""})
            colunas_texto[letra] = s
        else:
            colunas_texto[letra] = pd.Series([""] * len(df), index=df.index)

    log_callback(f"  🔍 Colunas (ordem de busca): {', '.join(col_nomes)}")

    # Concatenar todas as colunas para fallback
    texto_concat = colunas_texto["O"]
    for letra in ["P", "Q", "R", "S"]:
        texto_concat = texto_concat + " | " + colunas_texto[letra]

    # ── Aplicar regex linha a linha para linhas sem PN ──
    indices_sem_pn = df.index[mask_sem_pn].tolist()
    total_sem_pn = len(indices_sem_pn)

    pn_resultados = pd.Series("", index=df.index)
    pn_fonte = pd.Series("", index=df.index)  # rastrear onde encontrou

    stats = {"marcador": 0, "final": 0, "misto": 0, "nenhum": 0}
    ordem_colunas = ["S", "R", "Q", "P", "O"]

    for idx_row in indices_sem_pn:
        pn_encontrado = ""
        fonte = ""

        # Tentar cada coluna na ordem S → R → Q → P → O
        for letra in ordem_colunas:
            texto = colunas_texto[letra].iloc[idx_row] if isinstance(
                colunas_texto[letra].iloc[idx_row], str
            ) else str(colunas_texto[letra].iloc[idx_row])

            if not texto or texto in ("nan", "None", "NaN", ""):
                continue

            pn = _extrair_pn_de_texto(texto)
            if pn:
                pn_encontrado = pn
                fonte = letra
                break

        # Se não encontrou em colunas individuais, tentar no texto concatenado
        if not pn_encontrado:
            texto_full = texto_concat.iloc[idx_row]
            if texto_full and texto_full not in ("nan", "None", "NaN", ""):
                pn_encontrado = _extrair_pn_de_texto(str(texto_full))
                if pn_encontrado:
                    fonte = "CONCAT"

        if pn_encontrado:
            pn_resultados.iloc[idx_row] = pn_encontrado
            pn_fonte.iloc[idx_row] = fonte
            # Classificar o tipo de match
            texto_usado = (colunas_texto.get(fonte, texto_concat).iloc[idx_row]
                           if fonte != "CONCAT"
                           else texto_concat.iloc[idx_row])
            if _REGEX_MARCADORES.search(str(texto_usado)):
                stats["marcador"] += 1
            elif _REGEX_CODIGO_FINAL.search(str(texto_usado)):
                stats["final"] += 1
            else:
                stats["misto"] += 1
        else:
            stats["nenhum"] += 1

    # ── Aplicar resultados ──
    mask_resolvido = mask_sem_pn & (pn_resultados != "")
    resolvidos = mask_resolvido.sum()

    if resolvidos > 0:
        df.loc[mask_resolvido, "PARTNUMBER"] = pn_resultados[mask_resolvido]

    # ── Estatísticas por fonte ──
    fontes_count = pn_fonte[mask_resolvido].value_counts()
    log_callback(f"\n  ✓ Regex resolveu {resolvidos:,} de {total_sem_pn:,} "
                 f"({resolvidos/max(total_sem_pn,1)*100:.1f}%)")
    log_callback(f"  📊 Por tipo de padrão:")
    log_callback(f"     Marcador explícito (P/N, CODIGO, etc.): {stats['marcador']:,}")
    log_callback(f"     Código no final do texto: {stats['final']:,}")
    log_callback(f"     Código misto (qualquer posição): {stats['misto']:,}")
    log_callback(f"     Não encontrado: {stats['nenhum']:,}")
    log_callback(f"  📊 Por coluna de origem:")
    for fonte_nome, qtd in fontes_count.items():
        log_callback(f"     Coluna {fonte_nome}: {qtd:,}")

    # Amostras
    amostras = pn_resultados[mask_resolvido].drop_duplicates().head(20).tolist()
    log_callback(f"  📊 Amostras PN regex: {amostras}")

    restantes = total_sem_pn - resolvidos
    log_callback(f"  📊 Restantes sem PN (para IA): {restantes:,} "
                 f"({time.perf_counter()-t0:.2f}s)")


# ============================================================
# RESOLUÇÃO VIA IA (GEMINI)
# ============================================================

def _resolver_partnumber_via_ia(df: pd.DataFrame, mask_sem_pn: pd.Series,
                                 api_key: str, log_callback) -> None:
    """
    Para linhas sem partnumber:
    1. Concatena colunas O + P + Q + R + S
    2. Envia ao Gemini AI para detectar SOMENTE o partnumber
    3. Loga DETALHADAMENTE todos os padrões detectados
    """
    from google import genai

    log_callback("\n🤖 Consultando Gemini AI para detectar PARTNUMBER...")

    idx_o = col_idx("O")
    idx_p = col_idx("P")
    idx_q = col_idx("Q")
    idx_r = col_idx("R")
    idx_s = col_idx("S")

    # ── Concatenar colunas O, P, Q, R, S ──
    partes_concat = []
    col_names_usadas = []
    for idx_col, letra in [(idx_o, "O"), (idx_p, "P"), (idx_q, "Q"),
                            (idx_r, "R"), (idx_s, "S")]:
        if idx_col < len(df.columns):
            col_name = df.columns[idx_col]
            col_names_usadas.append(f"{letra}='{col_name}'")
            parte = df.iloc[:, idx_col].astype(str).str.strip()
            parte = parte.replace({"nan": "", "None": "", "NaN": ""})
            partes_concat.append(parte)
        else:
            partes_concat.append(pd.Series([""] * len(df), index=df.index))

    log_callback(f"  🔍 Colunas concatenadas: {', '.join(col_names_usadas)}")

    # Texto completo = O + " " + P + " " + Q + " " + R + " " + S
    # Filtrar partes vazias para não poluir com espaços
    texto_completo = partes_concat[0]
    for p in partes_concat[1:]:
        # Só concatena se a parte não for vazia
        texto_completo = texto_completo.where(
            p == "", texto_completo + " " + p
        )
    texto_completo = texto_completo.str.strip()

    # ── Log detalhado das linhas que vão para IA ──
    indices_sem_pn = df.index[mask_sem_pn].tolist()
    textos_para_ia = texto_completo.loc[mask_sem_pn].tolist()

    log_callback(f"\n  📊 ANÁLISE DETALHADA — {len(indices_sem_pn):,} linhas sem PN:")
    log_callback(f"  ─────────────────────────────────────────────")
    n_amostras = min(20, len(indices_sem_pn))
    for i in range(n_amostras):
        idx_row = indices_sem_pn[i]
        texto = str(textos_para_ia[i])
        log_callback(f"  [{i}] Linha {idx_row}: {texto[:150]}")
        # Mostrar cada coluna individual
        for j, (idx_col, letra) in enumerate([(idx_o, "O"), (idx_p, "P"),
                                               (idx_q, "Q"), (idx_r, "R"),
                                               (idx_s, "S")]):
            if idx_col < len(df.columns):
                val = str(df.iloc[idx_row, idx_col]).strip()
                if val and val not in ("nan", "None", "NaN", ""):
                    log_callback(f"       Col {letra} ({df.columns[idx_col]}): "
                                 f"{val[:100]}")
    if len(indices_sem_pn) > n_amostras:
        log_callback(f"  ... e mais {len(indices_sem_pn) - n_amostras:,} linhas")
    log_callback(f"  ─────────────────────────────────────────────")

    # ── Deduplicação ──
    desc_to_indices: dict[str, list[int]] = {}
    for idx_row, texto in zip(indices_sem_pn, textos_para_ia):
        key = str(texto).strip()
        if key and key not in ("", "nan", "None"):
            desc_to_indices.setdefault(key, []).append(idx_row)

    unique_descs = list(desc_to_indices.keys())
    economia = 100 - len(unique_descs) / max(len(indices_sem_pn), 1) * 100
    log_callback(f"\n  📊 {len(indices_sem_pn):,} linhas → "
                 f"{len(unique_descs):,} descrições únicas "
                 f"({economia:.0f}% economia)")

    # ── Envio em lotes (sem limite — regex resolve a maioria) ──
    BATCH_SIZE = 50  # Lotes menores para evitar truncamento de JSON
    total_batches = (len(unique_descs) + BATCH_SIZE - 1) // BATCH_SIZE
    client = genai.Client(api_key=api_key)

    resolvidos_pn = 0
    cache_resultados: dict[str, dict] = {}

    # Contadores de padrões para log detalhado
    padroes_detectados: Counter = Counter()
    pn_exemplos_por_padrao: dict[str, list] = {}
    todos_pn_detectados: list[dict] = []

    for batch_idx in range(total_batches):
        start = batch_idx * BATCH_SIZE
        end = min(start + BATCH_SIZE, len(unique_descs))
        batch_descs = unique_descs[start:end]

        log_callback(f"  📦 Lote {batch_idx + 1}/{total_batches} "
                     f"({len(batch_descs)} descrições únicas)...")

        prompt = _construir_prompt_partnumber_peru(batch_descs)
        resposta = _enviar_com_retries(client, prompt, log_callback)

        # Abortar se erro fatal (API key inválida)
        if resposta == _FATAL_ERROR:
            log_callback(f"  ⚠️ Pulando {total_batches - batch_idx - 1} "
                         f"lotes restantes.")
            break

        if resposta:
            resultados = _parsear_resposta_partnumber(resposta,
                                                      len(batch_descs),
                                                      log_callback)
            for i, desc_key in enumerate(batch_descs):
                if i < len(resultados):
                    cache_resultados[desc_key] = resultados[i]

        # Rate limiting
        if batch_idx < total_batches - 1:
            time.sleep(1)

    # ── Distribuir resultados e logar padrões ──
    log_callback(f"\n  📊 ANÁLISE DE PADRÕES DETECTADOS PELA IA:")
    log_callback(f"  ═══════════════════════════════════════════════")

    for desc_key, row_indices in desc_to_indices.items():
        resultado = cache_resultados.get(desc_key)
        if not resultado:
            continue
        pn_ia = (resultado.get("partnumber") or "").strip()

        if pn_ia and pn_ia.upper() not in ("", "N/A", "DESCONHECIDO",
                                            "SEM PARTNUMBER",
                                            "NAO IDENTIFICADO",
                                            "NÃO IDENTIFICADO",
                                            "NO APLICA"):
            # Classificar padrão do PN
            padrao = _classificar_padrao_pn(pn_ia)
            padroes_detectados[padrao] += len(row_indices)
            if padrao not in pn_exemplos_por_padrao:
                pn_exemplos_por_padrao[padrao] = []
            if len(pn_exemplos_por_padrao[padrao]) < 5:
                pn_exemplos_por_padrao[padrao].append({
                    "pn": pn_ia,
                    "input": desc_key[:100],
                })

            # Guardar para log detalhado
            todos_pn_detectados.append({
                "pn": pn_ia,
                "input": desc_key[:100],
                "padrao": padrao,
                "linhas": len(row_indices),
            })

            for idx_row in row_indices:
                df.at[idx_row, "PARTNUMBER"] = pn_ia
                resolvidos_pn += 1

    # ── Log individual de TODOS os PNs detectados ──
    log_callback(f"\n  📋 TODOS OS PARTNUMBERS DETECTADOS ({len(todos_pn_detectados)}):")
    log_callback(f"  ─────────────────────────────────────────────")
    for i, item in enumerate(todos_pn_detectados):
        log_callback(f"  [{i:3d}] PN='{item['pn']}' | "
                     f"Padrão={item['padrao']} | "
                     f"{item['linhas']}x | "
                     f"← '{item['input']}'")
    log_callback(f"  ─────────────────────────────────────────────")

    # ── Sumário de padrões ──
    log_callback(f"\n  📊 SUMÁRIO DE PADRÕES DE PARTNUMBER:")
    log_callback(f"  ─────────────────────────────────────────────")
    for padrao, count in padroes_detectados.most_common():
        log_callback(f"  [{padrao}] → {count:,} ocorrências")
        exemplos = pn_exemplos_por_padrao.get(padrao, [])
        for ex in exemplos:
            log_callback(f"     ex: '{ex['pn']}' ← '{ex['input']}'")
    log_callback(f"  ─────────────────────────────────────────────")

    nao_resolvidos = len(indices_sem_pn) - resolvidos_pn
    log_callback(f"\n  ✓ IA resolveu {resolvidos_pn:,} partnumbers")
    if nao_resolvidos > 0:
        log_callback(f"  ⚠️ {nao_resolvidos:,} linhas permaneceram sem PN")

    # ── ANÁLISE FINAL: onde o PN foi encontrado ──
    log_callback(f"\n  ═══════════════════════════════════════════════")
    log_callback(f"  📊 ANÁLISE DE LOCALIZAÇÃO DOS PNs DETECTADOS:")
    log_callback(f"  ═══════════════════════════════════════════════")

    # Classificar em qual coluna/posição a IA encontrou o PN
    localizacao_counter: Counter = Counter()
    localizacao_exemplos: dict[str, list] = {}

    for item in todos_pn_detectados:
        pn = item["pn"].upper()
        inp = item["input"]

        # Tentar achar onde o PN aparece no texto de entrada
        local = _identificar_localizacao_pn(pn, inp)
        localizacao_counter[local] += item["linhas"]
        if local not in localizacao_exemplos:
            localizacao_exemplos[local] = []
        if len(localizacao_exemplos[local]) < 8:
            localizacao_exemplos[local].append({
                "pn": item["pn"],
                "input": inp,
            })

    for local, count in localizacao_counter.most_common():
        log_callback(f"\n  📍 [{local}] → {count:,} ocorrências")
        for ex in localizacao_exemplos.get(local, []):
            log_callback(f"     PN='{ex['pn']}'")
            log_callback(f"     ← '{ex['input']}'")

    # ── PEDIR À IA para sugerir padrões regex ──
    if todos_pn_detectados and api_key:
        _solicitar_analise_padroes(client, todos_pn_detectados, log_callback)


def _identificar_localizacao_pn(pn: str, texto_input: str) -> str:
    """Identifica onde no texto de entrada o PN aparece."""
    pn_upper = pn.upper()
    inp_upper = texto_input.upper()

    if pn_upper not in inp_upper:
        return "PN NÃO ENCONTRADO NO TEXTO (inferido pela IA)"

    # Verificar se está na parte da coluna S (último segmento)
    # O texto é "col_O col_P col_Q col_R col_S"
    # Heurística: se aparece nos últimos 30% do texto
    pos = inp_upper.find(pn_upper)
    ratio = pos / max(len(inp_upper), 1)

    if ratio > 0.7:
        return "FINAL DO TEXTO (provavelmente col S/Desc otros)"
    elif ratio > 0.5:
        return "MEIO-FIM (provavelmente col R/Desc de uso ou col S)"
    elif ratio > 0.3:
        return "MEIO (provavelmente col Q/Desc Mat Const ou col R)"
    elif ratio > 0.1:
        return "INÍCIO-MEIO (provavelmente col P/Desc Presentacion)"
    else:
        return "INÍCIO (provavelmente col O/Desc Comercial)"


def _solicitar_analise_padroes(client, todos_pn_detectados: list,
                                log_callback) -> None:
    """Envia os padrões encontrados à IA para ela sugerir regex Python."""
    log_callback(f"\n  ═══════════════════════════════════════════════")
    log_callback(f"  🧠 SOLICITANDO ANÁLISE DE PADRÕES À IA...")
    log_callback(f"  ═══════════════════════════════════════════════")

    # Montar exemplos para a IA (max 200 para não estourar contexto)
    exemplos = []
    for item in todos_pn_detectados[:200]:
        exemplos.append(f"INPUT: {item['input']}\nPN: {item['pn']}")

    prompt = (
        "Você é um engenheiro Python especialista em regex e extração de dados.\n"
        "Abaixo estão exemplos reais de textos de importação (INPUT) e os "
        "partnumbers (PN) que foram detectados neles.\n\n"
        "Cada INPUT é a concatenação de 5 colunas de uma planilha de importação:\n"
        "  - Col O: Desc.Comercial (formato: DESCRIÇÃO, MARCA, PN ou DESCRIÇÃO, MARCA, S/M)\n"
        "  - Col P: Desc.Presentación\n"
        "  - Col Q: Desc. Mat. Const (pode conter 'Nro.Autoparte: XXXX')\n"
        "  - Col R: Desc. de uso\n"
        "  - Col S: Desc otros (frequentemente contém códigos de peça)\n\n"
        "TAREFA: Analise TODOS os padrões e me liste:\n\n"
        "1. PADRÕES REGEX PYTHON para extrair PN automaticamente SEM IA.\n"
        "   Para cada padrão, informe:\n"
        "   - A regex Python (re.search/re.findall)\n"
        "   - Em qual coluna (O, P, Q, R ou S) buscar\n"
        "   - Exemplos de PNs que seriam capturados\n"
        "   - Prioridade (ordem de aplicação)\n\n"
        "2. PADRÕES DE TEXTO que indicam que NÃO há partnumber\n"
        "   (ex: quando todas as colunas são genéricas sem código)\n\n"
        "3. ESTRATÉGIA RECOMENDADA para implementar em Python puro,\n"
        "   com a ordem de prioridade dos padrões.\n\n"
        "4. ESTIMATIVA de cobertura: que % dos casos abaixo seria resolvido\n"
        "   com regex vs. os que realmente precisam de IA.\n\n"
        "Seja EXTREMAMENTE detalhado e prático. Liste TODOS os padrões "
        "distintos que você identificar.\n\n"
        "═══ EXEMPLOS ═══\n\n"
        + "\n\n".join(exemplos)
    )

    try:
        resposta = _enviar_com_retries(client, prompt, log_callback)
        if resposta and resposta != _FATAL_ERROR:
            log_callback(f"\n  ════════════════════════════════════════════")
            log_callback(f"  🧠 ANÁLISE DE PADRÕES PELA IA:")
            log_callback(f"  ════════════════════════════════════════════")
            # Logar a resposta inteira, linha a linha
            for line in resposta.split("\n"):
                log_callback(f"  {line}")
            log_callback(f"  ════════════════════════════════════════════")
        else:
            log_callback("  ⚠️ Não foi possível obter análise de padrões da IA")
    except Exception as e:
        log_callback(f"  ⚠️ Erro ao solicitar análise: {e}")


def _classificar_padrao_pn(pn: str) -> str:
    """Classifica o padrão do partnumber para análise futura.
    Facilita a construção de regex para substituir a IA."""
    pn = pn.strip().upper()

    if re.match(r"^\d{3,4}-\d{3,5}$", pn):
        return "NUM-NUM (ex: 238-8649)"
    if re.match(r"^\d+-\d+-\d+", pn):
        return "NUM-NUM-NUM (multi-traço)"
    if re.match(r"^[A-Z]{1,3}\d+[A-Z]*\d*$", pn):
        return "ALFA+NUM (ex: AF9900M8)"
    if re.match(r"^[A-Z]{1,5}-\d+", pn):
        return "ALFA-NUM (ex: PN-12345)"
    if re.match(r"^[A-Z]{1,5}\d+-[A-Z0-9]+", pn):
        return "ALFANUM-ALFANUM (ex: RE57394)"
    if re.match(r"^\d+[A-Z]+\d*$", pn):
        return "NUM+ALFA (ex: 123ABC)"
    if re.match(r"^\d+$", pn):
        return "SOMENTE NUMEROS"
    if re.match(r"^[A-Z]+$", pn):
        return "SOMENTE LETRAS"
    if re.match(r"^[A-Z0-9]+-[A-Z0-9]+$", pn):
        return "ALFANUM-ALFANUM"
    if re.match(r"^[A-Z0-9]+\s+[A-Z0-9]+$", pn):
        return "ALFANUM ALFANUM (com espaço)"
    if "/" in pn:
        return "COM BARRA"
    if "." in pn:
        return "COM PONTO"
    return f"OUTRO ({pn[:20]})"


# ============================================================
# PROMPT + CONSULTA IA (GEMINI)
# ============================================================

_FATAL_ERROR = "__FATAL__"


def _construir_prompt_partnumber_peru(descricoes: list) -> str:
    """Prompt focado SOMENTE em detectar partnumber."""
    itens = "|".join(desc.replace("|", " ") for desc in descricoes)
    return (
        "Identifique SOMENTE o part number (código de peça) em cada item separado por |.\n"
        "Partnumbers são códigos alfanuméricos como: 238-8649, AF9900M8, HLLCP-001, "
        "1R-0750, P552100, RE57394.\n"
        "NÃO confunda com quantidades, pesos, valores ou códigos aduaneiros.\n"
        "NÃO inclua descrições, apenas o partnumber.\n\n"
        "Responda JSON puro: [{\"p\":\"PARTNUMBER\"},...]\n"
        "Se não encontrar PN, p=\"\". Mesma quantidade de itens.\n\n"
        f"{itens}"
    )


def _enviar_com_retries(client, prompt: str, log_callback,
                         max_tentativas: int = 6) -> str | None:
    """Envia prompt ao Gemini com retries.
    Retorna _FATAL_ERROR para erros irrecuperáveis."""
    for tentativa in range(max_tentativas):
        try:
            response = client.models.generate_content(
                model="gemini-2.0-flash", contents=prompt,
                config={"max_output_tokens": 8192})
            return response.text.strip()
        except Exception as retry_err:
            err_str = str(retry_err)
            # Erros fatais — não adianta tentar de novo
            if any(k in err_str for k in ("403", "PERMISSION_DENIED", "leaked",
                                           "401", "UNAUTHENTICATED", "invalid")):
                log_callback(f"  ❌ Erro fatal de autenticação: {retry_err}")
                log_callback("  ❌ Abortando consultas IA — "
                             "verifique sua API Key.")
                return _FATAL_ERROR
            if tentativa >= max_tentativas - 1:
                log_callback(f"  ⚠️ Falha final: {retry_err}")
                return None
            wait = _calcular_wait(tentativa, retry_err)
            log_callback(f"  ⏳ Tentativa {tentativa + 1}/{max_tentativas} "
                         f"falhou, aguardando {wait}s...")
            time.sleep(wait)
    return None


def _calcular_wait(tentativa: int, erro: Exception) -> int:
    """Calcula o tempo de espera entre retries."""
    if "429" in str(erro):
        return min((tentativa + 1) * 15, 90)
    return (tentativa + 1) * 5


def _parsear_resposta_partnumber(resposta_texto: str, expected_count: int,
                                  log_callback) -> list:
    """Extrai lista de partnumbers do JSON retornado pela IA.
    Aceita chaves compactas (p/d) ou completas (partnumber/descricao).
    Recupera JSON truncado (quando IA atinge limite de tokens)."""
    # Limpar markdown
    resposta_texto = re.sub(r"```json\s*", "", resposta_texto)
    resposta_texto = re.sub(r"```\s*", "", resposta_texto)
    resposta_texto = resposta_texto.strip()

    resultados = None

    # ── Tentativa 1: parse normal ──
    try:
        resultados = json.loads(resposta_texto)
    except json.JSONDecodeError:
        pass

    # ── Tentativa 2: JSON truncado — recuperar itens válidos ──
    if resultados is None:
        resultados = _recuperar_json_truncado(resposta_texto, log_callback)

    if resultados is None or not isinstance(resultados, list):
        log_callback(f"  ⚠️ Resposta IA inválida (não é lista)")
        log_callback(f"     Resposta: {resposta_texto[:200]}...")
        return []

    # Normalizar chaves compactas p→partnumber, d→descricao
    normalizados = []
    for item in resultados:
        if not isinstance(item, dict):
            normalizados.append({"partnumber": "", "descricao": ""})
            continue
        normalizados.append({
            "partnumber": item.get("partnumber") or item.get("p") or "",
            "descricao": item.get("descricao") or item.get("d") or "",
        })

    # Se recebemos menos itens que o esperado, preencher o resto com vazio
    if len(normalizados) < expected_count:
        faltam = expected_count - len(normalizados)
        log_callback(f"  ⚠️ JSON truncado: recuperados {len(normalizados)}/{expected_count} "
                     f"itens ({faltam} perdidos)")
        normalizados.extend([{"partnumber": "", "descricao": ""}] * faltam)

    return normalizados


def _recuperar_json_truncado(texto: str, log_callback) -> list | None:
    """Recupera o máximo de itens de um JSON array truncado.
    Ex: [{"p":"A"},{"p":"B"},{"p":"C   ← cortou aqui
    Resultado: [{"p":"A"},{"p":"B"}]  ← salva os 2 completos
    """
    # Garantir que começa com [
    inicio = texto.find("[")
    if inicio == -1:
        return None

    texto = texto[inicio:]

    # Encontrar o último item completo: último '}' seguido de ',' ou ']'
    # Estratégia: truncar no último '}' completo e fechar o array
    ultimo_fecha = texto.rfind("}")
    if ultimo_fecha == -1:
        return None

    # Cortar até o último } e fechar o array
    tentativa = texto[:ultimo_fecha + 1] + "]"
    try:
        resultado = json.loads(tentativa)
        if isinstance(resultado, list):
            log_callback(f"  🔧 JSON truncado recuperado: {len(resultado)} itens salvos")
            return resultado
    except json.JSONDecodeError:
        pass

    # Estratégia 2: tentar removendo o último item (pode estar incompleto)
    penultimo_fecha = texto.rfind("}", 0, ultimo_fecha)
    if penultimo_fecha == -1:
        return None

    tentativa2 = texto[:penultimo_fecha + 1] + "]"
    try:
        resultado = json.loads(tentativa2)
        if isinstance(resultado, list):
            log_callback(f"  🔧 JSON truncado recuperado (modo 2): {len(resultado)} itens salvos")
            return resultado
    except json.JSONDecodeError:
        pass

    # Estratégia 3: busca binária — encontrar o maior prefixo parseável
    # Itera de trás para frente nos '}'
    posicoes = [i for i, c in enumerate(texto) if c == "}"]
    for pos in reversed(posicoes[:-2]):  # já tentamos os 2 últimos
        tentativa3 = texto[:pos + 1] + "]"
        try:
            resultado = json.loads(tentativa3)
            if isinstance(resultado, list) and len(resultado) > 0:
                log_callback(f"  🔧 JSON truncado recuperado (modo 3): {len(resultado)} itens salvos")
                return resultado
        except json.JSONDecodeError:
            continue

    return None


# ============================================================
# DEBUG
# ============================================================

def _debug_colunas_finais(df: pd.DataFrame, log_callback) -> None:
    """Loga o estado das novas colunas adicionadas."""
    log_callback("\n🔍 DEBUG — Novas colunas Peru:")
    novas = ["NANDINA", "IDENTIFICADOR", "CANTIDAD", "USD FOB",
             "DESCRICAO", "DATA", "MARCA", "PARTNUMBER", "IMPORTADOR"]
    for col in novas:
        if col in df.columns:
            nn = int(df[col].notna().sum())
            # Contar valores reais (não-vazios)
            if df[col].dtype == object:
                real = int((df[col].astype(str).str.strip() != "").sum())
            else:
                real = nn
            fvi = df[col].first_valid_index()
            sample = df[col].loc[fvi] if fvi is not None else "VAZIO"
            log_callback(f"  {col}: {real:,} vals | "
                         f"ex: {repr(sample)[:60]}")
        else:
            log_callback(f"  {col}: NÃO ENCONTRADA")
    log_callback(f"  Total: {len(df.columns)} colunas, {len(df):,} linhas")
    log_callback("  --- FIM DEBUG ---")
