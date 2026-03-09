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
    """Consulta Gemini AI para resolver SIN-CODIGO.
    Otimizado: deduplica descrições idênticas antes de enviar à IA,
    usa lotes grandes e prompt compacto para minimizar tokens.
    """
    from google import genai

    log_callback("\n🤖 Consultando Gemini AI para SIN-CODIGO...")

    # ── Deduplicação ──────────────────────────────────────────
    # Agrupa linhas com mesma descrição para enviar apenas 1x à IA
    indices_sin = df.index[mask_sin].tolist()
    descricoes = descricao_concat.loc[mask_sin].tolist()

    # Mapa: descrição → lista de índices no df
    desc_to_indices: dict[str, list[int]] = {}
    for idx_row, desc in zip(indices_sin, descricoes):
        key = str(desc).strip()
        if key and key not in ("", "nan", "None"):
            desc_to_indices.setdefault(key, []).append(idx_row)

    unique_descs = list(desc_to_indices.keys())
    log_callback(f"  📊 {len(indices_sin):,} linhas SIN-CODIGO → "
                 f"{len(unique_descs):,} descrições únicas "
                 f"({100 - len(unique_descs)/max(len(indices_sin),1)*100:.0f}% economia)")

    # ── Envio em lotes ────────────────────────────────────────
    BATCH_SIZE = 150  # Lotes maiores = menos chamadas
    total_batches = (len(unique_descs) + BATCH_SIZE - 1) // BATCH_SIZE
    client = genai.Client(api_key=api_key)

    resolvidos_pn = 0
    resolvidos_desc = 0
    cache_resultados: dict[str, dict] = {}

    for batch_idx in range(total_batches):
        start = batch_idx * BATCH_SIZE
        end = min(start + BATCH_SIZE, len(unique_descs))
        batch_descs = unique_descs[start:end]

        log_callback(f"  📦 Lote {batch_idx+1}/{total_batches} "
                     f"({len(batch_descs)} descrições únicas)...")

        prompt = _construir_prompt_partnumber(batch_descs)
        resposta = _enviar_com_retries(client, prompt, log_callback)

        # Abortar se erro fatal (API key inválida/vazada)
        if resposta == _FATAL_ERROR:
            log_callback(f"  ⚠️ Pulando {total_batches - batch_idx - 1} lotes restantes.")
            break

        if resposta:
            resultados = _parsear_resposta_partnumber(resposta, len(batch_descs),
                                                      log_callback)
            for i, desc_key in enumerate(batch_descs):
                if i < len(resultados):
                    cache_resultados[desc_key] = resultados[i]

        # Rate limiting leve (Gemini 2.0 Flash suporta alto throughput)
        if batch_idx < total_batches - 1:
            time.sleep(1)

    # ── Distribuir resultados para todas as linhas ────────────
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

    log_callback(f"  ✓ IA resolveu {resolvidos_pn:,} partnumbers e "
                 f"{resolvidos_desc:,} descrições")


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
    """Extrai lista de partnumbers/descrições do JSON retornado pela IA.
    Aceita chaves compactas (p/d) ou completas (partnumber/descricao)."""
    try:
        # Limpar markdown
        resposta_texto = re.sub(r"```json\s*", "", resposta_texto)
        resposta_texto = re.sub(r"```\s*", "", resposta_texto)
        resposta_texto = resposta_texto.strip()

        resultados = json.loads(resposta_texto)
        if not isinstance(resultados, list):
            log_callback(f"  ⚠️ Resposta IA não é uma lista: {type(resultados)}")
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
        return normalizados
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
