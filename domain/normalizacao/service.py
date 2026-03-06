"""
Normalização de MARCA e PARTNUMBER usando Aho-Corasick.
Cada função faz uma única coisa.
"""

import os
import time

from infrastructure.file_io import ler_arquivo, exportar_resultado


# ============================================================
# ORQUESTRADOR
# ============================================================

def processar_normalizacao(data_path: str, regras: list, output_dir: str,
                           formato: str, log_callback, done_callback,
                           pn_conversions: list | None = None) -> None:
    """Orquestra normalização de MARCA e PARTNUMBER."""
    try:
        df = _carregar_arquivo(data_path, log_callback)
        col_marca, col_pn = _encontrar_colunas(df, regras, log_callback)
        if col_marca is None and regras:
            log_callback("❌ Coluna MARCA não encontrada no arquivo de dados!")
            done_callback(False)
            return
        if col_pn is None:
            log_callback("❌ Coluna PARTNUMBER não encontrada no arquivo de dados!")
            done_callback(False)
            return

        _limpar_colunas(df, col_marca, col_pn)
        total_changes = _aplicar_todas_regras(
            df, regras, pn_conversions, col_marca, col_pn, log_callback
        )
        log_callback(f"\n📊 Total de alterações: {total_changes:,}")

        base_name = os.path.splitext(os.path.basename(data_path))[0]
        exportar_resultado(df, output_dir, base_name + "_normalizado",
                           formato, log_callback)
        done_callback(True)
    except Exception as e:
        log_callback(f"❌ ERRO: {e}")
        done_callback(False)


# ============================================================
# CARREGAMENTO
# ============================================================

def _carregar_arquivo(data_path: str, log_callback):
    """Carrega o arquivo de dados como texto."""
    log_callback("📂 Carregando arquivo de dados...")
    t0 = time.perf_counter()
    df = ler_arquivo(data_path, dtype=str)
    log_callback(f"  ✓ {len(df):,} linhas × {len(df.columns)} colunas "
                 f"({time.perf_counter()-t0:.1f}s)")
    return df


# ============================================================
# DESCOBERTA DE COLUNAS
# ============================================================

def _encontrar_colunas(df, regras, log_callback) -> tuple:
    """Identifica colunas MARCA e PARTNUMBER no DataFrame."""
    col_marca = None
    col_pn = None
    for col in df.columns:
        upper = col.upper().strip()
        if upper == "MARCA":
            col_marca = col
        elif upper in ("PARTNUMBER", "PART NUMBER", "PART_NUMBER", "PN"):
            col_pn = col

    if col_marca:
        log_callback(f"  Coluna MARCA: '{col_marca}'")
    if col_pn:
        log_callback(f"  Coluna PARTNUMBER: '{col_pn}'")
    return col_marca, col_pn


# ============================================================
# LIMPEZA
# ============================================================

def _limpar_colunas(df, col_marca, col_pn) -> None:
    """Normaliza MARCA e PARTNUMBER: upper + strip + remove 'NAN'."""
    for c in (col_marca, col_pn):
        if c is None:
            continue
        s = df[c].fillna("").astype(str).str.upper().str.strip()
        s[s == "NAN"] = ""
        df[c] = s


# ============================================================
# APLICAÇÃO DE REGRAS (dispatcher)
# ============================================================

def _aplicar_todas_regras(df, regras, pn_conversions,
                          col_marca, col_pn, log_callback) -> int:
    """Aplica regras de normalização e conversões de PN."""
    total = 0
    if regras:
        total += _aplicar_regras_pn(df, regras, col_marca, col_pn, log_callback)
        total += _aplicar_regras_marca_aho_corasick(
            df, regras, col_marca, log_callback
        )
    if pn_conversions:
        total += _aplicar_conversoes_pn(df, pn_conversions, col_pn, log_callback)
    return total


# ============================================================
# REGRAS PN (dict lookup O(1))
# ============================================================

def _aplicar_regras_pn(df, regras, col_marca, col_pn, log_callback) -> int:
    """Atualiza MARCA com base no PARTNUMBER via dict lookup."""
    pn_rules = [(r["marca"].upper().strip(), r["partnumber"].upper().strip())
                for r in regras if r.get("partnumber", "").strip()]
    if not pn_rules:
        return 0

    log_callback(f"\n🔧 Aplicando {len(pn_rules)} regra(s) de PARTNUMBER...")
    t0 = time.perf_counter()
    pn_map = {pn: marca for marca, pn in pn_rules}
    nova_marca = df[col_pn].map(pn_map)
    mask = nova_marca.notna()
    changes = int(mask.sum())
    if changes > 0:
        df.loc[mask, col_marca] = nova_marca[mask]
    log_callback(f"  ✓ {changes:,} linha(s) atualizadas via PN "
                 f"({time.perf_counter()-t0:.2f}s)")
    _log_detalhes_pn(df, pn_map, pn_rules, col_pn, log_callback)
    return changes


def _log_detalhes_pn(df, pn_map, pn_rules, col_pn, log_callback) -> None:
    """Loga detalhes de cada regra PN aplicada."""
    pn_counts = df.loc[df[col_pn].isin(pn_map), col_pn].value_counts()
    for marca_regra, pn_regra in pn_rules:
        c = int(pn_counts.get(pn_regra, 0))
        if c > 0:
            log_callback(f"    PN '{pn_regra}' → '{marca_regra}': {c:,}")
        else:
            log_callback(f"    ⚠ PN '{pn_regra}' não encontrado")


# ============================================================
# REGRAS MARCA (Aho-Corasick O(n))
# ============================================================

def _aplicar_regras_marca_aho_corasick(df, regras, col_marca,
                                       log_callback) -> int:
    """Normaliza MARCA via Aho-Corasick multi-pattern matching."""
    import ahocorasick

    marca_rules = [r["marca"].upper().strip()
                   for r in regras if not r.get("partnumber", "").strip()]
    if not marca_rules:
        return 0

    log_callback(f"\n🏷️ Aplicando {len(marca_rules)} regra(s) de MARCA (Aho-Corasick)...")
    t0 = time.perf_counter()

    automaton = _construir_automaton(marca_rules)
    remap = _encontrar_matches(automaton, df[col_marca].unique())
    total = _aplicar_remap(df, col_marca, remap, marca_rules, log_callback)

    log_callback(f"  ✓ Matching concluído ({time.perf_counter()-t0:.2f}s)")
    return total


def _construir_automaton(marca_rules: list):
    """Constrói o autômato Aho-Corasick a partir das regras de marca."""
    import ahocorasick
    A = ahocorasick.Automaton()
    for idx, marca_regra in enumerate(marca_rules):
        A.add_word(marca_regra, (idx, marca_regra))
    A.make_automaton()
    return A


def _encontrar_matches(automaton, valores_unicos) -> dict:
    """Busca o melhor match para cada valor único de marca."""
    remap = {}
    for valor in valores_unicos:
        if not valor:
            continue
        best_match = None
        best_len = 0
        for _, (_, matched_marca) in automaton.iter(valor):
            if valor == matched_marca:
                best_match = None
                break
            if len(matched_marca) > best_len:
                best_match = matched_marca
                best_len = len(matched_marca)
        if best_match is not None:
            remap[valor] = best_match
    return remap


def _aplicar_remap(df, col_marca, remap: dict, marca_rules: list,
                   log_callback) -> int:
    """Aplica o dicionário de remapeamento ao DataFrame."""
    total = 0
    if not remap:
        return total

    mapped = df[col_marca].map(remap)
    mask = mapped.notna()
    df.loc[mask, col_marca] = mapped[mask]

    vc = mapped.dropna().value_counts()
    for regra_val, cnt in vc.items():
        total += int(cnt)

    for marca_regra in marca_rules:
        c = int(vc.get(marca_regra, 0))
        if c > 0:
            log_callback(f"    '{marca_regra}': {c:,} linha(s) normalizada(s)")
        else:
            log_callback(f"    ⚠ '{marca_regra}': nenhum match (ou já normalizada)")

    return total


# ============================================================
# CONVERSÃO PN → PN
# ============================================================

def _aplicar_conversoes_pn(df, pn_conversions, col_pn, log_callback) -> int:
    """Converte PARTNUMBER antigo para novo via dict lookup."""
    log_callback(f"\n🔄 Aplicando {len(pn_conversions)} conversão(ões) de PARTNUMBER...")
    t0 = time.perf_counter()
    conv_map = {c["de"]: c["para"] for c in pn_conversions}
    novo_pn = df[col_pn].map(conv_map)
    mask_conv = novo_pn.notna()
    conv_changes = int(mask_conv.sum())
    if conv_changes > 0:
        df.loc[mask_conv, col_pn] = novo_pn[mask_conv]
    log_callback(f"  ✓ {conv_changes:,} partnumber(s) convertido(s) "
                 f"({time.perf_counter()-t0:.2f}s)")
    _log_detalhes_conversoes(pn_conversions, novo_pn, log_callback)
    return conv_changes


def _log_detalhes_conversoes(pn_conversions, novo_pn, log_callback) -> None:
    """Loga detalhes de cada conversão de PN aplicada."""
    for conv in pn_conversions:
        mapped_count = int((novo_pn == conv["para"]).sum()) if conv["para"] else 0
        if mapped_count > 0:
            log_callback(f"    '{conv['de']}' → '{conv['para']}': {mapped_count:,}")
        else:
            log_callback(f"    ⚠ PN '{conv['de']}' não encontrado no arquivo")
