"""Step 2 — Explosão de PartNumbers: verifica e explode linhas multi-PN."""

import ast
import math
import re
from typing import Any, List, Optional

import numpy as np
import pandas as pd

PN = "PARTNUMBER_CONSIDERAR"
CE = "CANTIDADE_CONSIDERAR"
DCMS = "cantidade_dcms"
NUM_FORM = "numero_formulario"
MARCA = "marca"
DESC = "descripcion_mercancia"
EXPLODED_FLAG = "explosion_added"


# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def _ensure_list(v: Any) -> List[str]:
    """Normaliza vários formatos para lista de strings."""
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x).strip() for x in v if x is not None and str(x).strip() != ""]
    if isinstance(v, (tuple, set)):
        return [str(x).strip() for x in list(v) if x is not None and str(x).strip() != ""]
    if isinstance(v, (int, float)) and not (isinstance(v, float) and np.isnan(v)):
        return [str(int(v))]
    s = str(v).strip()
    if not s or s.lower() in ("nan", "none", "na"):
        return []
    if (s.startswith("[") and s.endswith("]")) or (s.startswith("(") and s.endswith(")")):
        try:
            parsed = ast.literal_eval(s)
            if isinstance(parsed, (list, tuple)):
                return [str(x).strip() for x in parsed if x is not None and str(x).strip() != ""]
        except Exception:
            pass
    if " | " in s:
        parts = [p.strip() for p in s.split("|")]
        return [p for p in parts if p]
    for sep in (";", ",", "|", "/"):
        if sep in s:
            parts = [p.strip() for p in s.split(sep)]
            if len(parts) > 1:
                return [p for p in parts if p]
    return [s]


def _to_int_safe(x: Any) -> Optional[int]:
    """Tenta extrair número inteiro de x."""
    if x is None:
        return None
    if isinstance(x, int) and not isinstance(x, bool):
        return int(x)
    if isinstance(x, float):
        if np.isnan(x):
            return None
        return int(round(x))
    s = str(x).strip()
    if not s:
        return None
    s = re.sub(r"\s+", "", s)
    m = re.search(r"(\d+(?:[.,]\d+)?)", s)
    if not m:
        return None
    try:
        return int(round(float(m.group(1).replace(",", "."))))
    except Exception:
        return None


def parse_listish(value: Any) -> List[str]:
    """Interpreta celas como lista."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return []
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    if isinstance(value, (int, float)):
        return [str(value)]
    s = str(value).strip()
    if not s:
        return []
    try:
        parsed = ast.literal_eval(s)
        if isinstance(parsed, list):
            return [str(x).strip() for x in parsed if str(x).strip()]
    except Exception:
        pass
    parts = re.split(r"[;,]", s)
    tokens: List[str] = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        tokens.extend(re.findall(r"[A-Za-z0-9./-]+", p))
    return tokens


def to_number(token: Any):
    """Converte token em número quando possível."""
    if token is None:
        return None
    if isinstance(token, (int, float)) and not (isinstance(token, float) and math.isnan(token)):
        return int(token) if float(token).is_integer() else float(token)
    s = str(token).strip()
    if not s:
        return None
    if "." in s and "," in s:
        s_norm = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s_norm = s.replace(",", ".")
    else:
        s_norm = s
    try:
        val = float(s_norm)
        return int(val) if float(val).is_integer() else val
    except Exception:
        return None


def parse_qty_list(value: Any) -> List[Any]:
    """Interpreta a lista de quantidades preservando números quando possível."""
    lst = parse_listish(value)
    if not lst:
        if value is None:
            return []
        num = to_number(value)
        return [num] if num is not None else [str(value)]
    out: List[Any] = []
    for tok in lst:
        num = to_number(tok)
        out.append(num if num is not None else tok)
    return out


# ============================================================
# VERIFICAÇÃO PRÉ-EXPLOSÃO
# ============================================================

def verifica_explosao(df: pd.DataFrame) -> pd.DataFrame:
    """Adiciona 'SIN PN' quando dcms > soma(CANTIDADE_CONSIDERAR)."""
    df_copy = df.copy()
    if EXPLODED_FLAG not in df_copy.columns:
        df_copy[EXPLODED_FLAG] = False

    # Garante dtype object para permitir armazenar listas em células
    for col in (PN, CE):
        if col in df_copy.columns:
            df_copy[col] = df_copy[col].astype(object)

    for idx, row in df_copy.iterrows():
        pn_list = _ensure_list(row.get(PN))
        ce_list_raw = _ensure_list(row.get(CE))
        ce_nums: List[int] = []
        for c in ce_list_raw:
            n = _to_int_safe(c)
            if n is not None:
                ce_nums.append(n)

        sum_known = sum(ce_nums) if ce_nums else 0

        dcms_raw = row.get(DCMS)
        try:
            dcms_val = None if pd.isna(dcms_raw) else int(round(float(str(dcms_raw).strip().replace(",", "."))))
        except Exception:
            dcms_val = _to_int_safe(dcms_raw)

        if dcms_val is None:
            continue
        remainder = int(round(dcms_val - sum_known))
        if remainder <= 0:
            continue
        if sum_known <= 0:
            continue

        new_pn_list = list(pn_list)
        new_ce_list = list(ce_nums)

        sin_idx = None
        for i, p in enumerate(new_pn_list):
            if isinstance(p, str) and p.strip().upper() == "SIN PN":
                sin_idx = i
                break

        if sin_idx is not None:
            try:
                new_ce_list[sin_idx] = int(round(new_ce_list[sin_idx] + remainder))
            except Exception:
                new_ce_list[sin_idx] = remainder
        else:
            new_pn_list.append("SIN PN")
            new_ce_list.append(remainder)

        df_copy.at[idx, PN] = new_pn_list
        df_copy.at[idx, CE] = new_ce_list
        df_copy.at[idx, EXPLODED_FLAG] = True

    return df_copy


# ============================================================
# EXPLOSÃO
# ============================================================

def explode_partnumbers(df: pd.DataFrame,
                        pn_col: str = PN,
                        qty_col: str = CE) -> pd.DataFrame:
    """Explode linhas multi-PN em linhas individuais."""
    required = [NUM_FORM, MARCA, DCMS, qty_col, pn_col, DESC]
    for c in required:
        if c not in df.columns:
            raise KeyError(f"Coluna obrigatória ausente no Excel: '{c}'")

    rows_out: list[dict] = []

    for _, row in df.iterrows():
        pns = parse_listish(row[pn_col])
        qtys = parse_qty_list(row[qty_col])

        if len(qtys) == 1 and len(pns) > 1:
            qtys = qtys * len(pns)
        if len(qtys) < len(pns):
            qtys = qtys + [None] * (len(pns) - len(qtys))

        for i, pn in enumerate(pns if pns else [None]):
            qty_val = qtys[i] if i < len(qtys) else None
            rows_out.append({
                NUM_FORM: row.get(NUM_FORM),
                MARCA: row.get(MARCA),
                PN: pn,
                CE: qty_val,
                DCMS: row.get(DCMS),
                DESC: row.get(DESC)
            })

    result = pd.DataFrame(rows_out, columns=[
        NUM_FORM, MARCA, PN, CE, DCMS, DESC
    ])
    return result


# ============================================================
# ORQUESTRADOR STEP 2
# ============================================================

def processar_explosao(path_in: str, output_dir: str, log_callback) -> None:
    """Step 2 — Verifica e explode PNs. Salva resultado na pasta de saída."""
    import os
    import time

    log_callback("═" * 50)
    log_callback("STEP 2 — Explosão de PartNumbers")
    log_callback("═" * 50)

    # 1. Carregar
    log_callback("\n📂 Carregando arquivo...")
    t0 = time.perf_counter()
    df = pd.read_excel(path_in, engine="calamine")
    log_callback(f"  ✓ {len(df):,} linhas carregadas ({time.perf_counter()-t0:.1f}s)")

    # 2. Verificação pré-explosão
    log_callback("\n🔍 Verificando dados pré-explosão...")
    df = verifica_explosao(df)
    added = df[EXPLODED_FLAG].sum() if EXPLODED_FLAG in df.columns else 0
    log_callback(f"  ✓ Linhas com 'SIN PN' adicionado: {added}")

    # 3. Explodir
    log_callback("\n💥 Explodindo linhas multi-PN...")
    t1 = time.perf_counter()
    exploded = explode_partnumbers(df)
    log_callback(f"  ✓ {len(df):,} linhas → {len(exploded):,} linhas ({time.perf_counter()-t1:.1f}s)")

    # 4. Exportar
    base_name = os.path.splitext(os.path.basename(path_in))[0]
    out_name = f"{base_name}_explodido"
    out_path = os.path.join(output_dir, f"{out_name}.xlsx")

    log_callback(f"\n📝 Salvando resultado...")
    t2 = time.perf_counter()
    exploded.to_excel(out_path, index=False, engine="openpyxl")
    log_callback(f"  ✓ XLSX salvo ({time.perf_counter()-t2:.1f}s): {out_path}")

    log_callback(f"\n✅ Explosão concluída com sucesso!")
    log_callback(f"  Linhas originais: {len(df):,}")
    log_callback(f"  Linhas geradas:   {len(exploded):,}")
