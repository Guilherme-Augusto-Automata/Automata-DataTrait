"""Fluxo 5 — Pós-processamento: soma quantidades, refina PNs, normaliza marca."""

from math import prod
import re
import ast
import pandas as pd
from typing import Any, List, Union, Tuple, Dict
from .labels import ALLOWED_CHARS_RE, NOISE_PATTERNS


def _to_list(values: Any) -> List[str]:
    """Normaliza diferentes formatos para uma lista de strings."""
    if values is None:
        return []
    if isinstance(values, (list, tuple)):
        return [str(v) for v in values if v is not None]
    if isinstance(values, (int, float)):
        return [str(values)]
    s = str(values).strip()
    if (s.startswith("[") and s.endswith("]")) or (s.startswith("(") and s.endswith(")")):
        try:
            parsed = ast.literal_eval(s)
            if isinstance(parsed, (list, tuple)):
                return [str(v) for v in parsed if v is not None]
        except Exception:
            pass
    parts = re.split(r"[;,|/]\s*|\s{2,}", s)
    return [p.strip() for p in parts if p.strip()]


def _parse_item_value(item: str) -> Union[float, None]:
    """Interpreta um item numérico, retorna lista de floats ou None."""
    if item is None:
        return None
    s = str(item).strip()
    nums = re.findall(r"(\d+(?:[.,]\d+)?)", s)
    if not nums:
        return None
    out = []
    for n in nums:
        try:
            out.append(float(n.replace(",", ".")))
        except Exception:
            continue
    return out if out else None


def soma_cantidad_ext(cantidad_ext: Any, cantidad_dcms: Any) -> int:
    """Calcula a quantidade final a partir de cantidad_ext e compara com dcms."""
    items = _to_list(cantidad_ext)
    item_sums: List[float] = []
    item_prods: List[float] = []
    any_multi = False

    for it in items:
        nums = _parse_item_value(it) or []
        if not nums:
            continue
        item_sum = sum(nums)
        item_prod = prod(nums) if len(nums) > 0 else 0.0
        item_sums.append(item_sum)
        item_prods.append(item_prod)
        if len(nums) > 1:
            any_multi = True

    if not item_sums:
        return 0

    total_sum = sum(item_sums)
    total_prod = sum(item_prods)

    try:
        dcms = float(cantidad_dcms) if cantidad_dcms is not None and str(cantidad_dcms).strip() != "" else None
    except Exception:
        dcms = None

    tol = 1e-6
    if dcms is not None:
        if abs(total_sum - dcms) < tol:
            return int(round(dcms))
        if abs(total_prod - dcms) < tol:
            return int(round(dcms))

    if any_multi and total_prod != total_sum:
        return int(round(total_prod))

    return int(round(total_sum))


def parse_listish(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x) for x in v]
    if isinstance(v, str):
        try:
            parsed = ast.literal_eval(v.strip())
            if isinstance(parsed, list):
                return [str(x) for x in parsed]
        except Exception:
            pass
        return re.findall(r"[A-Za-z0-9./-]+", v)
    return re.findall(r"[A-Za-z0-9./-]+", str(v))


def clean_token(t: str) -> str:
    t = str(t).strip().upper().strip(".,;:!()[]{}")
    t = ALLOWED_CHARS_RE.sub("", t)
    t = re.sub(r"[.]{2,}", ".", t)
    t = re.sub(r"[-]{2,}", "-", t)
    t = re.sub(r"[/]{2,}", "/", t)
    return t


def looks_like_pn(t: str) -> bool:
    if not t:
        return False
    if t.endswith("/"):
        return False
    if any(p.search(t) for p in NOISE_PATTERNS):
        return False
    if len(t) < 6 and t.isdigit():
        return False
    cleaned = ALLOWED_CHARS_RE.sub("", t)
    return any(c.isalpha() for c in cleaned) or "-" in cleaned or len(cleaned) >= 6


def canonical_key(t: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", t)


def prefer_token(a: str | None, b: str, desc_upper: str) -> str:
    if a is None:
        return b
    aU, bU = a.upper(), b.upper()
    in_desc_a, in_desc_b = (aU in desc_upper), (bU in desc_upper)
    if in_desc_b and not in_desc_a:
        return b
    if in_desc_a and not in_desc_b:
        return a
    if "-" in bU and "-" not in aU:
        return b
    if "-" in aU and "-" not in bU:
        return a
    return b if len(bU) > len(aU) else a


def refine_row(row) -> Tuple[List[str], str, List[str]]:
    toks = parse_listish(row["part_numbers"]) + parse_listish(row["part_numbers_ia"])
    descU = str(row.get("descripcion_mercancia", "")).upper()
    cleaned, removed = [], []
    for tok in toks:
        ct = clean_token(tok)
        (cleaned if ct and looks_like_pn(ct) else removed).append(str(tok))
    chosen: Dict[str, str] = {}
    for ct in cleaned:
        k = canonical_key(ct)
        chosen[k] = prefer_token(chosen.get(k), ct, descU)
    refined = list(dict.fromkeys(chosen.values()))
    primary = refined[0] if refined else None
    return refined, primary, removed


def refine_part_numbers(df: pd.DataFrame) -> pd.DataFrame:
    res = df.apply(refine_row, axis=1, result_type="expand")
    df = df.copy()
    df["part_numbers_refined"] = res[0]
    df["primary_part_number"] = res[1]
    df["removed_tokens_debug"] = res[2]
    return df


def _normalize_marca(v: Any) -> Any:
    """Normaliza 'NO TIENE' em listas/strings de marca."""
    if v is None:
        return v
    if isinstance(v, (list, tuple)):
        for el in v:
            try:
                if isinstance(el, str) and 'NO TIENE' in el.upper():
                    return 'NO TIENE'
            except Exception:
                continue
        return list(v)
    if isinstance(v, str):
        return 'NO TIENE' if 'NO TIENE' in v.upper() else v
    try:
        s = str(v)
        return 'NO TIENE' if 'NO TIENE' in s.upper() else v
    except Exception:
        return v


def checa_colunas(df: pd.DataFrame) -> pd.DataFrame:
    """Pós-processamento: soma quantidades, refina PNs, normaliza marcas."""
    df = df.copy()

    if 'qtd' in df.columns:
        src_col = 'qtd'
    else:
        df['soma_cantidade_ext'] = 0
        return df

    def _compute(row):
        return soma_cantidad_ext(row.get(src_col),
                                  row.get('cantidade_dcms') or row.get('cantidad_dcms'))

    df['soma_cantidade_ext'] = df.apply(_compute, axis=1)

    df = df.rename(columns={
        "qtd": "cantidade_ext_ia",
        "pn": "part_numbers_ia"
    })

    df = refine_part_numbers(df)
    df['marca'] = df['marca'].apply(_normalize_marca)

    return df
