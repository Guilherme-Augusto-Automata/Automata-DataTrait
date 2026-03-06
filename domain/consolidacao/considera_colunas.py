"""Fluxo 6 — Calcula colunas PARTNUMBER_CONSIDERAR e CANTIDADE_CONSIDERAR."""

import ast
import re
import numpy as np
import pandas as pd


def _ensure_list(v):
    """Normaliza várias formas de entrada para uma lista de strings."""
    if v is None:
        return []
    if (isinstance(v, float) and np.isnan(v)):
        return []
    try:
        if isinstance(v, (pd._libs.missing.NAType,)):
            return []
    except Exception:
        pass
    if isinstance(v, (list, tuple)):
        return [str(x).strip() for x in v if x is not None and str(x).strip() != ""]
    if isinstance(v, (int, float)):
        if pd.isna(v):
            return []
        return [str(v)]
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
    if any(sep in s for sep in (";", "|", "/", "\n")):
        parts = re.split(r'[;|/\n]+', s)
        return [p.strip() for p in parts if p.strip()]
    if re.search(r'\s{2,}', s):
        parts = re.split(r'\s{2,}', s)
        return [p.strip() for p in parts if p.strip()]
    return [s]


def calcular_part_number_considerar(row):
    """Lógica para determinar o PARTNUMBER_CONSIDERAR."""
    part_number = _ensure_list(row.get('part_numbers'))
    part_number_refined = _ensure_list(row.get('part_numbers_refined'))
    cantidade_ext = _ensure_list(row.get('cantidade_ext'))

    if part_number == part_number_refined:
        return part_number_refined

    q_pn = len(part_number)
    q_pnr = len(part_number_refined)
    q_ce = len(cantidade_ext)

    comuns = sorted(list(set(part_number).intersection(set(part_number_refined))))

    if q_pn == q_ce:
        if q_pnr == q_ce:
            return comuns
        else:
            return part_number
    else:
        return comuns


def calcular_cantidade_considerar(row):
    """Calcula CANTIDADE_CONSIDERAR considerando PNs equivalentes."""

    def _parse_nums(s):
        if s is None:
            return []
        ss = str(s).strip()
        if not ss:
            return []
        ss = re.sub(r'\s+', '', ss)
        nums = re.findall(r'(\d+(?:[.,]\d+)?)', ss)
        out = []
        for n in nums:
            try:
                out.append(float(n.replace(',', '.')))
            except Exception:
                continue
        return out

    def _item_value(s):
        nums = _parse_nums(s)
        if not nums:
            return None, 0
        if len(nums) == 1:
            return nums[0], 1
        prod_val = 1.0
        for v in nums:
            prod_val *= v
        return prod_val, len(nums)

    def _canon(s):
        return re.sub(r'[^A-Z0-9]', '', str(s).upper())

    cantidade_ext = _ensure_list(row.get('cantidade_ext'))
    cantidade_ext_ia = _ensure_list(row.get('cantidade_ext_ia'))
    pns_raw = row.get('PARTNUMBER_CONSIDERAR') or []
    pns = pns_raw if isinstance(pns_raw, list) else _ensure_list(pns_raw)

    canon_set = set(_canon(p) for p in pns if _canon(p))
    qtd_pns_effective = 1 if len(canon_set) <= 1 else max(1, len(pns))

    dcms_raw = row.get('cantidade_dcms')
    try:
        dcms = None if pd.isna(dcms_raw) else float(dcms_raw)
    except Exception:
        try:
            dcms = float(str(dcms_raw).strip())
        except Exception:
            dcms = None

    known_vals = []
    any_multi = False
    for it in cantidade_ext:
        val, count = _item_value(it)
        if val is None:
            continue
        known_vals.append(val)
        if count > 1:
            any_multi = True

    ia_prods = []
    for it in cantidade_ext_ia:
        nums = _parse_nums(it)
        if not nums:
            continue
        if len(nums) == 1:
            ia_prods.append(nums[0])
        else:
            prod_val = 1.0
            for n in nums:
                prod_val *= n
            ia_prods.append(prod_val)

    sum_known = float(sum(known_vals)) if known_vals else 0.0
    tol = 1e-6

    if dcms is not None and abs(sum_known - dcms) < tol and len(known_vals) != qtd_pns_effective:
        total = int(round(dcms))
        base = total // qtd_pns_effective
        rem = total % qtd_pns_effective
        distributed = [base + (1 if i < rem else 0) for i in range(qtd_pns_effective)]
        return distributed

    if qtd_pns_effective == 1:
        if dcms is not None:
            if abs(dcms - sum_known) < tol:
                return [int(round(dcms))]
            for p in ia_prods:
                if abs(p - dcms) < tol:
                    return [int(round(dcms))]
            if len(known_vals) == 1:
                for p in ia_prods:
                    if abs(p - dcms) < tol:
                        return [int(round(dcms))]
            return [int(round(dcms))]
        if not known_vals and ia_prods:
            return [int(round(ia_prods[0]))]
        return [int(round(sum_known))]

    # múltiplos PNs
    if len(known_vals) == qtd_pns_effective:
        prod_all = 1.0
        for v in known_vals:
            prod_all *= v
        if dcms is not None and abs(prod_all - dcms) < tol:
            return [int(round(dcms))]
        return [int(round(v)) for v in known_vals]

    if dcms is not None and ia_prods:
        for p in ia_prods:
            if abs(p - dcms) < tol:
                return [int(round(dcms))]

    result = [int(round(v)) for v in known_vals]
    if dcms is not None:
        remaining = int(round(dcms - sum_known))
        if remaining < 0:
            remaining = 0
        while len(result) < qtd_pns_effective:
            result.append(remaining if remaining > 0 else 0)
            remaining = 0
        return [int(round(v)) for v in result[:qtd_pns_effective]]

    while len(result) < qtd_pns_effective:
        result.append(0)
    return [int(round(v)) for v in result[:qtd_pns_effective]]


def calcular_qtd_elementos(row):
    """Conta o número de elementos na lista."""
    return len(row)
