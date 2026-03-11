"""Fluxo 7 — Compara qtd_pns_considerar com qtd_cantidade_considerar e corrige."""

import ast
import pandas as pd
from typing import Any, List


def _to_list(v: Any) -> List[str]:
    """Normaliza valores para lista de strings."""
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x).strip() for x in v if x is not None and str(x).strip() != ""]
    if isinstance(v, (tuple, set)):
        return [str(x).strip() for x in list(v) if x is not None and str(x).strip() != ""]
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
        if sep in s and len(s) > 1:
            parts = [p.strip() for p in s.split(sep)]
            if len(parts) > 1:
                return [p for p in parts if p]
    return [s]


def compara_corrige_qtd(df: pd.DataFrame) -> pd.DataFrame:
    """Ajusta PARTNUMBER_CONSIDERAR para ficar coerente com qtd_cantidade_considerar."""
    df = df.copy()

    def _fix_row(row):
        pn_raw = row.get("PARTNUMBER_CONSIDERAR")
        pn_list = _to_list(pn_raw)

        try:
            qtd_pns = int(row.get("qtd_pns_considerar")) if row.get("qtd_pns_considerar") is not None else len(pn_list)
        except Exception:
            qtd_pns = len(pn_list)
        try:
            qtd_cant = int(row.get("qtd_cantidade_considerar")) if row.get("qtd_cantidade_considerar") is not None else 0
        except Exception:
            qtd_cant = 0

        if qtd_pns != len(pn_list):
            qtd_pns = len(pn_list)

        if qtd_pns > qtd_cant:
            to_remove = qtd_pns - qtd_cant
            if to_remove >= len(pn_list):
                pn_list = []
            else:
                pn_list = pn_list[:len(pn_list) - to_remove]
            qtd_pns = len(pn_list)

        row["PARTNUMBER_CONSIDERAR"] = pn_list
        row["qtd_pns_considerar"] = qtd_pns
        row["_ok"] = (qtd_pns == qtd_cant)

        if row["_ok"]:
            total = 0.0
            cant = row.get('CANTIDADE_CONSIDERAR')
            if isinstance(cant, (list, tuple)):
                for it in cant:
                    try:
                        total += float(str(it).strip())
                    except Exception:
                        continue
            else:
                try:
                    total = float(str(cant).strip())
                except Exception:
                    total = 0.0

            dcms = row.get('cantidade_dcms')
            try:
                dcms_val = None if pd.isna(dcms) else float(dcms)
            except Exception:
                try:
                    dcms_val = float(str(dcms).strip())
                except Exception:
                    dcms_val = None

            row["_ok"] = (dcms_val is not None and abs(total - float(dcms_val)) < 1e-6)

        return row

    df = df.apply(_fix_row, axis=1)
    return df
