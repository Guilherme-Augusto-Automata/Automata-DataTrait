"""Fluxo 0 — Une descrições de mercadorias agrupando por formulário."""

import pandas as pd


def unite_desc(
    df: pd.DataFrame,
    id_col: str = "NUMERO_DE_FORMULARIO",
    desc_col: str = "DESCRIPCION_MERCANCIA",
    sep: str = " "
) -> pd.DataFrame:
    """Agrupa linhas por NUMERO_DE_FORMULARIO e concatena descrições."""
    work = df.copy()

    agg_map = {c: "first" for c in work.columns if c != desc_col}

    def _concat_descriptions(s: pd.Series) -> str:
        parts = [x.strip() for x in s.dropna().astype(str)]
        parts = [p for p in parts if p]
        return sep.join(parts)

    agg_map[desc_col] = _concat_descriptions

    out = (work
           .groupby(id_col, sort=False, as_index=False)
           .agg(agg_map))
    return out
