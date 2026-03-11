"""Fluxo 3 — Valida registros e marca linhas com problemas."""

import pandas as pd
from typing import Dict, List


def _check_errors_row(row: pd.Series) -> Dict[str, List[str]]:
    """Verifica problemas de qualidade em uma única linha."""
    problemas: Dict[str, List[str]] = {
        'marca': [],
        'pn': [],
        'qtd': []
    }

    # 1. MARCA
    marca_refinada = row['marca']
    tamanho_marca = len(marca_refinada) if marca_refinada != ['SIN'] else 0
    if tamanho_marca > 1:
        problemas['marca'].append("mais de uma marca encontrada")
    if tamanho_marca < 1:
        problemas['marca'].append("nenhuma marca encontrada")

    # 2. PART NUMBER
    pn_refinado = row['part_numbers']
    if pn_refinado == ['SIN'] or not pn_refinado:
        problemas['pn'].append("nenhum partnumber encontrado")

    # 3. QUANTIDADE
    qtd_refinada = row['cantidade_ext']
    if qtd_refinada == ['SIN'] or not qtd_refinada:
        problemas['qtd'].append("nenhuma quantidade encontrada")
    else:
        try:
            quantidades_extraidas_int = [int(q) for q in qtd_refinada]
            soma_quantidades_extraidas = sum(quantidades_extraidas_int)
        except ValueError:
            soma_quantidades_extraidas = -1
        try:
            qtd_dcms_int = int(row['cantidade_dcms'])
        except (ValueError, TypeError):
            qtd_dcms_int = -2
        if len(qtd_refinada) > 1 and soma_quantidades_extraidas != qtd_dcms_int:
            problemas['qtd'].append("soma das quantidades diferente de dcms")

    problemas_totais = problemas['marca'] + problemas['pn'] + problemas['qtd']
    return problemas if problemas_totais else {}


def separar_erros(df: pd.DataFrame) -> pd.DataFrame:
    """Marca registros com problemas na coluna 'problemas'."""
    df['problemas'] = df.apply(_check_errors_row, axis=1)
    return df
