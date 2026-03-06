"""Fluxo 2 — Refina dados brutos: limpa marcas, PNs e quantidades."""

import re
import ast
from typing import Dict, List, Any, Set, Union
from .labels import LABELS, PALAVRAS_EXCLUIR, STOP_WORDS


def _normalizar_string(texto: str, remover_labels: List[str]) -> str:
    """Remove labels e espaços extras do início da string."""
    texto_upper = texto.upper()
    for label in remover_labels:
        pattern = re.compile(r'^\s*' + re.escape(label.upper()) + r'[:=\s./]*', re.IGNORECASE)
        match = pattern.match(texto_upper)
        if match:
            return texto[match.end():].strip()
    return texto.strip()


def _ensure_list(v: Any) -> List[str]:
    """Normaliza entrada que pode ser lista, str ou None para lista de strings."""
    if v is None:
        return []
    if isinstance(v, (list, tuple)):
        return [str(x) for x in v if x is not None]
    s = str(v).strip()
    if (s.startswith('[') and s.endswith(']')) or (s.startswith('(') and s.endswith(')')):
        try:
            parsed = ast.literal_eval(s)
            if isinstance(parsed, (list, tuple)):
                return [str(x) for x in parsed if x is not None]
        except Exception:
            pass
    return [s]


def clean_part_numbers(raw: Any) -> List[str]:
    """Extrai e limpa possíveis part numbers de uma lista/strings com 'sujeira'."""
    candidates: List[str] = []
    seen = set()
    pn_re = re.compile(r'[A-Z0-9][A-Z0-9\-\./]{3,30}', flags=re.I)

    for item in _ensure_list(raw):
        if not item:
            continue
        for m in pn_re.findall(item):
            tok = m.strip().upper()
            if any(sw in tok for sw in STOP_WORDS):
                continue
            if not re.search(r'\d', tok):
                continue
            if len(tok) < 4:
                continue
            if tok in seen:
                continue
            seen.add(tok)
            candidates.append(tok)

    return candidates


def refinar_dados(dados_brutos: Dict[str, List[str]]) -> Dict[str, Union[List[str], str]]:
    """Aplica as regras de refinamento sobre os dados extraídos."""

    # 1. MARCA
    marcas_refinadas = []
    if dados_brutos['marca'] != ['SIN']:
        marcas_limpas = [_normalizar_string(m, LABELS['marca']) for m in dados_brutos['marca']]
        marcas_limpas = [m for m in marcas_limpas if m and m.upper() not in PALAVRAS_EXCLUIR]
        marcas_unicas = sorted(list(set(marcas_limpas)), key=lambda x: marcas_limpas.index(x))
        if len(marcas_unicas) == 1:
            marcas_refinadas = [marcas_unicas[0]]
        elif len(marcas_unicas) > 1:
            marcas_refinadas = marcas_unicas
    resultado_marca = marcas_refinadas if marcas_refinadas else ['SIN']

    # 2. PART NUMBER
    pn_originais = dados_brutos['part_numbers']
    pn_refinados = []

    if pn_originais != ['SIN']:
        pn_strict_encontrados = [
            pn for pn in pn_originais
            if any(pn.upper().startswith(label.upper()) for label in LABELS['pn_strict'])
        ]
        if pn_strict_encontrados:
            pn_para_filtrar = pn_strict_encontrados
            labels_remover = LABELS['pn_strict']
        else:
            pn_para_filtrar = [
                pn for pn in pn_originais
                if any(pn.upper().startswith(label.upper()) for label in LABELS['pn_fallback'])
            ]
            labels_remover = LABELS['pn_fallback']

        for pn_completo in pn_para_filtrar:
            pn_limpo = _normalizar_string(pn_completo, labels_remover)
            palavras_pn = pn_limpo.upper().split()
            palavras_filtradas = [p for p in palavras_pn if p not in PALAVRAS_EXCLUIR]
            pn_final = ' '.join(palavras_filtradas).strip()
            if pn_final:
                pn_refinados.append(pn_final)

    resultado_pn = sorted(list(set(pn_refinados))) if pn_refinados else ['SIN']

    # 3. QUANTIDADE
    qtd_originais = dados_brutos['quantidades']
    qtd_refinadas = []

    if qtd_originais != ['SIN']:
        for qtd_completa in qtd_originais:
            qtd_limpa = _normalizar_string(qtd_completa, LABELS['qtd'])
            match = re.search(r'\(?(\d+)\)?', qtd_limpa)
            if match:
                qtd_refinadas.append(match.group(1))

    resultado_qtd = sorted(list(set(qtd_refinadas))) if qtd_refinadas else ['SIN']

    return {
        'marca': resultado_marca,
        'part_numbers': resultado_pn,
        'cantidade_ext': resultado_qtd
    }
