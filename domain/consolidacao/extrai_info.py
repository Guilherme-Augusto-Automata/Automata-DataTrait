"""Fluxo 1 — Extrai marca, part numbers e quantidades de descrições textuais."""

import re
from typing import Dict, List
from .labels import LABELS


def extrair_informacoes(texto: str) -> Dict[str, List[str]]:
    """Extrai marca, part numbers e quantidades de um texto de descrição."""
    resultado = {
        'marca': [],
        'part_numbers': [],
        'quantidades': []
    }

    if not texto or not isinstance(texto, str):
        return resultado

    texto_upper = texto.upper()

    def extrair(labels: List[str], texto_original: str, texto_upper: str,
                max_len: int, extra_delimiters: List[str] = [],
                prefix_space_required: bool = False) -> List[str]:
        encontrados = []
        posicoes_usadas = set()
        delimiters = [',', '\n', '//'] + extra_delimiters

        for label in labels:
            pattern_str = r'\b' + re.escape(label.upper()) + r'\b|' + re.escape(label.upper())
            pattern = pattern_str if len(label) < 4 else re.escape(label.upper())
            matches = re.finditer(pattern, texto_upper)

            for match in matches:
                pos_inicio = match.start()
                if pos_inicio in posicoes_usadas:
                    continue
                inicio = match.end()
                fim = inicio
                while fim < len(texto_original) and fim - inicio < max_len:
                    char = texto_original[fim]
                    if char in delimiters:
                        break
                    if (label in LABELS['pn_strict'] or label in LABELS['pn_fallback']
                            or label in LABELS['qtd']) and char in [' ', '.']:
                        if label in LABELS['qtd']:
                            if (char == ' ' and fim + 1 < len(texto_original)
                                    and not texto_original[fim + 1].isdigit()
                                    and texto_original[fim + 1] != '('):
                                break
                        pass
                    fim += 1

                trecho = texto_original[inicio:fim].strip()
                if trecho:
                    trecho_completo = texto_original[match.start():fim].strip()
                    encontrados.append(trecho_completo)
                    posicoes_usadas.add(pos_inicio)

        return encontrados

    # === MARCA ===
    marcas_encontradas = extrair(LABELS['marca'], texto, texto_upper, 50,
                                  extra_delimiters=['.', '/'])
    resultado['marca'] = marcas_encontradas if marcas_encontradas else ['SIN']

    # === PART NUMBERS ===
    pn_strict_encontrados = extrair(LABELS['pn_strict'], texto, texto_upper, 50)

    if pn_strict_encontrados:
        resultado['part_numbers'] = pn_strict_encontrados
    else:
        pn_fallback_encontrados = []
        posicoes_usadas_pn = set()

        for label_pn in LABELS['pn_fallback']:
            pattern = re.escape(label_pn.upper())
            matches = re.finditer(pattern, texto_upper)

            for match in matches:
                pos_inicio = match.start()
                if pos_inicio in posicoes_usadas_pn:
                    continue
                inicio = match.end()
                fim = inicio
                while fim < len(texto) and fim - inicio < 50:
                    if texto[fim] in [',', '\n', '//']:
                        break
                    fim += 1
                trecho = texto[inicio:fim].strip()
                if len(trecho) > 0:
                    if (re.search(r'[A-Z0-9/-]{3,}', trecho, re.IGNORECASE)
                            and not re.search(
                                r'\b(NO APLICA|QUE LO IDENTIFICA|VER SERIAL)\b',
                                trecho, re.IGNORECASE)):
                        trecho_completo = texto[match.start():fim].strip()
                        pn_fallback_encontrados.append(trecho_completo)
                        posicoes_usadas_pn.add(pos_inicio)

        resultado['part_numbers'] = pn_fallback_encontrados if pn_fallback_encontrados else ['SIN']

    # === QUANTIDADES ===
    qtd_encontradas = []
    posicoes_usadas_qtd = set()

    for label_qtd in LABELS['qtd']:
        pattern = re.escape(label_qtd.upper())
        matches = re.finditer(pattern, texto_upper)

        for match in matches:
            pos_inicio = match.start()
            if pos_inicio in posicoes_usadas_qtd:
                continue
            inicio = match.end()
            fim = inicio
            while fim < len(texto) and fim - inicio < 30:
                char = texto[fim]
                if char in ['\n', '//']:
                    break
                if (char == ' ' and fim + 1 < len(texto)
                        and not texto[fim + 1].isdigit()
                        and texto[fim + 1] != '('):
                    break
                if char == ')':
                    fim += 1
                    break
                fim += 1
            trecho = texto[inicio:fim].strip()
            if trecho and re.search(r'\d', trecho):
                trecho_completo = texto[match.start():fim].strip()
                qtd_encontradas.append(trecho_completo)
                posicoes_usadas_qtd.add(pos_inicio)

    padrao_unidade = r'(\d+(?:\s*[.,]\s*\d(?:\s*\d)*)?)\s*(UNIDAD(?:ES)?|EA|PZA|PC|PCS)\b'
    matches_unidade = re.findall(padrao_unidade, texto_upper)
    for qtd_raw, unidade in matches_unidade:
        qtd_norm = re.sub(r'\s+', '', qtd_raw).replace(',', '.')
        if not qtd_norm or re.fullmatch(r'[.,]+', qtd_norm):
            continue
        resultado_str = f"{qtd_norm} {unidade}"
        if resultado_str not in qtd_encontradas:
            qtd_encontradas.append(resultado_str)

    resultado['quantidades'] = sorted(list(set(qtd_encontradas))) if qtd_encontradas else ['SIN']

    return resultado
