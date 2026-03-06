"""
Orquestrador Step 1 — Análise de PartNumbers.
Pipeline: une_linhas → extrai_info → refinar_dados → separar_erros
         → consulta_ia → checa_colunas → considera_colunas → compara_corrige_qtd.
"""

import os
import time
from datetime import timedelta

import pandas as pd

from .une_linhas import unite_desc
from .extrai_info import extrair_informacoes
from .refinar_dados import refinar_dados, clean_part_numbers
from .separar_erros import separar_erros
from .consulta_ia import consulta_ia
from .checa_colunas import checa_colunas
from .considera_colunas import (
    calcular_cantidade_considerar,
    calcular_part_number_considerar,
    calcular_qtd_elementos,
)
from .compara_corrige_qtd import compara_corrige_qtd


COLS_FINAL_SAIDA = [
    'numero_formulario', 'marca', 'part_numbers', 'part_numbers_refined',
    'cantidade_ext', 'cantidade_ext_ia', 'soma_cantidade_ext', 'cantidade_dcms',
    'CANTIDADE_CONSIDERAR', 'PARTNUMBER_CONSIDERAR', 'descripcion_mercancia',
    'qtd_pns_considerar', 'qtd_cantidade_considerar', '_ok'
]


def _process_df(df: pd.DataFrame, api_key: str, log_callback) -> pd.DataFrame:
    """Processa um chunk do DataFrame pelo pipeline completo."""

    # Fluxo 0: Unir Descrições
    log_callback("  [0/7] Unindo descrições por formulário...")
    df = unite_desc(df)
    log_callback(f"       {len(df):,} registros após agrupamento")

    # Fluxo 1: Extrair Informações Brutas
    log_callback("  [1/7] Extraindo informações brutas (marca, PN, quantidade)...")
    resultados_brutos = df['DESCRIPCION_MERCANCIA'].apply(extrair_informacoes)

    df['numero_formulario'] = df['NUMERO_DE_FORMULARIO']
    df['cantidade_dcms'] = df['CANTIDAD_DCMS']
    df['descripcion_mercancia'] = df['DESCRIPCION_MERCANCIA']
    df['marca_bruta'] = resultados_brutos.apply(lambda x: x['marca'])
    df['part_numbers_brutos'] = resultados_brutos.apply(lambda x: x['part_numbers'])
    df['cantidade_ext_bruta'] = resultados_brutos.apply(lambda x: x['quantidades'])

    # Fluxo 2: Refinar Dados
    log_callback("  [2/7] Refinando dados (limpeza e priorização)...")
    resultados_refinados = resultados_brutos.apply(refinar_dados)
    df['marca'] = resultados_refinados.apply(lambda x: x['marca'])
    df['part_numbers'] = resultados_refinados.apply(lambda x: x['part_numbers'])
    df['part_numbers'] = df['part_numbers'].apply(clean_part_numbers)
    df['cantidade_ext'] = resultados_refinados.apply(lambda x: x['cantidade_ext'])

    # Fluxo 3: Separar Erros
    log_callback("  [3/7] Validando registros...")
    df_temp = df.copy()
    df_problemas = separar_erros(df_temp)

    if not df_problemas.empty:
        log_callback(f"       {len(df_problemas):,} registros para correção via IA")

        # Fluxo 4: Consulta IA
        log_callback("  [4/7] Consultando IA (Claude) para correção...")
        df_corrigido_ia = consulta_ia(df=df_problemas, api_key=api_key)
    else:
        log_callback("       Nenhum problema encontrado, pulando IA")
        df_corrigido_ia = df_problemas

    # Fluxo 5: Checa Colunas
    log_callback("  [5/7] Pós-processamento (soma quantidades, refina PNs)...")
    df_final = checa_colunas(df_corrigido_ia)

    # Fluxo 6: Colunas CONSIDERAR
    log_callback("  [6/7] Calculando colunas finais (CONSIDERAR)...")
    df_final['PARTNUMBER_CONSIDERAR'] = df_final.apply(calcular_part_number_considerar, axis=1)
    df_final['CANTIDADE_CONSIDERAR'] = df_final.apply(calcular_cantidade_considerar, axis=1)
    df_final['qtd_pns_considerar'] = df_final['PARTNUMBER_CONSIDERAR'].apply(calcular_qtd_elementos)
    df_final['qtd_cantidade_considerar'] = df_final['CANTIDADE_CONSIDERAR'].apply(calcular_qtd_elementos)

    # Fluxo 7: Comparação e correção PN vs QTD
    log_callback("  [7/7] Comparando e corrigindo PN vs quantidade...")
    df_final = compara_corrige_qtd(df_final)

    # Filtra colunas de saída
    cols_disponiveis = [c for c in COLS_FINAL_SAIDA if c in df_final.columns]
    return df_final[cols_disponiveis]


def processar_analise(path_in: str, output_dir: str, api_key: str,
                      log_callback) -> None:
    """Step 1 — Análise completa de PartNumbers. Salva resultado na pasta de saída."""

    log_callback("═" * 50)
    log_callback("STEP 1 — Análise de PartNumbers")
    log_callback("═" * 50)

    # Carregar
    log_callback("\n📂 Carregando arquivo...")
    t_start = time.perf_counter()
    df_complet = pd.read_excel(path_in, engine="calamine")
    log_callback(f"  ✓ {len(df_complet):,} linhas carregadas")

    # Processar em chunks
    chunk_size = 20000
    processed_chunks = []
    total_chunks = (len(df_complet) + chunk_size - 1) // chunk_size

    for i, start in enumerate(range(0, len(df_complet), chunk_size)):
        chunk = df_complet.iloc[start:start + chunk_size]
        log_callback(f"\n📦 Processando chunk {i+1}/{total_chunks} ({len(chunk):,} linhas)...")
        df_chunk = _process_df(chunk, api_key, log_callback)
        processed_chunks.append(df_chunk)

    # Concatenar
    if processed_chunks:
        df_final = pd.concat(processed_chunks, ignore_index=True)
    else:
        df_final = pd.DataFrame(columns=COLS_FINAL_SAIDA)

    # Estatísticas
    total_lines = len(df_final)
    if '_ok' in df_final.columns:
        try:
            correct = int(df_final['_ok'].astype(bool).sum())
        except Exception:
            correct = int(sum(1 for v in df_final['_ok'] if bool(v)))
    else:
        correct = 0
    incorrect = total_lines - correct
    percent = (correct / total_lines * 100) if total_lines > 0 else 0.0

    elapsed = time.perf_counter() - t_start
    elapsed_str = str(timedelta(seconds=int(elapsed)))

    # Exportar
    base_name = os.path.splitext(os.path.basename(path_in))[0]
    out_name = f"{base_name}_analise_pn"
    out_path = os.path.join(output_dir, f"{out_name}.xlsx")

    log_callback(f"\n📝 Salvando resultado...")
    t_save = time.perf_counter()
    df_final.to_excel(out_path, index=False, engine="openpyxl")
    log_callback(f"  ✓ XLSX salvo ({time.perf_counter()-t_save:.1f}s): {out_path}")

    # Resumo
    log_callback(f"\n{'═' * 50}")
    log_callback(f"✅ Análise concluída!")
    log_callback(f"  Linhas processadas:  {total_lines:,}")
    log_callback(f"  Linhas corretas:     {correct:,} ({percent:.1f}%)")
    log_callback(f"  Linhas com problemas:{incorrect:,}")
    log_callback(f"  Tempo total:         {elapsed_str}")
    if percent < 100.0:
        log_callback(f"  ⚠️ Há chance de erros nos dados!")
    log_callback(f"  Arquivo: {out_path}")
