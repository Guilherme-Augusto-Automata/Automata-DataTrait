import pandas as pd
import re
import json
import time
from google import genai

# ============================================================
# CONFIGURAÇÃO
# ============================================================
INPUT_FILE = "ARG_2025.xlsx"
OUTPUT_XLSX = "ARG_2025_tratado.xlsx"
OUTPUT_CSV = "ARG_2025_tratado.csv"
# ============================================================
# 1. CARREGAR DADOS
# ============================================================
print("=" * 60)
print("ETAPA 1: Carregando ARG_2025.xlsx com engine calamine...")
print("=" * 60)
df = pd.read_excel(INPUT_FILE, sheet_name="Planilha1", engine="calamine")
print(f"  Linhas: {len(df):,}")
print(f"  Colunas: {len(df.columns)}")

# ============================================================
# 2. COLUNA AK (MARCA) - Extrair marca da coluna AB
#    AB = "Marca - Sufixos" → Extrair texto após "MARCA: "
# ============================================================
print("\n" + "=" * 60)
print("ETAPA 2: Preenchendo coluna MARCA (AK) a partir de 'Marca - Sufixos' (AB)")
print("=" * 60)


def extrair_marca(valor):
    if pd.isna(valor):
        return None
    valor = str(valor)
    match = re.search(r"MARCA:\s*(.+)", valor)
    if match:
        return match.group(1).strip()
    return None


df["MARCA"] = df["Marca - Sufixos"].apply(extrair_marca)
marcas_preenchidas = df["MARCA"].notna().sum()
print(f"  Marcas extraídas: {marcas_preenchidas:,} de {len(df):,} linhas")

# ============================================================
# 3. COLUNA AL (PARTNUMBER) - Extrair do último parêntese da coluna AH
#    AH = "Marca ou Descrição" → conteúdo do último (...) antes de -NA
# ============================================================
print("\n" + "=" * 60)
print("ETAPA 3: Preenchendo coluna PARTNUMBER (AL) a partir de 'Marca ou Descrição' (AH)")
print("=" * 60)


def extrair_partnumber(valor):
    if pd.isna(valor):
        return None
    valor = str(valor)
    # Encontra todos os conteúdos entre parênteses
    matches = re.findall(r"\(([^)]+)\)", valor)
    if matches:
        return matches[-1].strip()
    return None


df["PARTNUMBER"] = df["Marca ou Descrição"].apply(extrair_partnumber)
pn_preenchidos = df["PARTNUMBER"].notna().sum()
print(f"  Partnumbers extraídos: {pn_preenchidos:,} de {len(df):,} linhas")

# ============================================================
# 4. COLUNA AM (CANTIDAD) - Copiar da coluna AC (Quantidade.1)
# ============================================================
print("\n" + "=" * 60)
print("ETAPA 4: Preenchendo coluna CANTIDAD (AM) a partir de 'Quantidade.1' (AC)")
print("=" * 60)

df["CANTIDAD"] = df["Quantidade.1"]
cant_preenchidas = df["CANTIDAD"].notna().sum()
print(f"  Cantidades copiadas: {cant_preenchidas:,} de {len(df):,} linhas")

# ============================================================
# 5. COLUNA AN (FOB) - Converter para USD
#    Se Moeda (AF) == "DOLAR USA" → copiar U$S FOB (col N)
#    Senão → converter FOB Moeda (AE) usando cotação via Gemini
# ============================================================
print("\n" + "=" * 60)
print("ETAPA 5: Preenchendo coluna FOB (AN) - Conversão de moedas")
print("=" * 60)

# Identificar moedas diferentes de DOLAR USA
moedas_distintas = df["Moeda"].unique().tolist()
moedas_outras = [m for m in moedas_distintas if m != "DOLAR USA" and m.strip() != "`" and pd.notna(m)]

print(f"  Moedas encontradas: {len(moedas_distintas)}")
print(f"  Moedas que precisam conversão: {len(moedas_outras)}")
for m in moedas_outras:
    count = (df["Moeda"] == m).sum()
    print(f"    - {m}: {count:,} linhas")

# Obter cotações automaticamente via Gemini AI
taxas_cambio = {}

if moedas_outras:
    print("\n  Buscando cotações médias via Gemini AI...")
    GEMINI_API_KEY = "AIzaSyCUQl6taCkO9mDyipd7XAHFm2j3EcUsJKQ"
    client = genai.Client(api_key=GEMINI_API_KEY)

    moedas_lista = ", ".join(moedas_outras)
    prompt = (
        f"Preciso da cotação MÉDIA do ano inteiro de 2025 das seguintes moedas "
        f"convertidas para USD (dólar americano): {moedas_lista}.\n"
        f"IMPORTANTE: Se aparecer 'PESOS' ou algo similar, considere como PESOS ARGENTINOS (ARS).\n"
        f"Retorne APENAS um JSON válido no formato: "
        f'{{"NOME_MOEDA": valor_float, ...}}\n'
        f"Use os nomes exatamente como fornecidos. "
        f"Os valores devem ser numéricos (float), representando quanto 1 unidade da moeda vale em USD. "
        f"Não inclua texto adicional, apenas o JSON."
    )

    try:
        # Retry com backoff para lidar com rate limit (429)
        resposta_texto = None
        for tentativa in range(3):
            try:
                response = client.models.generate_content(
                    model="gemini-2.0-flash",
                    contents=prompt,
                )
                resposta_texto = response.text.strip()
                break
            except Exception as retry_err:
                if "429" in str(retry_err) and tentativa < 2:
                    wait = (tentativa + 1) * 10
                    print(f"    Rate limit atingido. Aguardando {wait}s antes de tentar novamente...")
                    time.sleep(wait)
                else:
                    raise retry_err
        if resposta_texto is None:
            raise Exception("Não foi possível obter resposta após 3 tentativas.")
        # Limpar possíveis marcadores de código
        resposta_texto = re.sub(r"json\s*", "", resposta_texto)
        resposta_texto = re.sub(r"\s*", "", resposta_texto)
        cotacoes_gemini = json.loads(resposta_texto)

        for moeda in moedas_outras:
            if moeda in cotacoes_gemini:
                taxas_cambio[moeda] = float(cotacoes_gemini[moeda])
            else:
                # Tentar buscar por nome parcial
                for chave, valor in cotacoes_gemini.items():
                    if chave.upper() in moeda.upper() or moeda.upper() in chave.upper():
                        taxas_cambio[moeda] = float(valor)
                        break

        print("\n  Cotações obtidas via Gemini AI:")
        for moeda, taxa in taxas_cambio.items():
            print(f"    1 {moeda} = {taxa} USD")

        # Verificar se alguma moeda ficou sem cotação
        moedas_sem_cotacao = [m for m in moedas_outras if m not in taxas_cambio]
        if moedas_sem_cotacao:
            print(f"\n  ⚠ Moedas sem cotação encontrada: {moedas_sem_cotacao}")
            for moeda in moedas_sem_cotacao:
                while True:
                    print(f"    1 {moeda} = ? USD: ", end="")
                    valor = input().strip()
                    try:
                        taxas_cambio[moeda] = float(valor)
                        break
                    except ValueError:
                        print(f"      Valor inválido. Digite um número (ex: 1.08)")

    except Exception as e:
        print(f"\n  ⚠ Erro ao consultar Gemini: {e}")
        print("  Inserindo cotações manualmente...")
        for moeda in moedas_outras:
            while True:
                print(f"    1 {moeda} = ? USD: ", end="")
                valor = input().strip()
                try:
                    taxas_cambio[moeda] = float(valor)
                    break
                except ValueError:
                    print(f"      Valor inválido. Digite um número (ex: 1.08)")

    # Permitir que o usuário revise e altere os valores
    print("\n  Deseja revisar/alterar alguma cotação? (s/n): ", end="")
    revisar = input().strip().lower()
    if revisar == "s":
        for moeda in list(taxas_cambio.keys()):
            print(f"    1 {moeda} = {taxas_cambio[moeda]} USD. Novo valor (Enter para manter): ", end="")
            novo_valor = input().strip()
            if novo_valor:
                try:
                    taxas_cambio[moeda] = float(novo_valor)
                    print(f"      ✓ Atualizado para {taxas_cambio[moeda]}")
                except ValueError:
                    print(f"      Valor inválido, mantendo {taxas_cambio[moeda]}")

    print("\n  Cotações finais:")
    for moeda, taxa in taxas_cambio.items():
        print(f"    1 {moeda} = {taxa} USD")

# Calcular a coluna FOB (AN)
print("\n  Calculando valores FOB em USD...")


def calcular_fob_usd(row):
    moeda = row["Moeda"]
    if pd.isna(moeda) or str(moeda).strip() == "`":
        return None
    if moeda == "DOLAR USA":
        fob_usd = row["U$S FOB"]
        # Se U$S FOB for 0, usar o valor de FOB Moeda (col AE)
        if pd.isna(fob_usd) or fob_usd == 0:
            return row["FOB Moeda"]
        return fob_usd
    # Converter usando a taxa de câmbio
    taxa = taxas_cambio.get(moeda)
    if taxa is not None:
        return round(row["FOB Moeda"] * taxa, 2)
    return None


df["FOB"] = df.apply(calcular_fob_usd, axis=1)
fob_preenchidos = df["FOB"].notna().sum()
print(f"  FOB calculados: {fob_preenchidos:,} de {len(df):,} linhas")

# ============================================================
# 6. RESUMO E EXPORTAÇÃO
# ============================================================
print("\n" + "=" * 60)
print("ETAPA 6: Exportando arquivos")
print("=" * 60)

# Verificar amostra das novas colunas
print("\n  Amostra das colunas tratadas (primeiras 10 linhas):")
print(df[["Marca - Sufixos", "MARCA", "Marca ou Descrição", "PARTNUMBER",
          "Quantidade.1", "CANTIDAD", "Moeda", "FOB Moeda", "U$S FOB", "FOB"]].head(10).to_string())

# Exportar CSV
print(f"\n  Salvando {OUTPUT_CSV}...")
df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig", sep="|")
print(f"  ✓ {OUTPUT_CSV} salvo com sucesso!")

# Exportar XLSX
print(f"\n  Salvando {OUTPUT_XLSX}...")
df.to_excel(OUTPUT_XLSX, index=False, engine="openpyxl")
print(f"  ✓ {OUTPUT_XLSX} salvo com sucesso!")

print("\n" + "=" * 60)
print("CONCLUÍDO!")
print(f"  Linhas processadas: {len(df):,}")
print(f"  MARCA preenchidas: {marcas_preenchidas:,}")
print(f"  PARTNUMBER preenchidos: {pn_preenchidos:,}")
print(f"  CANTIDAD preenchidas: {cant_preenchidas:,}")
print(f"  FOB calculados: {fob_preenchidos:,}")
print("=" * 60)