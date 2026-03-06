import pandas as pd

INPUT_FILE  = "EQU_2025.xlsx"
OUTPUT_XLSX = "EQU_2025_tratado.xlsx"
OUTPUT_CSV  = "EQU_2025_tratado.csv"

def col_idx(col: str) -> int:
    """Converte letra de coluna Excel (ex: 'AQ') para índice 0-based."""
    col = col.upper()
    result = 0
    for char in col:
        result = result * 26 + (ord(char) - ord('A') + 1)
    return result - 1

# Lê o arquivo xlsx usando o engine calamine
df = pd.read_excel(INPUT_FILE, engine="calamine", dtype=str)
print(f"Arquivo lido: {len(df)} linhas, {len(df.columns)} colunas")

# Garante que o df tenha colunas suficientes até AZ (índice 51 = 52 colunas)
while len(df.columns) <= col_idx("AZ"):
    df[f"_extra_{len(df.columns)}"] = None

# Copia os dados usando posição das colunas Excel (índice 0-based):
# AQ (42) -> AW (48)
# AR (43) -> AX (49)
# AL (37) -> AY (50)
# AG (32) -> AZ (51)
df.iloc[:, col_idx("AW")] = df.iloc[:, col_idx("AQ")].values
df.iloc[:, col_idx("AX")] = df.iloc[:, col_idx("AR")].values
df.iloc[:, col_idx("AY")] = df.iloc[:, col_idx("AL")].values
df.iloc[:, col_idx("AZ")] = df.iloc[:, col_idx("AG")].values

# Gera o xlsx tratado
df.to_excel(OUTPUT_XLSX, index=False)
print(f"XLSX gerado: {OUTPUT_XLSX}")

# Gera o CSV separado por "|"
df.to_csv(OUTPUT_CSV, index=False, sep="|", encoding="utf-8-sig")
print(f"CSV  gerado: {OUTPUT_CSV}")
