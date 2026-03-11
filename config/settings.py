"""Constantes e valores padrão."""
import time

# Sem chave padrão — o usuário sempre digita na interface
DEFAULT_API_KEY = ""

# Ano default para cotações (ano anterior)
DEFAULT_ANO = str(time.localtime().tm_year - 1)

# Cotações fallback (médias aproximadas) caso a IA falhe
FALLBACK_COTACOES = {
    "EURO": 1.08,
    "YENS": 0.0067,
    "YUAN": 0.137,
    "CORONAS SUECAS": 0.095,
    "FRANCOS SUIZOS": 1.13,
    "PESOS": 0.00085,
    "LIBRA ESTERLINA": 1.27,
    "REAL": 0.175,
    "CORONAS DANESAS": 0.145,
    "DOLAR AUSTRALIA": 0.645,
    "DOLAR CANADIENS": 0.73,
    "CORONAS NORUEGA": 0.091,
    "DOLAR NEOZELAND": 0.59,
    "RAND": 0.054,
}

# Mapeamento de colunas para banco de dados: (coluna_destino, [nomes_possíveis_na_origem])
DB_COLUMN_MAP = [
    ("NUMERO_DE_FORMULARIO",       ["identificador"]),
    ("RAZON_SOCIAL_IMPORTADOR",    ["importador"]),
    ("CODIGO_LUGAR_INGRESO_MERCA", ["país de origem", "pais de origem",
                                    "pais orig.", "pais orig",
                                    "país orig.", "país orig"]),
    ("SUBPARTIDA_ARANCELARIA",     ["nandina", "ncm-sim", "ncm_sim", "ncm sim"]),
    ("CANTIDAD_DCMS",              ["cantidad"]),
    ("VALOR_FOB_USD",              ["usd fob", "fob dolar", "fob dólar", "fob"]),
    ("DESCRIPCION_MERCANCIA",      ["descricao", "descrição comercial",
                                    "descricao comercial",
                                    "descripcion arancelaria",
                                    "descripción arancelaria"]),
    ("FECHA_LEVANTE",              ["data", "fech.num", "fech num",
                                    "fecha numeracion", "fecha_numeracion",
                                    "fecha de numeración", "fecha numeración"]),
    ("PARTNUMBERS",                ["partnumber", "partnumbers", "part number", "part_number"]),
    ("MARCA",                      ["marca"]),
]

# Colunas que ficam vazias
DB_EMPTY_COLS = ["STATUS", "AVG", "DBL_MARKET", "DBL_SEGMENT", "COUNTRY"]

# Ordem final das colunas no arquivo de saída
DB_OUTPUT_COLUMNS = [
    "NUMERO_DE_FORMULARIO", "RAZON_SOCIAL_IMPORTADOR", "CODIGO_LUGAR_INGRESO_MERCA",
    "SUBPARTIDA_ARANCELARIA", "CANTIDAD_DCMS", "VALOR_FOB_USD", "DESCRIPCION_MERCANCIA",
    "FECHA_LEVANTE", "PARTNUMBERS", "MARCA", "STATUS", "IMPORTADORES",
    "AVG", "DBL_MARKET", "DBL_SEGMENT", "COUNTRY", "CANTIDAD", "VALOR_FOB_USD_2",
]

# Tipo de string otimizado (PyArrow quando disponível)
try:
    import pyarrow  # noqa: F401
    STR_DTYPE = "string[pyarrow]"
except ImportError:
    STR_DTYPE = "string"
