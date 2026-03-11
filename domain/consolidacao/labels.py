"""Constantes, labels e padrões usados na extração de partnumbers."""

import re

LABELS = {
    "pn_strict": ["p/n", "pn", "part number", "partnumber", "P/N", "P/N ", "P/N:"],
    "pn_fallback": [
        "referencia", "referência", "ref",
        "sku", "CODIGO SEGÚN FACTURA",
        "item", "ITEM", "item ", "item:",
        "parte", "referencia:", "referencia="
    ],
    "marca": ["marca", "marca;", "marca:", "brand", "maker", "make", "MAR CA"],
    "qtd": ["qtd", "qtde", "qde", "qnt", "qty", "quantidade", "cantidad", "cant", "QTY",
            'Unidad', 'ca nt', 'c ant', 'can t', 'Cant']
}

PALAVRAS_EXCLUIR = {
    'SERIAL', 'TIENE', 'USO', 'DESTINO', 'VEHICULOS', 'AUTOMOTORES',
    'DELCAPITULO', 'TIPO', 'FILTRO', 'CAJA', 'ELEMENTO', 'FILTRANTE',
    'CELULOSA', 'MATERIA', 'FILTRAR', 'AIRE', 'CAPITULO', 'MODELO',
    'PRODUCTO', 'MARCA', 'REFERENCIA', 'DEL', 'SIN', 'TODAS', 'MALLA',
    'NO-TIENE', 'NO-APLICA', 'SISTEMA', 'PANEL', 'PLANO', 'PAPEL',
    'IMPUREZAS', 'ORIGEN', 'PAIS', 'BRASIL', 'ALEMANIA', 'CHINA',
    'USA', 'MEXICO', 'COLOMBIA', 'ARGENTINA', 'INDUSTRIAL', 'PARTES',
    'EXCLUSIVO', 'COMPUESTO', 'CINTAS', 'GOMA', 'INTEGRADAS', 'QUE',
    'IDENTIFICA', 'LO', 'EXCLUSIVAMENTE', '_ ', 'MODELO_',
    'ARCHITECT', '_ ARCHITECT', 'ARCHITECH', 'Y', 'MODELO_ ARCHITECT',
    'MARCA_', 'CELL', 'DYN', 'ACCESORIO', 'NO', 'LA' 'ORIGEN'
}

STOP_WORDS = {
    'SERIAL', 'TIENE', 'USO', 'DESTINO', 'VEHICULOS', 'AUTOMOTORES',
    'MODELO', 'PRODUCTO', 'MARCA', 'REFERENCIA', 'DEL', 'SIN', 'TODAS',
    'NO-TIENE', 'NO-APLICA', 'SISTEMA', 'PANEL', 'PLANO', 'PAPEL'
}

ALLOWED_CHARS_RE = re.compile(r"[^A-Z0-9./-]")

NOISE_PATTERNS = [
    re.compile(r"^DO[.\-_]?\d+", re.I),
    re.compile(r"^DO$", re.I),
    re.compile(r"^ADD\d+$", re.I),
    re.compile(r"^B\d{3,}$", re.I),
    re.compile(r"^\d{4}DM(\b|[-].*)?$", re.I),
]

STOP_CHARS = r"[;,\n|]"
SENTINELS_NONE = r"(?:N/?A|NAO\s*TEM|NÃO\s*TEM|NO\s*TIENE|SIN\s*DATO|NO\s*APLICA|NONE|NOT\s*AVAILABLE)"
