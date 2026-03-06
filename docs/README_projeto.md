# Automata-DataTrait — Documentação do Projeto

## Visão Geral

**Automata-DataTrait** é uma aplicação desktop para tratamento, normalização e preparação de dados de importação comercial (Equador e Argentina) para inserção em banco de dados.

A interface gráfica é construída com **CustomTkinter** + **TkinterDnD2** (drag-and-drop), e o processamento utiliza **pandas**, **NumPy**, **Polars**, **Aho-Corasick** e **Google Gemini AI** para conversão de moedas.

---

## Índice

1. [Arquitetura](#arquitetura)
2. [Estrutura de Diretórios](#estrutura-de-diretórios)
3. [Dependências](#dependências)
4. [Como Executar](#como-executar)
5. [Funcionalidades](#funcionalidades)
6. [Fluxos de Processamento](#fluxos-de-processamento)
7. [Referência de Módulos](#referência-de-módulos)
8. [Princípios de Design](#princípios-de-design)
9. [Build (Executável)](#build-executável)

---

## Arquitetura

O projeto segue o padrão **DDD (Domain-Driven Design)** com separação em 4 camadas:

```
┌─────────────────────────────────────────────────┐
│                    main.py                      │  ← Ponto de entrada
├─────────────────────────────────────────────────┤
│                   ui/ (Apresentação)            │  ← Interface gráfica
│   app.py │ tab_tratamento │ tab_normalizacao    │
│          │ tab_banco_dados │ components         │
├─────────────────────────────────────────────────┤
│               domain/ (Regras de Negócio)       │  ← Lógica de domínio
│   tratamento/       │ normalizacao/             │
│   equador │ argentina │ service                 │
│                     │ banco_dados/              │
│                     │ service                   │
├─────────────────────────────────────────────────┤
│            infrastructure/ (I/O)                │  ← Leitura/escrita
│                file_io.py                       │
├─────────────────────────────────────────────────┤
│               config/ (Configuração)            │  ← Constantes
│           settings.py │ colors.py               │
└─────────────────────────────────────────────────┘
```

**Fluxo de dependência:** `config ← infrastructure ← domain ← ui ← main`

---

## Estrutura de Diretórios

```
Automata-DataTrait/
├── main.py                          # Ponto de entrada (9 linhas)
├── config/
│   ├── settings.py                  # Constantes, fallbacks, mapeamentos (59 linhas)
│   └── colors.py                    # Paleta de cores da UI (17 linhas)
├── infrastructure/
│   └── file_io.py                   # Leitura/escrita Excel/CSV (161 linhas, 13 funções)
├── domain/
│   ├── tratamento/
│   │   ├── __init__.py              # Re-exporta funções públicas
│   │   ├── equador.py               # Tratamento Equador (64 linhas, 5 funções)
│   │   └── argentina.py             # Tratamento Argentina (337 linhas, 20 funções)
│   ├── normalizacao/
│   │   ├── __init__.py              # Re-exporta processar_normalizacao
│   │   └── service.py               # Normalização Aho-Corasick (258 linhas, 13 funções)
│   └── banco_dados/
│       ├── __init__.py              # Re-exporta processar_banco_dados
│       └── service.py               # Preparação para BD (163 linhas, 10 funções)
├── ui/
│   ├── components.py                # Componentes reutilizáveis (224 linhas, 16 funções)
│   ├── app.py                       # Shell principal (224 linhas, 16 métodos)
│   ├── tab_tratamento.py            # Aba Tratamento (521 linhas, 39 métodos)
│   ├── tab_normalizacao.py          # Aba Normalização (721 linhas, 54 métodos)
│   └── tab_banco_dados.py           # Aba Banco de Dados (379 linhas, 27 métodos)
├── _logo_data.py                    # Logo em base64 (embutida no .exe)
├── _logo_b64.txt                    # Base64 da logo
├── _gen_logo.py                     # Script gerador da logo
├── build_exe.py                     # Script de build PyInstaller
└── TratamentoDados.spec             # Spec PyInstaller
```

**Total: ~3.137 linhas | 213 funções/métodos | 16 arquivos fonte**

---

## Dependências

| Pacote | Versão Mín. | Uso |
|--------|------------|-----|
| `pandas` | 2.x | Manipulação de DataFrames |
| `numpy` | 1.x | Operações vetorizadas |
| `polars` | 0.20+ | Export XLSX de alta performance |
| `pyarrow` | 12+ | Backend de strings otimizado |
| `openpyxl` | 3.x | Fallback de escrita XLSX |
| `python-calamine` | 0.2+ | Leitura rápida de Excel |
| `customtkinter` | 5.x | Interface gráfica moderna |
| `tkinterdnd2` | 0.3+ | Drag-and-drop nativo |
| `Pillow` | 10+ | Carregamento de logo |
| `google-genai` | 1.x | API Gemini para cotações |
| `pyahocorasick` | 2.x | Multi-pattern matching O(n) |

### Instalação

```bash
python -m venv venv
source venv/bin/activate          # Linux/Mac
# venv\Scripts\activate           # Windows

pip install pandas numpy polars pyarrow openpyxl python-calamine \
            customtkinter tkinterdnd2 Pillow google-genai pyahocorasick
```

---

## Como Executar

```bash
cd Automata-DataTrait
python main.py
```

A aplicação abre uma janela com 3 abas: **Tratamento**, **Normalização** e **Banco de Dados**.

---

## Funcionalidades

### 1. Tratamento de Dados

#### Equador
- Carrega planilha de importação do Equador (`.xlsx` ou `.csv`)
- Copia dados entre colunas por posição:
  - `AQ → AW` (coluna de origem → destino)
  - `AR → AX`
  - `AL → AY`
  - `AG → AZ`
- Exporta resultado tratado (XLSX, CSV ou ambos)

#### Argentina
- Carrega planilha de importação da Argentina
- **Extrai MARCA** via regex: `MARCA:\s*(.+)` da coluna "Marca - Sufixos"
- **Extrai PARTNUMBER** via regex: último conteúdo entre parênteses `.*\(([^)]+)\)` da coluna "Marca ou Descrição"
- **Copia CANTIDAD** da coluna "Quantidade.1"
- **Identifica moedas** distintas (exceto DOLAR USA)
- **Consulta cotações** via Google Gemini AI (com retry automático)
- **Tela de revisão** permite ao usuário editar cotações antes de confirmar
- **Calcula FOB em USD** usando cotações vetorizadas
- Exporta resultado tratado

### 2. Normalização

- Carrega arquivo de dados com colunas MARCA e PARTNUMBER
- **Via Excel**: carrega arquivo de regras (MARCA + PARTNUMBER opcional)
- **Via APP**: entrada manual de regras na interface
- **Regras de PARTNUMBER**: se o PN existe no arquivo, altera a MARCA (lookup O(1) via dict)
- **Regras de MARCA**: Aho-Corasick multi-pattern matching O(n) sobre valores únicos
- **Conversão de PN**: substitui partnumber antigo por novo
- Exporta resultado normalizado

### 3. Banco de Dados

- Carrega arquivo já tratado (Equador ou Argentina)
- **Mapeia colunas** para o formato padrão do banco (case-insensitive)
- **Adiciona colunas vazias** (STATUS, AVG, DBL_MARKET, etc.)
- **Duplica colunas** (IMPORTADORES, CANTIDAD, VALOR_FOB_USD_2)
- **Divide em partes** configurável (10K, 50K, 100K linhas)
- Exporta em XLSX, CSV ou ambos

---

## Fluxos de Processamento

### Fluxo Equador

```
Arquivo .xlsx/.csv
       │
       ▼
  ler_arquivo()          ← infrastructure/file_io.py
       │
       ▼
  _garantir_colunas_ate_az()
       │
       ▼
  _copiar_colunas()      ← AQ→AW, AR→AX, AL→AY, AG→AZ
       │
       ▼
  exportar_resultado()   ← XLSX (Polars) + CSV (pipe-separated)
```

### Fluxo Argentina

```
Arquivo .xlsx/.csv
       │
       ▼
  _carregar_arquivo()
       │
       ├── _extrair_marca()         ← regex vetorizado
       ├── _extrair_partnumber()    ← regex vetorizado
       ├── _copiar_cantidad()
       │
       ▼
  _listar_moedas_nao_usd()
       │
       ▼
  _obter_cotacoes()
       ├── _construir_prompt()      ← monta prompt para Gemini
       ├── _enviar_com_retries()    ← 6 tentativas com backoff
       ├── _parsear_resposta()      ← JSON → dict
       └── _aplicar_fallback()      ← valores de referência
       │
       ▼
  [UI: Tela de revisão de cotações]
       │
       ▼
  _calcular_fob_usd()
       ├── _preencher_dolar_usa()   ← linhas já em USD
       └── _converter_outras_moedas() ← multiplicar por taxa
       │
       ▼
  exportar_resultado()
```

### Fluxo Normalização

```
Arquivo de dados + Regras (Excel ou manual)
       │
       ▼
  _carregar_arquivo()
       │
       ▼
  _encontrar_colunas()    ← busca MARCA e PARTNUMBER
       │
       ▼
  _limpar_colunas()       ← upper + strip + remove NAN
       │
       ▼
  _aplicar_todas_regras()
       ├── _aplicar_regras_pn()                 ← dict lookup O(1)
       ├── _aplicar_regras_marca_aho_corasick() ← O(n) sobre únicos
       │       ├── _construir_automaton()
       │       ├── _encontrar_matches()
       │       └── _aplicar_remap()
       └── _aplicar_conversoes_pn()             ← dict lookup O(1)
       │
       ▼
  exportar_resultado()
```

### Fluxo Banco de Dados

```
Arquivo tratado
       │
       ▼
  _carregar_arquivo()
       │
       ▼
  _mapear_colunas()               ← case-insensitive via DB_COLUMN_MAP
       │
       ▼
  _adicionar_colunas_vazias()     ← STATUS, AVG, etc.
       │
       ▼
  _adicionar_colunas_repetidas()  ← IMPORTADORES, CANTIDAD, FOB_2
       │
       ▼
  _reordenar_colunas()            ← DB_OUTPUT_COLUMNS
       │
       ▼
  _dividir_e_exportar()
       └── _exportar_em_partes()  ← N partes de X linhas
```

---

## Referência de Módulos

### `config/settings.py`

| Constante | Descrição |
|-----------|-----------|
| `DEFAULT_API_KEY` | Chave API padrão do Gemini |
| `DEFAULT_ANO` | Ano anterior (para cotações) |
| `FALLBACK_COTACOES` | Dict de cotações de referência (14 moedas) |
| `DB_COLUMN_MAP` | Mapeamento coluna_destino → nomes_possíveis (10 mapeamentos) |
| `DB_EMPTY_COLS` | Colunas que ficam vazias no banco (5 colunas) |
| `DB_OUTPUT_COLUMNS` | Ordem final das 18 colunas de saída |
| `STR_DTYPE` | `"string[pyarrow]"` se PyArrow disponível, senão `"string"` |

### `config/colors.py`

Paleta dark-mode com 14 cores: `background`, `surface`, `primary`, `secondary`, `error`, `info`, `success`, `warning`, `text`, `text_dim`, `card`, `card_hover`, `border`.

### `infrastructure/file_io.py`

| Função | Responsabilidade |
|--------|-----------------|
| `ler_arquivo(path, **kwargs)` | Dispatcher: CSV ou Excel |
| `_ler_csv(path, **kwargs)` | Lê CSV separado por pipe |
| `_ler_excel(path, **kwargs)` | Lê Excel via calamine |
| `col_idx(col)` | Converte letra Excel → índice 0-based |
| `exportar_resultado(df, dir, name, fmt, log)` | Dispatcher: XLSX+debug e/ou CSV |
| `exportar_banco(df, dir, name, fmt, log)` | Dispatcher: XLSX simples e/ou CSV |
| `_caminho_saida(dir, name, ext)` | Constrói path de saída |
| `_salvar_csv(df, path, log)` | Salva CSV pipe-separated |
| `_salvar_xlsx_com_debug(df, path, log)` | Salva XLSX via Polars com debug |
| `_salvar_xlsx_simples(df, path, log)` | Salva XLSX via Polars sem debug |
| `_salvar_xlsx_openpyxl(df, path, log)` | Fallback XLSX via openpyxl |
| `_debug_polars_colunas(df, df_pl, log)` | Compara pandas vs polars |
| `_verificar_pos_save(path, log)` | Relê XLSX e verifica dados |

### `domain/tratamento/equador.py`

| Função | Responsabilidade |
|--------|-----------------|
| `processar_equador(...)` | Orquestrador |
| `_carregar_arquivo(path, log)` | Carrega arquivo |
| `_garantir_colunas_ate_az(df)` | Estende DataFrame até coluna AZ |
| `_copiar_colunas(df, log)` | Copia AQ→AW, AR→AX, AL→AY, AG→AZ |
| `_exportar(df, path, dir, fmt, log)` | Exporta resultado |

### `domain/tratamento/argentina.py`

| Função | Responsabilidade |
|--------|-----------------|
| `processar_argentina(...)` | Orquestrador principal |
| `finalizar_argentina(...)` | Orquestrador de finalização |
| `_carregar_arquivo(path, log)` | Carrega arquivo |
| `_extrair_marca(df, log)` | Extrai MARCA via regex |
| `_extrair_partnumber(df, log)` | Extrai PARTNUMBER via regex |
| `_copiar_cantidad(df, log)` | Copia Quantidade.1 → CANTIDAD |
| `_listar_moedas_nao_usd(df, log)` | Lista moedas não-USD |
| `_obter_cotacoes(key, ano, moedas, log)` | Obtém cotações (IA + fallback) |
| `_chamar_gemini(key, ano, moedas, log)` | Chama API Gemini |
| `_construir_prompt(ano, moedas)` | Monta prompt |
| `_enviar_com_retries(client, prompt, log)` | Envia com retry (6 tentativas) |
| `_calcular_wait(tentativa, erro)` | Calcula tempo de backoff |
| `_parsear_resposta(texto, moedas, log)` | Extrai cotações do JSON |
| `_encontrar_moeda_no_json(cotacoes, moeda)` | Match exato ou parcial |
| `_aplicar_fallback(taxas, moedas, log)` | Preenche com valores padrão |
| `_calcular_fob_usd(df, taxas, log)` | Calcula FOB em USD |
| `_preencher_dolar_usa(result, moeda, usd, fob)` | FOB para linhas USD |
| `_converter_outras_moedas(result, moeda, fob, taxas)` | Converte via taxa |
| `_debug_colunas_finais(df, log)` | Debug de todas as colunas |
| `_indice_para_letra(i)` | Índice → letra Excel |

### `domain/normalizacao/service.py`

| Função | Responsabilidade |
|--------|-----------------|
| `processar_normalizacao(...)` | Orquestrador |
| `_carregar_arquivo(path, log)` | Carrega arquivo |
| `_encontrar_colunas(df, regras, log)` | Busca MARCA e PN |
| `_limpar_colunas(df, marca, pn)` | Upper + strip + remove NAN |
| `_aplicar_todas_regras(...)` | Dispatcher de regras |
| `_aplicar_regras_pn(df, regras, marca, pn, log)` | Dict lookup O(1) |
| `_log_detalhes_pn(df, map, rules, pn, log)` | Log por regra PN |
| `_aplicar_regras_marca_aho_corasick(df, regras, marca, log)` | AC matching |
| `_construir_automaton(rules)` | Monta autômato AC |
| `_encontrar_matches(automaton, unicos)` | Busca matches |
| `_aplicar_remap(df, marca, remap, rules, log)` | Aplica substituição |
| `_aplicar_conversoes_pn(df, convs, pn, log)` | Conversão PN→PN |
| `_log_detalhes_conversoes(convs, novo_pn, log)` | Log por conversão |

### `domain/banco_dados/service.py`

| Função | Responsabilidade |
|--------|-----------------|
| `processar_banco_dados(...)` | Orquestrador |
| `_carregar_arquivo(path, log)` | Carrega arquivo |
| `_find_column(cols, candidates)` | Busca coluna case-insensitive |
| `_mapear_colunas(df, log)` | Mapeamento via DB_COLUMN_MAP |
| `_adicionar_colunas_vazias(df, log)` | Colunas em branco |
| `_adicionar_colunas_repetidas(df, log)` | Colunas duplicadas |
| `_reordenar_colunas(df, log)` | Ordena por DB_OUTPUT_COLUMNS |
| `_log_resumo(df, df_orig, log)` | Loga métricas |
| `_dividir_e_exportar(df, name, dir, fmt, linhas, log)` | Dispatcher |
| `_exportar_em_partes(df, name, dir, fmt, linhas, log)` | Split + export |

### `ui/app.py`

| Método | Responsabilidade |
|--------|-----------------|
| `_build_ui()` | Orquestra montagem da UI |
| `_build_header(parent)` | Cabeçalho com logo e título |
| `_build_tabview(parent)` | Widget de abas |
| `_build_status_bar(parent)` | Barra de status inferior |
| `_build_tabs()` | Instancia as 3 abas |
| `_build_logo(parent)` | Dispatcher de logo |
| `_load_logo_embedded()` | Logo em base64 |
| `_load_logo_from_file()` | Logo de arquivo |
| `_render_logo_image(parent, img)` | Renderiza PIL como CTkImage |
| `_render_logo_fallback(parent)` | Ícone "A" como fallback |
| `_set_status(text, progresso)` | Atualiza status bar |

### `ui/components.py`

Componentes reutilizáveis da UI: `create_drop_zone()`, `create_file_label()`, `create_section_label()`, `create_separator()`, `create_log_panel()`, `create_output_dir_row()`, `create_format_selector()`, `parse_drop_path()`, `validate_file_path()`, `browse_file()`, `browse_directory()`, `log_message()`, `clear_log()`, `build_logo()`.

### `ui/tab_tratamento.py` (39 métodos)

Aba de tratamento com sub-builders para cada seção (país, API key, formato, pasta, drop zone, cotações) e separação de validação (`_validar_entrada`) e dispatch (`_despachar_processamento`).

### `ui/tab_normalizacao.py` (54 métodos)

Aba de normalização com sub-builders para método de entrada, drop zones (dados e regras), entrada manual, conversão PN, formato e saída. Validação separada (`_validar_entrada`), coleta de regras (`_coletar_regras`, `_carregar_regras_excel`, `_parsear_regras_do_dataframe`) e dispatch (`_despachar_normalizacao`).

### `ui/tab_banco_dados.py` (27 métodos)

Aba de banco de dados com sub-builders para drop zone, configuração de linhas, formato e saída. Validação separada (`_validar_entrada`, `_validar_linhas`) e dispatch (`_despachar_processamento`).

---

## Princípios de Design

### Single Responsibility Principle (SRP)
Cada função faz **exatamente uma coisa**. Funções orquestradoras chamam funções específicas:

```python
# Orquestrador — só coordena
def processar_argentina(...):
    df = _carregar_arquivo(...)
    _extrair_marca(df, ...)
    _extrair_partnumber(df, ...)
    _copiar_cantidad(df, ...)
    moedas = _listar_moedas_nao_usd(df, ...)
    taxas = _obter_cotacoes(...)
    cotacoes_callback(taxas, moedas, df, ...)

# Cada função faz uma única operação
def _extrair_marca(df, log_callback):
    df["MARCA"] = df["Marca - Sufixos"].astype(STR_DTYPE) \
        .str.extract(r"MARCA:\s*(.+)", expand=False).str.strip()
```

### Padrão aplicado em toda a UI

```python
# ANTES (monolítico):
def _build(self, tab):
    # 180 linhas construindo tudo...

# DEPOIS (sub-builders):
def _build(self, tab):
    self._build_country_selector(left)
    self._build_api_key_section(left)
    self._build_format_selector(left)
    self._build_output_dir(left)
    self._build_drop_zone(left)
    self._build_right_panel(body)

# ANTES (validação + dispatch misturados):
def _on_processar(self):
    if not self.input_file: ...  # validação
    threading.Thread(...).start()  # dispatch

# DEPOIS (separados):
def _on_processar(self):
    erro = self._validar_entrada()
    if erro: messagebox.showwarning(...); return
    self._despachar_processamento()
```

### Estratégia de Exportação
- **XLSX primário**: Polars `write_excel()` (performance)
- **XLSX fallback**: openpyxl `to_excel()` (compatibilidade)
- **CSV**: pandas `to_csv()` com separador pipe `|`
- **Debug pós-save**: relê XLSX e verifica preenchimento

### Otimizações de Performance
- **PyArrow strings** (`string[pyarrow]`): operações regex 2-5x mais rápidas
- **Aho-Corasick**: multi-pattern matching O(n) vs O(n×p) ingênuo
- **Deduplicação**: matching só sobre valores únicos, depois aplica via `.map()`
- **Vetorização**: todas operações pandas usam `.str.extract()`, `.map()`, `np.where()`
- **Calamine**: leitura de Excel 3-10x mais rápida que openpyxl

---

## Build (Executável)

```bash
python build_exe.py
```

Gera executável standalone via PyInstaller com logo embutida em base64 (`_logo_data.py`).

---

## Mapeamento de Colunas — Banco de Dados

| Coluna Destino | Possíveis Nomes na Origem |
|----------------|--------------------------|
| `NUMERO_DE_FORMULARIO` | identificador |
| `RAZON_SOCIAL_IMPORTADOR` | importador |
| `CODIGO_LUGAR_INGRESO_MERCA` | país de origem, pais de origem |
| `SUBPARTIDA_ARANCELARIA` | nandina, ncm-sim, ncm_sim, ncm sim |
| `CANTIDAD_DCMS` | cantidad |
| `VALOR_FOB_USD` | usd fob, fob dolar, fob dólar |
| `DESCRIPCION_MERCANCIA` | descrição comercial, descricao comercial, descripcion arancelaria |
| `FECHA_LEVANTE` | data |
| `PARTNUMBERS` | partnumber, partnumbers, part number, part_number |
| `MARCA` | marca |

**Colunas duplicadas automáticas:**
- `RAZON_SOCIAL_IMPORTADOR` → `IMPORTADORES`
- `CANTIDAD_DCMS` → `CANTIDAD`
- `VALOR_FOB_USD` → `VALOR_FOB_USD_2`

**Colunas vazias:** `STATUS`, `AVG`, `DBL_MARKET`, `DBL_SEGMENT`, `COUNTRY`

---

## Cotações Fallback

Valores de referência usados quando a IA não responde:

| Moeda | Valor em USD |
|-------|-------------|
| EURO | 1.08 |
| LIBRA ESTERLINA | 1.27 |
| FRANCOS SUIZOS | 1.13 |
| DOLAR CANADIENS | 0.73 |
| DOLAR AUSTRALIA | 0.645 |
| DOLAR NEOZELAND | 0.59 |
| REAL | 0.175 |
| CORONAS DANESAS | 0.145 |
| YUAN | 0.137 |
| CORONAS SUECAS | 0.095 |
| CORONAS NORUEGA | 0.091 |
| YENS | 0.0067 |
| RAND | 0.054 |
| PESOS | 0.00085 |
