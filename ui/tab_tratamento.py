"""
Tab Tratamento — Interface para processamento de dados Equador/Argentina.
Cada método faz uma única coisa.
"""

import os
import time
import threading

import customtkinter as ctk
from tkinter import messagebox
from tkinterdnd2 import DND_FILES

from config.colors import COLORS
from config.settings import DEFAULT_API_KEY, DEFAULT_ANO
from domain.tratamento import processar_equador, processar_argentina, finalizar_argentina, processar_chile, processar_peru
from domain.consolidacao import processar_analise, processar_explosao
from ui.components import (
    parse_drop_path, validate_file_path, browse_file, browse_directory,
    log_message, clear_log,
)


class TabTratamento:
    """Gerencia a aba de Tratamento (Equador / Argentina)."""

    def __init__(self, tab_frame, root, status_callback, progress_bar):
        self.root = root
        self._set_status = status_callback
        self.progress = progress_bar

        # Estado
        self.input_file = None
        self.pais_selecionado = ctk.StringVar(value="equador")
        self.formato_saida = ctk.StringVar(value="ambos")
        self.api_key_var = ctk.StringVar(value=DEFAULT_API_KEY)
        self.api_key_visible = False
        self.ano_cotacao = ctk.StringVar(value=DEFAULT_ANO)
        self.output_dir_var = ctk.StringVar(value="")
        self.processando = False

        # Estado Chile
        self.input_file_secondary = None

        # Estado Colômbia
        self.etapa_colombia = ctk.StringVar(value="analise")
        self.api_key_claude_var = ctk.StringVar(value="")

        # Dados temporários Argentina (revisão de cotações)
        self._arg_df = None
        self._arg_taxas = {}
        self._arg_moedas = []
        self._arg_output_dir = ""
        self._arg_formato = ""
        self._cotacao_entries = {}

        self._build(tab_frame)

    # ============================================================
    # BUILD (orquestrador)
    # ============================================================
    def _build(self, body):
        """Orquestra a construção de todos os painéis da aba."""
        body.grid_columnconfigure(0, weight=1)
        body.grid_columnconfigure(1, weight=2)
        body.grid_rowconfigure(0, weight=1)

        left = ctk.CTkFrame(body, fg_color=COLORS["surface"], corner_radius=12,
                            border_width=1, border_color=COLORS["border"])
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 8))

        # Área scrollável para todo o conteúdo do formulário
        left_scroll = ctk.CTkScrollableFrame(left, fg_color="transparent")
        left_scroll.pack(fill="both", expand=True, padx=16, pady=(16, 0))

        self._build_country_selector(left_scroll)
        self._build_api_key_section(left_scroll)
        self._build_chile_section(left_scroll)
        self._build_peru_section(left_scroll)
        self._build_colombia_section(left_scroll)
        self._build_separator(left_scroll)
        self._build_format_selector(left_scroll)
        self._build_separator(left_scroll)
        self._build_output_dir(left_scroll)
        self._build_separator(left_scroll)
        self._build_drop_zone(left_scroll)

        # Sentinela no final da área scrollável (para posicionar frames dinâmicos)
        self._scroll_sentinel = ctk.CTkFrame(left_scroll, fg_color="transparent", height=0)
        self._scroll_sentinel.pack()

        # Botão fixo fora do scroll
        self._build_process_button(left)
        self._build_right_panel(body)

    # ============================================================
    # SUB-BUILDERS
    # ============================================================
    def _build_separator(self, parent):
        """Adiciona um separador horizontal."""
        ctk.CTkFrame(parent, fg_color=COLORS["border"], height=1).pack(fill="x", pady=16)

    def _build_country_selector(self, parent):
        """Constrói a seção de seleção de país."""
        ctk.CTkLabel(parent, text="🌍  PAÍS",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", pady=(0, 8))

        for val, label in [("equador", "🇪🇨  Equador"), ("argentina", "🇦🇷  Argentina"),
                           ("chile", "🇨🇱  Chile"), ("peru", "🇵🇪  Peru"),
                           ("colombia", "🇨🇴  Colômbia")]:
            ctk.CTkRadioButton(
                parent, text=label, variable=self.pais_selecionado,
                value=val, font=ctk.CTkFont(size=13),
                fg_color=COLORS["secondary"], hover_color=COLORS["primary"],
                border_color=COLORS["border"], text_color=COLORS["text"],
                command=self._on_pais_change
            ).pack(anchor="w", pady=3, padx=8)

    def _build_api_key_section(self, parent):
        """Constrói a seção de API key + seletor de ano (Argentina)."""
        self.api_frame = ctk.CTkFrame(parent, fg_color="transparent")
        self.api_frame.pack(fill="x", pady=(12, 0))

        ctk.CTkLabel(self.api_frame, text="🔑  API Key Gemini",
                     font=ctk.CTkFont(size=13, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", pady=(0, 4))

        self._build_api_entry(self.api_frame)
        self._build_year_selector(self.api_frame)

        self.api_frame.pack_forget()  # Oculto no início (Equador)

    def _build_chile_section(self, parent):
        """Constrói seção específica do Chile (API Gemini + arquivo secundário)."""
        self.chile_frame = ctk.CTkFrame(parent, fg_color="transparent")

        # API Key Gemini para Chile
        ctk.CTkLabel(self.chile_frame, text="🔑  API Key Gemini",
                     font=ctk.CTkFont(size=13, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", pady=(0, 4))

        self.chile_api_entry = ctk.CTkEntry(
            self.chile_frame, textvariable=self.api_key_var,
            fg_color=COLORS["card"], border_color=COLORS["border"],
            text_color=COLORS["text"], font=ctk.CTkFont(size=11), show="•")
        self.chile_api_entry.pack(fill="x", pady=(0, 4))

        ctk.CTkLabel(self.chile_frame,
                     text="Necessária para resolver partnumbers SIN-CODIGO via IA",
                     font=ctk.CTkFont(size=10),
                     text_color=COLORS["text_dim"]).pack(anchor="w", pady=(0, 8))

        # Drop zone para arquivo secundário (PROCV)
        ctk.CTkLabel(self.chile_frame, text="📁  ARQUIVO SECUNDÁRIO (PROCV Importador)",
                     font=ctk.CTkFont(size=13, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", pady=(8, 4))

        ctk.CTkLabel(self.chile_frame,
                     text="Planilha para buscar o importador (PROCV coluna A → AN → E)",
                     font=ctk.CTkFont(size=10),
                     text_color=COLORS["text_dim"]).pack(anchor="w", pady=(0, 6))

        self.chile_drop_zone = ctk.CTkFrame(
            self.chile_frame, fg_color=COLORS["card"],
            corner_radius=10, height=80,
            border_width=2, border_color=COLORS["border"])
        self.chile_drop_zone.pack(fill="x", pady=(0, 4))
        self.chile_drop_zone.pack_propagate(False)

        self.chile_drop_label = ctk.CTkLabel(
            self.chile_drop_zone,
            text="Arraste o arquivo secundário .xlsx / .csv aqui\nou clique para selecionar",
            font=ctk.CTkFont(size=11), text_color=COLORS["text_dim"],
            justify="center"
        )
        self.chile_drop_label.pack(expand=True)

        # Registrar drag-and-drop
        self.chile_drop_zone.drop_target_register(DND_FILES)
        self.chile_drop_zone.dnd_bind("<<Drop>>", self._on_drop_chile_secondary)
        self.chile_drop_label.drop_target_register(DND_FILES)
        self.chile_drop_label.dnd_bind("<<Drop>>", self._on_drop_chile_secondary)
        self.chile_drop_zone.bind("<Button-1>", self._on_browse_chile_secondary)
        self.chile_drop_label.bind("<Button-1>", self._on_browse_chile_secondary)

        self.chile_file_label = ctk.CTkLabel(
            self.chile_frame, text="",
            font=ctk.CTkFont(size=11),
            text_color=COLORS["success"], wraplength=250)
        self.chile_file_label.pack(anchor="w")

        self.chile_frame.pack_forget()  # Oculto no início

    def _build_peru_section(self, parent):
        """Constrói seção específica do Peru (API Gemini para PN)."""
        self.peru_frame = ctk.CTkFrame(parent, fg_color="transparent")

        # API Key Gemini para Peru
        ctk.CTkLabel(self.peru_frame, text="🔑  API Key Gemini",
                     font=ctk.CTkFont(size=13, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", pady=(0, 4))

        self.peru_api_entry = ctk.CTkEntry(
            self.peru_frame, textvariable=self.api_key_var,
            fg_color=COLORS["card"], border_color=COLORS["border"],
            text_color=COLORS["text"], font=ctk.CTkFont(size=11), show="•")
        self.peru_api_entry.pack(fill="x", pady=(0, 4))

        ctk.CTkLabel(self.peru_frame,
                     text="Necessária para detectar partnumbers via IA\n"
                          "quando não encontrados na coluna O",
                     font=ctk.CTkFont(size=10),
                     text_color=COLORS["text_dim"]).pack(anchor="w", pady=(0, 8))

        self.peru_frame.pack_forget()  # Oculto no início

    def _build_colombia_section(self, parent):
        """Constrói seção específica da Colômbia (etapa + API Claude)."""
        self.colombia_frame = ctk.CTkFrame(parent, fg_color="transparent")

        # Seletor de etapa
        ctk.CTkLabel(self.colombia_frame, text="🔬  Etapa",
                     font=ctk.CTkFont(size=13, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", pady=(0, 6))

        self.colombia_step_menu = ctk.CTkOptionMenu(
            self.colombia_frame, variable=self.etapa_colombia,
            values=["analise", "explosao"],
            font=ctk.CTkFont(size=13),
            fg_color=COLORS["card"],
            button_color=COLORS["secondary"],
            button_hover_color=COLORS["primary"],
            dropdown_fg_color=COLORS["surface"],
            dropdown_hover_color=COLORS["card_hover"],
            text_color=COLORS["text"],
            command=lambda _: self._on_colombia_step_change(),
        )
        self.colombia_step_menu.pack(fill="x", pady=(0, 4))

        self.colombia_step_desc = ctk.CTkLabel(
            self.colombia_frame, text="",
            font=ctk.CTkFont(size=10),
            text_color=COLORS["text_dim"],
            justify="left", wraplength=260)
        self.colombia_step_desc.pack(anchor="w", pady=(2, 8))

        # API Key Claude
        self.claude_key_label = ctk.CTkLabel(
            self.colombia_frame, text="🔑  API Key Claude",
            font=ctk.CTkFont(size=13, weight="bold"),
            text_color=COLORS["info"])
        self.claude_key_label.pack(anchor="w", pady=(0, 4))

        self.claude_key_entry = ctk.CTkEntry(
            self.colombia_frame, textvariable=self.api_key_claude_var,
            placeholder_text="sk-ant-...", show="•",
            fg_color=COLORS["card"], border_color=COLORS["border"],
            text_color=COLORS["text"], font=ctk.CTkFont(size=11))
        self.claude_key_entry.pack(fill="x", pady=(0, 4))

        self.claude_key_hint = ctk.CTkLabel(
            self.colombia_frame,
            text="Necessária para correção de PNs via IA (etapa Análise)",
            font=ctk.CTkFont(size=10),
            text_color=COLORS["text_dim"])
        self.claude_key_hint.pack(anchor="w", pady=(0, 0))

        self._colombia_key_widgets = [
            self.claude_key_label, self.claude_key_entry, self.claude_key_hint
        ]

        self.colombia_frame.pack_forget()  # Oculto no início
        self._on_colombia_step_change()  # Atualiza descrição

    def _build_api_entry(self, parent):
        """Constrói o campo de entrada da API key."""
        api_entry_frame = ctk.CTkFrame(parent, fg_color="transparent")
        api_entry_frame.pack(fill="x")

        self.api_entry = ctk.CTkEntry(api_entry_frame, textvariable=self.api_key_var,
                                       fg_color=COLORS["card"],
                                       border_color=COLORS["border"],
                                       text_color=COLORS["text"],
                                       font=ctk.CTkFont(size=11), show="•")
        self.api_entry.pack(side="left", fill="x", expand=True, padx=(0, 4))

        self.eye_btn = ctk.CTkButton(
            api_entry_frame, text="👁", width=36, height=28,
            font=ctk.CTkFont(size=14),
            fg_color=COLORS["card"], hover_color=COLORS["card_hover"],
            border_width=1, border_color=COLORS["border"],
            corner_radius=6, command=self._toggle_api_key_visibility
        )
        self.eye_btn.pack(side="right")

    def _build_year_selector(self, parent):
        """Constrói o seletor de ano de referência."""
        ano_row = ctk.CTkFrame(parent, fg_color="transparent")
        ano_row.pack(fill="x", pady=(8, 0))

        ctk.CTkLabel(ano_row, text="📅  Ano de referência",
                     font=ctk.CTkFont(size=12),
                     text_color=COLORS["text_dim"]).pack(side="left", padx=(0, 8))

        anos_disponiveis = [str(a) for a in range(2015, time.localtime().tm_year + 1)][::-1]
        ctk.CTkOptionMenu(
            ano_row, values=anos_disponiveis, variable=self.ano_cotacao,
            fg_color=COLORS["card"], button_color=COLORS["primary"],
            button_hover_color=COLORS["secondary"],
            text_color=COLORS["text"], font=ctk.CTkFont(size=12), width=90,
        ).pack(side="left")

    def _build_format_selector(self, parent):
        """Constrói a seção de seleção de formato de saída."""
        ctk.CTkLabel(parent, text="📄  FORMATO DE SAÍDA",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", pady=(0, 8))

        for val, label in [("xlsx", "XLSX (Excel)"), ("csv", "CSV (separado por |)"),
                           ("ambos", "Ambos")]:
            ctk.CTkRadioButton(
                parent, text=label, variable=self.formato_saida,
                value=val, font=ctk.CTkFont(size=13),
                fg_color=COLORS["secondary"], hover_color=COLORS["primary"],
                border_color=COLORS["border"], text_color=COLORS["text"]
            ).pack(anchor="w", pady=3, padx=8)

    def _build_output_dir(self, parent):
        """Constrói a seção de pasta de saída."""
        ctk.CTkLabel(parent, text="💾  PASTA DE SAÍDA",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", pady=(0, 6))

        out_row = ctk.CTkFrame(parent, fg_color="transparent")
        out_row.pack(fill="x", pady=(0, 4))

        self.out_dir_entry = ctk.CTkEntry(
            out_row, textvariable=self.output_dir_var,
            placeholder_text="Mesma pasta do arquivo de entrada",
            fg_color=COLORS["card"], border_color=COLORS["border"],
            text_color=COLORS["text"], font=ctk.CTkFont(size=11),
        )
        self.out_dir_entry.pack(side="left", fill="x", expand=True, padx=(0, 4))

        ctk.CTkButton(
            out_row, text="📂", width=36, height=28,
            font=ctk.CTkFont(size=14),
            fg_color=COLORS["card"], hover_color=COLORS["card_hover"],
            border_width=1, border_color=COLORS["border"],
            corner_radius=6, command=self._on_browse_output
        ).pack(side="right")

    def _build_drop_zone(self, parent):
        """Constrói a zona de drag-and-drop para arquivo de entrada."""
        ctk.CTkLabel(parent, text="📁  ARQUIVO DE ENTRADA",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", pady=(0, 8))

        self.drop_zone = ctk.CTkFrame(parent, fg_color=COLORS["card"],
                                       corner_radius=10, height=100,
                                       border_width=2, border_color=COLORS["border"])
        self.drop_zone.pack(fill="x", pady=(0, 8))
        self.drop_zone.pack_propagate(False)

        self.drop_label = ctk.CTkLabel(
            self.drop_zone,
            text="Arraste o arquivo .xlsx / .csv aqui\nou clique para selecionar",
            font=ctk.CTkFont(size=12), text_color=COLORS["text_dim"],
            justify="center"
        )
        self.drop_label.pack(expand=True)

        self._register_drop_handlers()

        self.file_label = ctk.CTkLabel(parent, text="",
                                        font=ctk.CTkFont(size=11),
                                        text_color=COLORS["success"], wraplength=250)
        self.file_label.pack(anchor="w")

    def _register_drop_handlers(self):
        """Registra os handlers de drag-and-drop e click."""
        self.drop_zone.drop_target_register(DND_FILES)
        self.drop_zone.dnd_bind("<<Drop>>", self._on_drop)
        self.drop_label.drop_target_register(DND_FILES)
        self.drop_label.dnd_bind("<<Drop>>", self._on_drop)
        self.drop_zone.bind("<Button-1>", self._on_browse)
        self.drop_label.bind("<Button-1>", self._on_browse)

    def _build_process_button(self, parent):
        """Constrói o botão de processar (fixo no fundo do painel esquerdo)."""
        self.btn_processar = ctk.CTkButton(
            parent, text="▶  PROCESSAR",
            font=ctk.CTkFont(size=14, weight="bold"),
            fg_color=COLORS["secondary"], hover_color=COLORS["primary"],
            corner_radius=8, height=44, command=self._on_processar
        )
        self.btn_processar.pack(fill="x", padx=16, pady=(8, 16))

    def _build_right_panel(self, body):
        """Constrói o painel direito com log e cotações."""
        right = ctk.CTkFrame(body, fg_color=COLORS["surface"], corner_radius=12,
                             border_width=1, border_color=COLORS["border"])
        right.grid(row=0, column=1, sticky="nsew", padx=(8, 0))

        right_inner = ctk.CTkFrame(right, fg_color="transparent")
        right_inner.pack(fill="both", expand=True, padx=16, pady=16)

        ctk.CTkLabel(right_inner, text="📋  LOG DE PROCESSAMENTO",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", pady=(0, 8))

        self.log_text = ctk.CTkTextbox(right_inner, fg_color=COLORS["card"],
                                        text_color=COLORS["text"],
                                        font=ctk.CTkFont(family="Consolas", size=12),
                                        corner_radius=8, border_width=1,
                                        border_color=COLORS["border"],
                                        state="disabled", wrap="word")
        self.log_text.pack(fill="both", expand=True)

        self.cotacoes_frame = ctk.CTkFrame(right_inner, fg_color=COLORS["card"],
                                            corner_radius=8, border_width=1,
                                            border_color=COLORS["border"])

    # ============================================================
    # EVENTOS
    # ============================================================
    def _on_pais_change(self):
        """Mostra/oculta seções conforme país selecionado."""
        pais = self.pais_selecionado.get()
        # Gemini API (Argentina)
        if pais == "argentina":
            self.api_frame.pack(fill="x", pady=(12, 0), before=self._scroll_sentinel)
        else:
            self.api_frame.pack_forget()
        # Chile section
        if pais == "chile":
            self.chile_frame.pack(fill="x", pady=(12, 0), before=self._scroll_sentinel)
        else:
            self.chile_frame.pack_forget()
        # Peru section
        if pais == "peru":
            self.peru_frame.pack(fill="x", pady=(12, 0), before=self._scroll_sentinel)
        else:
            self.peru_frame.pack_forget()
        # Colômbia section
        if pais == "colombia":
            self.colombia_frame.pack(fill="x", pady=(12, 0), before=self._scroll_sentinel)
        else:
            self.colombia_frame.pack_forget()

    def _on_colombia_step_change(self):
        """Atualiza UI quando a etapa da Colômbia muda."""
        etapa = self.etapa_colombia.get()
        if etapa == "analise":
            self.colombia_step_desc.configure(
                text="Extrai PNs, marcas e quantidades via\n"
                     "regex + IA (Claude). Pipeline completo.")
            for w in self._colombia_key_widgets:
                w.pack(anchor="w", padx=0, pady=(0, 4))
        else:
            self.colombia_step_desc.configure(
                text="Explode linhas com múltiplos PNs em\n"
                     "linhas individuais (1 PN por linha).")
            for w in self._colombia_key_widgets:
                w.pack_forget()

    def _toggle_api_key_visibility(self):
        """Alterna visibilidade da API key."""
        self.api_key_visible = not self.api_key_visible
        if self.api_key_visible:
            self.api_entry.configure(show="")
            self.eye_btn.configure(text="🙈")
        else:
            self.api_entry.configure(show="•")
            self.eye_btn.configure(text="👁")

    def _on_drop(self, event):
        """Trata evento de drag-and-drop."""
        path = parse_drop_path(event)
        self._set_file(path)

    def _on_drop_chile_secondary(self, event):
        """Trata drag-and-drop do arquivo secundário do Chile."""
        path = parse_drop_path(event)
        self._set_chile_secondary_file(path)

    def _on_browse_chile_secondary(self, _event=None):
        """Abre diálogo para selecionar arquivo secundário do Chile."""
        path = browse_file(title="Selecionar arquivo secundário (PROCV)")
        if path:
            self._set_chile_secondary_file(path)

    def _set_chile_secondary_file(self, path):
        """Valida e define o arquivo secundário do Chile."""
        if not validate_file_path(path):
            messagebox.showwarning("Arquivo inválido",
                                   "Selecione um arquivo .xlsx, .xls ou .csv")
            return
        self.input_file_secondary = path
        nome = os.path.basename(path)
        self.chile_file_label.configure(text=f"✓ {nome}")
        self.chile_drop_label.configure(text=f"📄 {nome}")
        self.chile_drop_zone.configure(border_color=COLORS["success"])
        self._log(f"📄 Arquivo secundário (Chile): {nome}")

    def _on_browse_output(self):
        """Abre diálogo para selecionar pasta de saída."""
        path = browse_directory()
        if path:
            self.output_dir_var.set(path)
            self._log(f"💾 Pasta de saída: {path}")

    def _on_browse(self, _event=None):
        """Abre diálogo para selecionar arquivo de entrada."""
        path = browse_file()
        if path:
            self._set_file(path)

    def _set_file(self, path):
        """Valida e define o arquivo de entrada."""
        if not validate_file_path(path):
            messagebox.showwarning("Arquivo inválido",
                                   "Selecione um arquivo .xlsx, .xls ou .csv")
            return
        self.input_file = path
        nome = os.path.basename(path)
        self.file_label.configure(text=f"✓ {nome}")
        self.drop_label.configure(text=f"📄 {nome}")
        self.drop_zone.configure(border_color=COLORS["success"])
        self._log(f"📄 Arquivo selecionado: {nome}")

    # ============================================================
    # LOG
    # ============================================================
    def _log(self, msg):
        log_message(self.log_text, msg, self.root)

    def _clear_log(self):
        clear_log(self.log_text)

    # ============================================================
    # PROCESSAMENTO
    # ============================================================
    def _set_processing(self, active):
        """Altera estado visual de processamento."""
        self.processando = active
        state = "disabled" if active else "normal"
        self.root.after(0, lambda: self.btn_processar.configure(state=state))
        if active:
            self.root.after(0, lambda: self.progress.configure(mode="indeterminate"))
            self.root.after(0, lambda: self.progress.start())
        else:
            self.root.after(0, lambda: self.progress.stop())
            self.root.after(0, lambda: self.progress.configure(mode="determinate"))
            self.root.after(0, lambda: self.progress.set(1.0))

    def _on_processar(self):
        """Valida entrada e despacha processamento."""
        if self.processando:
            return
        erro = self._validar_entrada()
        if erro:
            messagebox.showwarning("Atenção", erro)
            return
        self._despachar_processamento()

    def _validar_entrada(self) -> str | None:
        """Valida se todos os campos obrigatórios estão preenchidos. Retorna erro ou None."""
        if not self.input_file or not os.path.exists(self.input_file):
            return "Selecione um arquivo válido primeiro."
        output_dir = self.output_dir_var.get().strip() or os.path.dirname(self.input_file)
        if not os.path.isdir(output_dir):
            return f"A pasta de saída não existe:\n{output_dir}"
        pais = self.pais_selecionado.get()
        if pais == "argentina":
            if not self.api_key_var.get().strip():
                return "Insira a API Key do Gemini."
        if pais == "chile":
            if not self.input_file_secondary or not os.path.exists(self.input_file_secondary):
                return "Selecione o arquivo secundário (PROCV importador) para o Chile."
        if pais == "colombia" and self.etapa_colombia.get() == "analise":
            if not self.api_key_claude_var.get().strip():
                return "Insira a API Key do Claude (Anthropic)."
        return None

    def _despachar_processamento(self):
        """Inicia o processamento no thread correto."""
        self._clear_log()
        self._hide_cotacoes()
        output_dir = self.output_dir_var.get().strip() or os.path.dirname(self.input_file)
        formato = self.formato_saida.get()
        pais = self.pais_selecionado.get()

        self._set_processing(True)
        self._set_status(f"Processando {pais.capitalize()}...")

        if pais == "equador":
            self._iniciar_equador(output_dir, formato)
        elif pais == "argentina":
            self._iniciar_argentina(output_dir, formato)
        elif pais == "chile":
            self._iniciar_chile(output_dir, formato)
        elif pais == "peru":
            self._iniciar_peru(output_dir, formato)
        elif pais == "colombia":
            self._iniciar_colombia(output_dir)

    def _iniciar_equador(self, output_dir, formato):
        """Inicia thread de processamento do Equador."""
        t = threading.Thread(target=processar_equador, daemon=True,
                             args=(self.input_file, output_dir, formato,
                                   self._log, self._done))
        t.start()

    def _iniciar_argentina(self, output_dir, formato):
        """Inicia thread de processamento da Argentina."""
        api_key = self.api_key_var.get().strip()
        ano = self.ano_cotacao.get()
        t = threading.Thread(target=processar_argentina, daemon=True,
                             args=(self.input_file, output_dir, formato, api_key,
                                   ano, self._log, self._done, self._show_cotacoes))
        t.start()

    def _iniciar_chile(self, output_dir, formato):
        """Inicia thread de processamento do Chile."""
        api_key = self.api_key_var.get().strip()
        t = threading.Thread(target=processar_chile, daemon=True,
                             args=(self.input_file, self.input_file_secondary,
                                   output_dir, formato, api_key,
                                   self._log, self._done))
        t.start()

    def _iniciar_peru(self, output_dir, formato):
        """Inicia thread de processamento do Peru."""
        api_key = self.api_key_var.get().strip()
        t = threading.Thread(target=processar_peru, daemon=True,
                             args=(self.input_file, output_dir, formato,
                                   api_key, self._log, self._done))
        t.start()

    def _iniciar_colombia(self, output_dir):
        """Inicia thread de processamento da Colômbia."""
        etapa = self.etapa_colombia.get()
        if etapa == "analise":
            api_key = self.api_key_claude_var.get().strip()
            t = threading.Thread(target=self._executar_colombia_analise, daemon=True,
                                 args=(output_dir, api_key))
        else:
            t = threading.Thread(target=self._executar_colombia_explosao, daemon=True,
                                 args=(output_dir,))
        t.start()

    def _executar_colombia_analise(self, output_dir, api_key):
        """Executa análise de PNs (Colômbia) em thread."""
        try:
            processar_analise(
                path_in=self.input_file,
                output_dir=output_dir,
                api_key=api_key,
                log_callback=self._log,
            )
            self._done(True)
        except Exception as e:
            self._log(f"\n❌ Erro: {e}")
            self._done(False)

    def _executar_colombia_explosao(self, output_dir):
        """Executa explosão de PNs (Colômbia) em thread."""
        try:
            processar_explosao(
                path_in=self.input_file,
                output_dir=output_dir,
                log_callback=self._log,
            )
            self._done(True)
        except Exception as e:
            self._log(f"\n❌ Erro: {e}")
            self._done(False)

    def _done(self, success):
        """Callback de conclusão do processamento."""
        self._set_processing(False)
        if success:
            self._set_status("✅ Concluído com sucesso!", 1.0)
            self._log("\n✅ PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
            self.root.after(0, lambda: messagebox.showinfo(
                "Sucesso", "Arquivo processado e salvo com sucesso!"))
        else:
            self._set_status("❌ Erro no processamento", 0)

    # ============================================================
    # COTAÇÕES (Argentina)
    # ============================================================
    def _show_cotacoes(self, taxas, moedas_outras, df, output_dir, formato):
        """Salva estado de cotações e agenda construção da UI."""
        self._salvar_estado_cotacoes(taxas, moedas_outras, df, output_dir, formato)
        self.root.after(0, self._construir_ui_cotacoes)

    def _salvar_estado_cotacoes(self, taxas, moedas_outras, df, output_dir, formato):
        """Persiste os dados de cotação para uso na confirmação."""
        self._arg_df = df
        self._arg_taxas = dict(taxas)
        self._arg_moedas = moedas_outras
        self._arg_output_dir = output_dir
        self._arg_formato = formato

    def _construir_ui_cotacoes(self):
        """Constrói a interface de revisão de cotações."""
        self._set_processing(False)
        self._set_status("Aguardando revisão de cotações...")

        for w in self.cotacoes_frame.winfo_children():
            w.destroy()
        self._cotacao_entries = {}

        self.cotacoes_frame.pack(fill="x", pady=(12, 0))

        self._build_cotacoes_header()
        self._build_cotacoes_entries()
        self._build_cotacoes_confirm_button()

    def _build_cotacoes_header(self):
        """Constrói o cabeçalho da seção de cotações."""
        ctk.CTkLabel(self.cotacoes_frame, text="💱  REVISÃO DE COTAÇÕES",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["warning"]).pack(anchor="w", padx=12, pady=(12, 4))

        ctk.CTkLabel(self.cotacoes_frame,
                     text="Edite os valores abaixo se necessário e clique em Confirmar.",
                     font=ctk.CTkFont(size=11),
                     text_color=COLORS["text_dim"]).pack(anchor="w", padx=12, pady=(0, 8))

    def _build_cotacoes_entries(self):
        """Constrói os campos de entrada para cada moeda."""
        for moeda in self._arg_moedas:
            row = ctk.CTkFrame(self.cotacoes_frame, fg_color="transparent")
            row.pack(fill="x", padx=12, pady=3)

            ctk.CTkLabel(row, text=f"1 {moeda} =",
                         font=ctk.CTkFont(size=12),
                         text_color=COLORS["text"], width=200).pack(side="left")

            entry = ctk.CTkEntry(row, fg_color=COLORS["surface"],
                                  border_color=COLORS["border"],
                                  text_color=COLORS["text"],
                                  font=ctk.CTkFont(size=12), width=140)
            entry.pack(side="left", padx=6)
            valor_atual = self._arg_taxas.get(moeda, 0.0)
            entry.insert(0, str(valor_atual))
            self._cotacao_entries[moeda] = entry

            ctk.CTkLabel(row, text="USD", font=ctk.CTkFont(size=12),
                         text_color=COLORS["text_dim"]).pack(side="left")

    def _build_cotacoes_confirm_button(self):
        """Constrói o botão de confirmação de cotações."""
        btn_frame = ctk.CTkFrame(self.cotacoes_frame, fg_color="transparent")
        btn_frame.pack(fill="x", padx=12, pady=(8, 12))

        ctk.CTkButton(btn_frame, text="✓  Confirmar e Gerar Arquivo",
                      font=ctk.CTkFont(size=13, weight="bold"),
                      fg_color=COLORS["success"], hover_color="#4CAF50",
                      corner_radius=8, height=38,
                      command=self._confirmar_cotacoes).pack(fill="x")

    def _hide_cotacoes(self):
        """Oculta o painel de cotações."""
        self.cotacoes_frame.pack_forget()

    def _confirmar_cotacoes(self):
        """Valida cotações e inicia finalização da Argentina."""
        taxas_finais = self._parsear_entradas_cotacoes()
        if taxas_finais is None:
            return
        self._log_cotacoes_finais(taxas_finais)
        self._iniciar_finalizacao_argentina(taxas_finais)

    def _parsear_entradas_cotacoes(self) -> dict | None:
        """Converte os campos de cotação para float. Retorna None em caso de erro."""
        taxas = {}
        for moeda, entry in self._cotacao_entries.items():
            try:
                val = float(entry.get().strip().replace(",", "."))
                taxas[moeda] = val
            except ValueError:
                messagebox.showwarning("Valor inválido",
                                       f"Valor inválido para {moeda}. Use número decimal.")
                return None
        return taxas

    def _log_cotacoes_finais(self, taxas_finais: dict):
        """Registra no log as cotações confirmadas."""
        self._log("\n📈 Cotações finais confirmadas:")
        for moeda, taxa in taxas_finais.items():
            self._log(f"    1 {moeda} = {taxa} USD")

    def _iniciar_finalizacao_argentina(self, taxas_finais: dict):
        """Inicia thread de finalização com as cotações confirmadas."""
        self._hide_cotacoes()
        self._set_processing(True)
        self._set_status("Finalizando Argentina...")

        base_name = os.path.splitext(os.path.basename(self.input_file))[0]
        t = threading.Thread(target=finalizar_argentina, daemon=True,
                             args=(self._arg_df, taxas_finais, self._arg_output_dir,
                                   self._arg_formato, base_name,
                                   self._log, self._done))
        t.start()
