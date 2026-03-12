"""
Tab Normalização — Interface para normalização de marcas e partnumbers.
Cada método faz uma única coisa.
"""

import os
import threading

import customtkinter as ctk
import pandas as pd
from tkinter import filedialog, messagebox
from tkinterdnd2 import DND_FILES

from config.colors import COLORS
from infrastructure.file_io import ler_arquivo
from domain.normalizacao import processar_normalizacao
from ui.components import (
    parse_drop_path, validate_file_path, browse_file, browse_directory,
    log_message, clear_log,
)


class TabNormalizacao:
    """Gerencia a aba de Normalização."""

    def __init__(self, tab_frame, root, status_callback, progress_bar):
        self.root = root
        self._set_status = status_callback
        self.progress = progress_bar

        # Estado
        self.norm_input_file = None
        self.norm_rules_file = None
        self.norm_method = ctk.StringVar(value="excel")
        self.norm_formato = ctk.StringVar(value="xlsx")
        self.norm_output_dir = ctk.StringVar(value="")
        self.norm_regras_manual = []
        self.norm_processando = False
        self.norm_pn_conv_enabled = ctk.BooleanVar(value=False)
        self.norm_pn_conversions = []
        self.norm_pn_conv_file = None

        self._build(tab_frame)

    # ============================================================
    # BUILD (orquestrador)
    # ============================================================
    def _build(self, tab):
        """Orquestra a construção de todos os painéis."""
        tab.grid_columnconfigure(0, weight=1)
        tab.grid_columnconfigure(1, weight=2)
        tab.grid_rowconfigure(0, weight=1)

        left = ctk.CTkScrollableFrame(tab, fg_color=COLORS["surface"], corner_radius=12,
                                       border_width=1, border_color=COLORS["border"])
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 8))

        self._build_method_selector(left)
        self._build_separator(left)
        self._build_data_drop(left)
        self._build_separator(left)
        self._build_method_container(left)
        self._build_template_button(left)
        self._build_separator(left)
        self._build_pn_conversion_section(left)
        self._build_separator(left)
        self._build_format_selector(left)
        self._build_separator(left)
        self._build_output_dir(left)
        self._build_process_button(left)
        self._build_right_panel(tab)

    # ============================================================
    # SUB-BUILDERS
    # ============================================================
    def _build_separator(self, parent):
        """Adiciona separador horizontal."""
        ctk.CTkFrame(parent, fg_color=COLORS["border"], height=1).pack(fill="x", padx=16, pady=12)

    def _build_method_selector(self, parent):
        """Constrói seletor de método de entrada (Excel / APP)."""
        ctk.CTkLabel(parent, text="📋  MÉTODO DE ENTRADA",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", padx=16, pady=(16, 8))

        for val, label in [("excel", "📂  Via Excel (arquivo de regras)"),
                           ("app", "✏️  Via APP (entrada manual)")]:
            ctk.CTkRadioButton(
                parent, text=label, variable=self.norm_method,
                value=val, font=ctk.CTkFont(size=13),
                fg_color=COLORS["secondary"], hover_color=COLORS["primary"],
                border_color=COLORS["border"], text_color=COLORS["text"],
                command=self._on_norm_method_change
            ).pack(anchor="w", pady=3, padx=24)

    def _build_data_drop(self, parent):
        """Constrói zona de drop para arquivo de dados."""
        ctk.CTkLabel(parent, text="📁  ARQUIVO DE DADOS",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", padx=16, pady=(0, 4))
        ctk.CTkLabel(parent, text="Excel ou CSV (sep. |) com colunas MARCA e PARTNUMBER",
                     font=ctk.CTkFont(size=11), text_color=COLORS["text_dim"]
                     ).pack(anchor="w", padx=16, pady=(0, 8))

        self.norm_data_drop = ctk.CTkFrame(parent, fg_color=COLORS["card"],
                                            corner_radius=10, height=70,
                                            border_width=2, border_color=COLORS["border"])
        self.norm_data_drop.pack(fill="x", padx=16, pady=(0, 4))
        self.norm_data_drop.pack_propagate(False)

        self.norm_data_label = ctk.CTkLabel(
            self.norm_data_drop, text="Arraste ou clique para selecionar",
            font=ctk.CTkFont(size=11), text_color=COLORS["text_dim"], justify="center"
        )
        self.norm_data_label.pack(expand=True)

        self._register_data_drop_handlers()

        self.norm_data_file_label = ctk.CTkLabel(parent, text="", font=ctk.CTkFont(size=11),
                                                  text_color=COLORS["success"], wraplength=250)
        self.norm_data_file_label.pack(anchor="w", padx=16)

    def _register_data_drop_handlers(self):
        """Registra handlers de drag-and-drop para arquivo de dados."""
        self.norm_data_drop.drop_target_register(DND_FILES)
        self.norm_data_drop.dnd_bind("<<Drop>>", self._on_norm_data_drop)
        self.norm_data_label.drop_target_register(DND_FILES)
        self.norm_data_label.dnd_bind("<<Drop>>", self._on_norm_data_drop)
        self.norm_data_drop.bind("<Button-1>", self._on_norm_data_browse)
        self.norm_data_label.bind("<Button-1>", self._on_norm_data_browse)

    def _build_method_container(self, parent):
        """Constrói container para conteúdo específico de cada método."""
        self.norm_method_container = ctk.CTkFrame(parent, fg_color="transparent")
        self.norm_method_container.pack(fill="x")

        self._build_excel_method_frame()
        self._build_app_method_frame()

    def _build_excel_method_frame(self):
        """Constrói frame para método via Excel."""
        self.norm_excel_frame = ctk.CTkFrame(self.norm_method_container, fg_color="transparent")
        self.norm_excel_frame.pack(fill="x")

        ctk.CTkLabel(self.norm_excel_frame, text="📁  ARQUIVO DE REGRAS",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", padx=16, pady=(0, 4))
        ctk.CTkLabel(self.norm_excel_frame, text="Excel com colunas MARCA e PARTNUMBER",
                     font=ctk.CTkFont(size=11), text_color=COLORS["text_dim"]
                     ).pack(anchor="w", padx=16, pady=(0, 8))

        self.norm_rules_drop = ctk.CTkFrame(self.norm_excel_frame, fg_color=COLORS["card"],
                                             corner_radius=10, height=70,
                                             border_width=2, border_color=COLORS["border"])
        self.norm_rules_drop.pack(fill="x", padx=16, pady=(0, 4))
        self.norm_rules_drop.pack_propagate(False)

        self.norm_rules_label = ctk.CTkLabel(
            self.norm_rules_drop, text="Arraste ou clique para selecionar",
            font=ctk.CTkFont(size=11), text_color=COLORS["text_dim"], justify="center"
        )
        self.norm_rules_label.pack(expand=True)

        self._register_rules_drop_handlers()

        self.norm_rules_file_label = ctk.CTkLabel(self.norm_excel_frame, text="",
                                                   font=ctk.CTkFont(size=11),
                                                   text_color=COLORS["success"], wraplength=250)
        self.norm_rules_file_label.pack(anchor="w", padx=16)

    def _register_rules_drop_handlers(self):
        """Registra handlers de drag-and-drop para arquivo de regras."""
        self.norm_rules_drop.drop_target_register(DND_FILES)
        self.norm_rules_drop.dnd_bind("<<Drop>>", self._on_norm_rules_drop)
        self.norm_rules_label.drop_target_register(DND_FILES)
        self.norm_rules_label.dnd_bind("<<Drop>>", self._on_norm_rules_drop)
        self.norm_rules_drop.bind("<Button-1>", self._on_norm_rules_browse)
        self.norm_rules_label.bind("<Button-1>", self._on_norm_rules_browse)

    def _build_app_method_frame(self):
        """Constrói frame para método via entrada manual."""
        self.norm_app_frame = ctk.CTkFrame(self.norm_method_container, fg_color="transparent")
        # Inicialmente oculto

        ctk.CTkLabel(self.norm_app_frame, text="✏️  ENTRADA MANUAL",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", padx=16, pady=(0, 8))

        self._build_manual_entry_fields()
        self._build_add_rule_button()
        self._build_rules_list()

    def _build_manual_entry_fields(self):
        """Constrói campos de entrada manual de marca e partnumber."""
        ctk.CTkLabel(self.norm_app_frame, text="MARCA:",
                     font=ctk.CTkFont(size=12), text_color=COLORS["text"]
                     ).pack(anchor="w", padx=16, pady=(0, 2))
        self.norm_marca_entry = ctk.CTkEntry(
            self.norm_app_frame, placeholder_text="Ex: HELLA",
            fg_color=COLORS["card"], border_color=COLORS["border"],
            text_color=COLORS["text"], font=ctk.CTkFont(size=12)
        )
        self.norm_marca_entry.pack(fill="x", padx=16, pady=(0, 8))

        ctk.CTkLabel(self.norm_app_frame, text="PARTNUMBER (opcional):",
                     font=ctk.CTkFont(size=12), text_color=COLORS["text"]
                     ).pack(anchor="w", padx=16, pady=(0, 2))
        self.norm_pn_entry = ctk.CTkEntry(
            self.norm_app_frame, placeholder_text="Ex: P559000 (vazio = só marca)",
            fg_color=COLORS["card"], border_color=COLORS["border"],
            text_color=COLORS["text"], font=ctk.CTkFont(size=12)
        )
        self.norm_pn_entry.pack(fill="x", padx=16, pady=(0, 8))

    def _build_add_rule_button(self):
        """Constrói botão de adicionar regra."""
        ctk.CTkButton(
            self.norm_app_frame, text="+  Adicionar Regra",
            font=ctk.CTkFont(size=12), fg_color=COLORS["primary"],
            hover_color=COLORS["secondary"], corner_radius=6, height=32,
            command=self._add_norm_rule
        ).pack(fill="x", padx=16, pady=(0, 8))

    def _build_rules_list(self):
        """Constrói a lista visual de regras adicionadas."""
        self.norm_rules_list_frame = ctk.CTkFrame(self.norm_app_frame, fg_color=COLORS["card"],
                                                   corner_radius=8, border_width=1,
                                                   border_color=COLORS["border"])
        self.norm_rules_list_frame.pack(fill="x", padx=16, pady=(0, 4))

        ctk.CTkLabel(
            self.norm_rules_list_frame, text="Nenhuma regra adicionada",
            font=ctk.CTkFont(size=11), text_color=COLORS["text_dim"]
        ).pack(padx=12, pady=8)

    def _build_pn_conversion_section(self, parent):
        """Constrói a seção de conversão de partnumber."""
        self._build_pn_conv_checkbox(parent)
        self._build_pn_conv_frame(parent)

    def _build_pn_conv_checkbox(self, parent):
        """Constrói checkbox de ativação de conversão PN."""
        self.norm_pn_conv_check = ctk.CTkCheckBox(
            parent, text="🔄  CONVERSÃO DE PARTNUMBER",
            variable=self.norm_pn_conv_enabled,
            font=ctk.CTkFont(size=14, weight="bold"),
            fg_color=COLORS["secondary"], hover_color=COLORS["primary"],
            border_color=COLORS["border"], text_color=COLORS["info"],
            command=self._on_pn_conv_toggle
        )
        self.norm_pn_conv_check.pack(anchor="w", padx=16, pady=(0, 4))

        ctk.CTkLabel(parent, text="Converte um partnumber específico em outro",
                     font=ctk.CTkFont(size=11), text_color=COLORS["text_dim"]
                     ).pack(anchor="w", padx=16, pady=(0, 8))

    def _build_pn_conv_frame(self, parent):
        """Constrói frame de conversão PN (inicialmente oculto)."""
        self.norm_pn_conv_frame = ctk.CTkFrame(parent, fg_color="transparent")

        # Sub-frame para modo Excel (drop de arquivo de conversões)
        self.norm_pn_conv_excel_sub = ctk.CTkFrame(self.norm_pn_conv_frame, fg_color="transparent")
        self._build_pn_conv_excel_content()

        # Sub-frame para modo APP (entrada manual)
        self.norm_pn_conv_app_sub = ctk.CTkFrame(self.norm_pn_conv_frame, fg_color="transparent")
        self._build_pn_conv_entries()
        self._build_pn_conv_add_button()
        self._build_pn_conv_list()

        # Mostrar sub-frame correto baseado no método selecionado
        self._update_pn_conv_view()

        self._norm_pn_conv_separator = ctk.CTkFrame(parent, fg_color=COLORS["border"], height=1)
        self._norm_pn_conv_separator.pack(fill="x", padx=16, pady=12)

    def _build_pn_conv_entries(self):
        """Constrói campos de entrada de PN original e novo."""
        ctk.CTkLabel(self.norm_pn_conv_app_sub, text="PN ORIGINAL:",
                     font=ctk.CTkFont(size=12), text_color=COLORS["text"]
                     ).pack(anchor="w", padx=16, pady=(0, 2))
        self.norm_pn_de_entry = ctk.CTkEntry(
            self.norm_pn_conv_app_sub, placeholder_text="Ex: ABC123",
            fg_color=COLORS["card"], border_color=COLORS["border"],
            text_color=COLORS["text"], font=ctk.CTkFont(size=12)
        )
        self.norm_pn_de_entry.pack(fill="x", padx=16, pady=(0, 8))

        ctk.CTkLabel(self.norm_pn_conv_app_sub, text="PN NOVO:",
                     font=ctk.CTkFont(size=12), text_color=COLORS["text"]
                     ).pack(anchor="w", padx=16, pady=(0, 2))
        self.norm_pn_para_entry = ctk.CTkEntry(
            self.norm_pn_conv_app_sub, placeholder_text="Ex: XYZ456",
            fg_color=COLORS["card"], border_color=COLORS["border"],
            text_color=COLORS["text"], font=ctk.CTkFont(size=12)
        )
        self.norm_pn_para_entry.pack(fill="x", padx=16, pady=(0, 8))

    def _build_pn_conv_add_button(self):
        """Constrói botão de adicionar conversão PN."""
        ctk.CTkButton(
            self.norm_pn_conv_app_sub, text="+  Adicionar Conversão",
            font=ctk.CTkFont(size=12), fg_color=COLORS["primary"],
            hover_color=COLORS["secondary"], corner_radius=6, height=32,
            command=self._add_pn_conversion
        ).pack(fill="x", padx=16, pady=(0, 8))

    def _build_pn_conv_list(self):
        """Constrói lista visual de conversões PN."""
        self.norm_pn_conv_list_frame = ctk.CTkFrame(self.norm_pn_conv_app_sub, fg_color=COLORS["card"],
                                                     corner_radius=8, border_width=1,
                                                     border_color=COLORS["border"])
        self.norm_pn_conv_list_frame.pack(fill="x", padx=16, pady=(0, 4))

        ctk.CTkLabel(
            self.norm_pn_conv_list_frame, text="Nenhuma conversão adicionada",
            font=ctk.CTkFont(size=11), text_color=COLORS["text_dim"]
        ).pack(padx=12, pady=8)

    def _build_template_button(self, parent):
        """Constrói botão para baixar modelo base (visível apenas no modo Excel)."""
        self._template_container = ctk.CTkFrame(parent, fg_color="transparent")
        self._template_container.pack(fill="x")

        self.norm_template_btn = ctk.CTkButton(
            self._template_container, text="📥  Baixar Modelo Base (MARCA / PARTNUMBER)",
            font=ctk.CTkFont(size=12), fg_color=COLORS["primary"],
            hover_color=COLORS["secondary"], corner_radius=6, height=32,
            command=self._download_normalizacao_template
        )
        self.norm_template_btn.pack(fill="x", padx=16, pady=(8, 4))

    def _build_format_selector(self, parent):
        """Constrói seção de seleção de formato de saída."""
        ctk.CTkLabel(parent, text="📄  FORMATO DE SAÍDA",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", padx=16, pady=(0, 8))

        for val, label in [("xlsx", "XLSX (Excel)"), ("csv", "CSV (separado por |)"),
                           ("ambos", "Ambos")]:
            ctk.CTkRadioButton(
                parent, text=label, variable=self.norm_formato,
                value=val, font=ctk.CTkFont(size=13),
                fg_color=COLORS["secondary"], hover_color=COLORS["primary"],
                border_color=COLORS["border"], text_color=COLORS["text"]
            ).pack(anchor="w", pady=3, padx=24)

    def _build_output_dir(self, parent):
        """Constrói seção de pasta de saída."""
        ctk.CTkLabel(parent, text="💾  PASTA DE SAÍDA",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", padx=16, pady=(0, 6))

        norm_out_row = ctk.CTkFrame(parent, fg_color="transparent")
        norm_out_row.pack(fill="x", padx=16, pady=(0, 4))

        self.norm_out_entry = ctk.CTkEntry(
            norm_out_row, textvariable=self.norm_output_dir,
            placeholder_text="Mesma pasta do arquivo de dados",
            fg_color=COLORS["card"], border_color=COLORS["border"],
            text_color=COLORS["text"], font=ctk.CTkFont(size=11),
        )
        self.norm_out_entry.pack(side="left", fill="x", expand=True, padx=(0, 4))

        ctk.CTkButton(
            norm_out_row, text="📂", width=36, height=28,
            font=ctk.CTkFont(size=14),
            fg_color=COLORS["card"], hover_color=COLORS["card_hover"],
            border_width=1, border_color=COLORS["border"],
            corner_radius=6, command=self._on_norm_browse_output
        ).pack(side="right")

    def _build_process_button(self, parent):
        """Constrói botão de normalizar."""
        self.norm_btn_processar = ctk.CTkButton(
            parent, text="▶  NORMALIZAR", font=ctk.CTkFont(size=14, weight="bold"),
            fg_color=COLORS["secondary"], hover_color=COLORS["primary"],
            corner_radius=8, height=44, command=self._on_normalizar
        )
        self.norm_btn_processar.pack(fill="x", padx=16, pady=(12, 16))

    def _build_right_panel(self, tab):
        """Constrói painel direito com log."""
        right = ctk.CTkFrame(tab, fg_color=COLORS["surface"], corner_radius=12,
                              border_width=1, border_color=COLORS["border"])
        right.grid(row=0, column=1, sticky="nsew", padx=(8, 0))

        right_inner = ctk.CTkFrame(right, fg_color="transparent")
        right_inner.pack(fill="both", expand=True, padx=16, pady=16)

        ctk.CTkLabel(right_inner, text="📋  LOG DE NORMALIZAÇÃO",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", pady=(0, 8))

        self.norm_log_text = ctk.CTkTextbox(right_inner, fg_color=COLORS["card"],
                                             text_color=COLORS["text"],
                                             font=ctk.CTkFont(family="Consolas", size=12),
                                             corner_radius=8, border_width=1,
                                             border_color=COLORS["border"],
                                             state="disabled", wrap="word")
        self.norm_log_text.pack(fill="both", expand=True)

    # ============================================================
    # EVENTOS
    # ============================================================
    def _on_norm_method_change(self):
        """Alterna entre modo Excel e modo APP."""
        if self.norm_method.get() == "excel":
            self.norm_app_frame.pack_forget()
            self.norm_excel_frame.pack(fill="x")
            self.norm_template_btn.pack(fill="x", padx=16, pady=(8, 4))
        else:
            self.norm_excel_frame.pack_forget()
            self.norm_app_frame.pack(fill="x")
            self.norm_template_btn.pack_forget()
        self._update_pn_conv_view()

    def _on_norm_data_drop(self, event):
        """Trata drop de arquivo de dados."""
        path = parse_drop_path(event)
        self._set_norm_data_file(path)

    def _on_norm_data_browse(self, _event=None):
        """Abre diálogo para selecionar arquivo de dados."""
        path = browse_file()
        if path:
            self._set_norm_data_file(path)

    def _set_norm_data_file(self, path):
        """Valida e define o arquivo de dados."""
        if not validate_file_path(path):
            messagebox.showwarning("Arquivo inválido", "Selecione um arquivo .xlsx, .xls ou .csv")
            return
        self.norm_input_file = path
        nome = os.path.basename(path)
        self.norm_data_file_label.configure(text=f"✓ {nome}")
        self.norm_data_label.configure(text=f"📄 {nome}")
        self.norm_data_drop.configure(border_color=COLORS["success"])
        self._log(f"📄 Arquivo de dados: {nome}")

    def _on_norm_rules_drop(self, event):
        """Trata drop de arquivo de regras."""
        path = parse_drop_path(event)
        self._set_norm_rules_file(path)

    def _on_norm_rules_browse(self, _event=None):
        """Abre diálogo para selecionar arquivo de regras."""
        path = browse_file()
        if path:
            self._set_norm_rules_file(path)

    def _set_norm_rules_file(self, path):
        """Valida e define o arquivo de regras."""
        if not validate_file_path(path):
            messagebox.showwarning("Arquivo inválido", "Selecione um arquivo .xlsx, .xls ou .csv")
            return
        self.norm_rules_file = path
        nome = os.path.basename(path)
        self.norm_rules_file_label.configure(text=f"✓ {nome}")
        self.norm_rules_label.configure(text=f"📄 {nome}")
        self.norm_rules_drop.configure(border_color=COLORS["success"])
        self._log(f"📋 Arquivo de regras: {nome}")

    def _on_norm_browse_output(self):
        """Abre diálogo para selecionar pasta de saída."""
        path = browse_directory()
        if path:
            self.norm_output_dir.set(path)
            self._log(f"💾 Pasta de saída: {path}")

    # ============================================================
    # REGRAS MANUAIS
    # ============================================================
    def _add_norm_rule(self):
        """Adiciona uma regra manual de normalização."""
        marca = self.norm_marca_entry.get().strip()
        if not marca:
            messagebox.showwarning("Atenção", "Informe a MARCA.")
            return
        pn = self.norm_pn_entry.get().strip()
        self.norm_regras_manual.append({"marca": marca.upper(), "partnumber": pn.upper()})
        self.norm_marca_entry.delete(0, "end")
        self.norm_pn_entry.delete(0, "end")
        self._refresh_norm_rules_list()

    def _remove_norm_rule(self, idx):
        """Remove uma regra manual pelo índice."""
        if 0 <= idx < len(self.norm_regras_manual):
            self.norm_regras_manual.pop(idx)
            self._refresh_norm_rules_list()

    def _refresh_norm_rules_list(self):
        """Atualiza a lista visual de regras manuais."""
        for w in self.norm_rules_list_frame.winfo_children():
            w.destroy()
        if not self.norm_regras_manual:
            ctk.CTkLabel(
                self.norm_rules_list_frame, text="Nenhuma regra adicionada",
                font=ctk.CTkFont(size=11), text_color=COLORS["text_dim"]
            ).pack(padx=12, pady=8)
            return
        for i, regra in enumerate(self.norm_regras_manual):
            self._render_rule_row(i, regra)

    def _render_rule_row(self, idx, regra):
        """Renderiza uma linha de regra na lista visual."""
        row = ctk.CTkFrame(self.norm_rules_list_frame, fg_color="transparent")
        row.pack(fill="x", padx=8, pady=2)
        if regra["partnumber"]:
            texto = f"🔧 {regra['marca']} / {regra['partnumber']}"
        else:
            texto = f"🏷️ {regra['marca']}"
        ctk.CTkLabel(row, text=texto, font=ctk.CTkFont(size=11),
                     text_color=COLORS["text"]).pack(side="left", padx=4)
        ctk.CTkButton(
            row, text="🗑", width=28, height=24, font=ctk.CTkFont(size=11),
            fg_color=COLORS["error"], hover_color="#D32F2F",
            corner_radius=4, command=lambda i=idx: self._remove_norm_rule(i)
        ).pack(side="right", padx=4)

    # ============================================================
    # CONVERSÃO PN
    # ============================================================
    def _on_pn_conv_toggle(self):
        """Mostra/oculta seção de conversão de PN."""
        if self.norm_pn_conv_enabled.get():
            self.norm_pn_conv_frame.pack(fill="x", before=self._norm_pn_conv_separator)
            self._update_pn_conv_view()
        else:
            self.norm_pn_conv_frame.pack_forget()

    def _add_pn_conversion(self):
        """Adiciona uma conversão de PN."""
        pn_de = self.norm_pn_de_entry.get().strip()
        pn_para = self.norm_pn_para_entry.get().strip()
        if not pn_de:
            messagebox.showwarning("Atenção", "Informe o PN original.")
            return
        if not pn_para:
            messagebox.showwarning("Atenção", "Informe o PN novo.")
            return
        self.norm_pn_conversions.append({"de": pn_de.upper(), "para": pn_para.upper()})
        self.norm_pn_de_entry.delete(0, "end")
        self.norm_pn_para_entry.delete(0, "end")
        self._refresh_pn_conv_list()

    def _remove_pn_conversion(self, idx):
        """Remove uma conversão de PN pelo índice."""
        if 0 <= idx < len(self.norm_pn_conversions):
            self.norm_pn_conversions.pop(idx)
            self._refresh_pn_conv_list()

    def _refresh_pn_conv_list(self):
        """Atualiza a lista visual de conversões de PN."""
        for w in self.norm_pn_conv_list_frame.winfo_children():
            w.destroy()
        if not self.norm_pn_conversions:
            ctk.CTkLabel(
                self.norm_pn_conv_list_frame, text="Nenhuma conversão adicionada",
                font=ctk.CTkFont(size=11), text_color=COLORS["text_dim"]
            ).pack(padx=12, pady=8)
            return
        for i, conv in enumerate(self.norm_pn_conversions):
            self._render_pn_conv_row(i, conv)

    def _render_pn_conv_row(self, idx, conv):
        """Renderiza uma linha de conversão PN na lista visual."""
        row = ctk.CTkFrame(self.norm_pn_conv_list_frame, fg_color="transparent")
        row.pack(fill="x", padx=8, pady=2)
        texto = f"🔄 {conv['de']} → {conv['para']}"
        ctk.CTkLabel(row, text=texto, font=ctk.CTkFont(size=11),
                     text_color=COLORS["text"]).pack(side="left", padx=4)
        ctk.CTkButton(
            row, text="🗑", width=28, height=24, font=ctk.CTkFont(size=11),
            fg_color=COLORS["error"], hover_color="#D32F2F",
            corner_radius=4, command=lambda i=idx: self._remove_pn_conversion(i)
        ).pack(side="right", padx=4)

    # ============================================================
    # CONVERSÃO PN — EXCEL
    # ============================================================
    def _build_pn_conv_excel_content(self):
        """Constrói conteúdo do sub-frame Excel para conversão de PN."""
        sub = self.norm_pn_conv_excel_sub

        ctk.CTkLabel(sub, text="📁  ARQUIVO DE CONVERSÕES PN",
                     font=ctk.CTkFont(size=13, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", padx=16, pady=(0, 4))
        ctk.CTkLabel(sub, text="Excel com colunas PN ORIGINAL e PN NOVO",
                     font=ctk.CTkFont(size=11), text_color=COLORS["text_dim"]
                     ).pack(anchor="w", padx=16, pady=(0, 8))

        self.norm_pn_conv_drop = ctk.CTkFrame(sub, fg_color=COLORS["card"],
                                               corner_radius=10, height=70,
                                               border_width=2, border_color=COLORS["border"])
        self.norm_pn_conv_drop.pack(fill="x", padx=16, pady=(0, 4))
        self.norm_pn_conv_drop.pack_propagate(False)

        self.norm_pn_conv_drop_label = ctk.CTkLabel(
            self.norm_pn_conv_drop, text="Arraste ou clique para selecionar",
            font=ctk.CTkFont(size=11), text_color=COLORS["text_dim"], justify="center"
        )
        self.norm_pn_conv_drop_label.pack(expand=True)

        self.norm_pn_conv_drop.drop_target_register(DND_FILES)
        self.norm_pn_conv_drop.dnd_bind("<<Drop>>", self._on_pn_conv_file_drop)
        self.norm_pn_conv_drop_label.drop_target_register(DND_FILES)
        self.norm_pn_conv_drop_label.dnd_bind("<<Drop>>", self._on_pn_conv_file_drop)
        self.norm_pn_conv_drop.bind("<Button-1>", self._on_pn_conv_file_browse)
        self.norm_pn_conv_drop_label.bind("<Button-1>", self._on_pn_conv_file_browse)

        self.norm_pn_conv_file_label = ctk.CTkLabel(sub, text="",
                                                     font=ctk.CTkFont(size=11),
                                                     text_color=COLORS["success"], wraplength=250)
        self.norm_pn_conv_file_label.pack(anchor="w", padx=16)

        ctk.CTkButton(
            sub, text="📥  Baixar Modelo Conversão PN",
            font=ctk.CTkFont(size=12), fg_color=COLORS["primary"],
            hover_color=COLORS["secondary"], corner_radius=6, height=32,
            command=self._download_pn_conversion_template
        ).pack(fill="x", padx=16, pady=(8, 4))

    def _update_pn_conv_view(self):
        """Alterna entre sub-frames Excel e APP na conversão de PN."""
        if self.norm_method.get() == "excel":
            self.norm_pn_conv_app_sub.pack_forget()
            self.norm_pn_conv_excel_sub.pack(fill="x")
        else:
            self.norm_pn_conv_excel_sub.pack_forget()
            self.norm_pn_conv_app_sub.pack(fill="x")

    def _on_pn_conv_file_drop(self, event):
        """Trata drop de arquivo de conversões PN."""
        path = parse_drop_path(event)
        self._set_pn_conv_file(path)

    def _on_pn_conv_file_browse(self, _event=None):
        """Abre diálogo para selecionar arquivo de conversões PN."""
        path = browse_file()
        if path:
            self._set_pn_conv_file(path)

    def _set_pn_conv_file(self, path):
        """Valida e define o arquivo de conversões PN."""
        if not validate_file_path(path):
            messagebox.showwarning("Arquivo inválido", "Selecione um arquivo .xlsx, .xls ou .csv")
            return
        self.norm_pn_conv_file = path
        nome = os.path.basename(path)
        self.norm_pn_conv_file_label.configure(text=f"✓ {nome}")
        self.norm_pn_conv_drop_label.configure(text=f"📄 {nome}")
        self.norm_pn_conv_drop.configure(border_color=COLORS["success"])
        self._log(f"🔄 Arquivo de conversões PN: {nome}")

    def _download_pn_conversion_template(self):
        """Salva modelo Excel para conversão de partnumber."""
        path = filedialog.asksaveasfilename(
            title="Salvar Modelo Conversão PN",
            defaultextension=".xlsx",
            filetypes=[("Excel", "*.xlsx")],
            initialfile="modelo_conversao_pn.xlsx"
        )
        if not path:
            return
        try:
            template_df = pd.DataFrame({
                "PN ORIGINAL": ["ABC123", "XYZ789"],
                "PN NOVO": ["DEF456", "UVW012"]
            })
            template_df.to_excel(path, index=False, engine="openpyxl")
            self._log(f"📥 Modelo conversão PN salvo: {path}")
            messagebox.showinfo("Modelo Salvo",
                                f"Modelo de conversão salvo em:\n{path}\n\n"
                                f"• PN ORIGINAL: partnumber atual no arquivo\n"
                                f"• PN NOVO: partnumber que substituirá o original")
        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao salvar modelo:\n{e}")

    # ============================================================
    # TEMPLATE
    # ============================================================
    def _download_normalizacao_template(self):
        """Salva modelo Excel de referência para normalização."""
        path = filedialog.asksaveasfilename(
            title="Salvar Modelo Base",
            defaultextension=".xlsx",
            filetypes=[("Excel", "*.xlsx")],
            initialfile="modelo_normalizacao.xlsx"
        )
        if not path:
            return
        try:
            template_df = pd.DataFrame({
                "MARCA": ["HELLA", "DONALDSON", "BOSCH"],
                "PARTNUMBER": ["", "P559000", ""]
            })
            template_df.to_excel(path, index=False, engine="openpyxl")
            self._log(f"📥 Modelo salvo: {path}")
            messagebox.showinfo("Modelo Salvo",
                                f"Modelo base salvo em:\n{path}\n\n"
                                f"• Linha com só MARCA: normaliza marcas que contêm o texto\n"
                                f"• Linha com MARCA + PARTNUMBER: se o PN existir no arquivo,\n"
                                f"  a marca será alterada para a MARCA da regra")
        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao salvar modelo:\n{e}")

    # ============================================================
    # LOG
    # ============================================================
    def _log(self, msg):
        log_message(self.norm_log_text, msg, self.root)

    def _clear_log(self):
        clear_log(self.norm_log_text)

    # ============================================================
    # PROCESSAMENTO
    # ============================================================
    def _set_processing(self, active):
        """Altera estado visual de processamento."""
        self.norm_processando = active
        state = "disabled" if active else "normal"
        self.root.after(0, lambda: self.norm_btn_processar.configure(state=state))
        if active:
            self.root.after(0, lambda: self.progress.configure(mode="indeterminate"))
            self.root.after(0, lambda: self.progress.start())
        else:
            self.root.after(0, lambda: self.progress.stop())
            self.root.after(0, lambda: self.progress.configure(mode="determinate"))
            self.root.after(0, lambda: self.progress.set(1.0))

    def _on_normalizar(self):
        """Valida entrada e despacha normalização."""
        if self.norm_processando:
            return
        erro = self._validar_entrada()
        if erro:
            messagebox.showwarning("Atenção", erro)
            return
        regras = self._coletar_regras()
        if regras is None:
            return  # Erro já mostrado
        pn_conversions = self._coletar_conversoes_pn()
        if not regras and not pn_conversions:
            messagebox.showwarning("Atenção",
                                   "Adicione pelo menos uma regra de normalização\n"
                                   "ou uma conversão de partnumber.")
            return
        self._despachar_normalizacao(regras, pn_conversions)

    def _validar_entrada(self) -> str | None:
        """Valida se os campos obrigatórios estão preenchidos. Retorna erro ou None."""
        if not self.norm_input_file or not os.path.exists(self.norm_input_file):
            return "Selecione o arquivo de dados primeiro."
        output_dir = self.norm_output_dir.get().strip() or os.path.dirname(self.norm_input_file)
        if not os.path.isdir(output_dir):
            return f"A pasta de saída não existe:\n{output_dir}"
        return None

    def _coletar_regras(self) -> list | None:
        """Coleta regras do método selecionado. Retorna None em caso de erro."""
        method = self.norm_method.get()
        if method == "excel":
            return self._carregar_regras_excel()
        else:
            return list(self.norm_regras_manual) if self.norm_regras_manual else []

    def _carregar_regras_excel(self) -> list | None:
        """Carrega regras do arquivo Excel. Retorna None em caso de erro."""
        if not self.norm_rules_file or not os.path.exists(self.norm_rules_file):
            messagebox.showwarning("Atenção", "Selecione o arquivo de regras.")
            return None
        try:
            df_regras = ler_arquivo(self.norm_rules_file, dtype=str)
            return self._parsear_regras_do_dataframe(df_regras)
        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao ler arquivo de regras:\n{e}")
            return None

    def _parsear_regras_do_dataframe(self, df_regras) -> list | None:
        """Extrai regras de um DataFrame. Retorna None se coluna MARCA faltar."""
        col_marca, col_pn = self._encontrar_colunas_regras(df_regras)
        if col_marca is None:
            messagebox.showwarning("Erro",
                                   "Coluna MARCA não encontrada no arquivo de regras.")
            return None

        regras = []
        for _, row in df_regras.iterrows():
            marca = str(row.get(col_marca, "")).strip()
            pn = str(row.get(col_pn, "")).strip() if col_pn else ""
            if marca and marca.upper() != "NAN":
                pn_clean = pn.upper() if pn.upper() != "NAN" else ""
                regras.append({"marca": marca.upper(), "partnumber": pn_clean})
        return regras

    def _encontrar_colunas_regras(self, df_regras) -> tuple:
        """Encontra colunas MARCA e PARTNUMBER no DataFrame de regras."""
        col_marca = None
        col_pn = None
        for c in df_regras.columns:
            upper = c.upper().strip()
            if upper == "MARCA":
                col_marca = c
            elif upper in ("PARTNUMBER", "PART NUMBER", "PART_NUMBER", "PN"):
                col_pn = c
        return col_marca, col_pn

    def _coletar_conversoes_pn(self) -> list:
        """Retorna lista de conversões de PN se habilitadas."""
        if not self.norm_pn_conv_enabled.get():
            return []
        if self.norm_method.get() == "app":
            return list(self.norm_pn_conversions) if self.norm_pn_conversions else []
        # Modo Excel: carregar conversões do arquivo
        if not self.norm_pn_conv_file or not os.path.exists(self.norm_pn_conv_file):
            return []
        return self._carregar_conversoes_pn_excel()

    def _carregar_conversoes_pn_excel(self) -> list:
        """Carrega conversões de PN do arquivo Excel."""
        try:
            df_conv = ler_arquivo(self.norm_pn_conv_file, dtype=str)
            col_de, col_para = self._encontrar_colunas_conv(df_conv)
            if not col_de or not col_para:
                messagebox.showwarning("Atenção",
                    "Colunas 'PN ORIGINAL' e 'PN NOVO' não encontradas\n"
                    "no arquivo de conversões de PN.")
                return []
            conversoes = []
            for _, row in df_conv.iterrows():
                pn_de = str(row.get(col_de, "")).strip().upper()
                pn_para = str(row.get(col_para, "")).strip().upper()
                if pn_de and pn_de != "NAN" and pn_para and pn_para != "NAN":
                    conversoes.append({"de": pn_de, "para": pn_para})
            return conversoes
        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao ler arquivo de conversões:\n{e}")
            return []

    def _encontrar_colunas_conv(self, df) -> tuple:
        """Encontra colunas PN ORIGINAL e PN NOVO no DataFrame."""
        col_de = col_para = None
        for c in df.columns:
            upper = c.upper().strip()
            if upper in ("PN ORIGINAL", "PN_ORIGINAL", "PNORIGINAL"):
                col_de = c
            elif upper in ("PN NOVO", "PN_NOVO", "PNNOVO"):
                col_para = c
        return col_de, col_para

    def _despachar_normalizacao(self, regras, pn_conversions):
        """Inicia thread de normalização."""
        self._clear_log()
        output_dir = self.norm_output_dir.get().strip() or os.path.dirname(self.norm_input_file)
        formato = self.norm_formato.get()

        self._set_processing(True)
        self._set_status("Normalizando dados...")

        t = threading.Thread(target=processar_normalizacao, daemon=True,
                             args=(self.norm_input_file, regras, output_dir, formato,
                                   self._log, self._norm_done),
                             kwargs={"pn_conversions": pn_conversions})
        t.start()

    def _norm_done(self, success):
        """Callback de conclusão da normalização."""
        self._set_processing(False)
        if success:
            self._set_status("✅ Normalização concluída!", 1.0)
            self._log("\n✅ NORMALIZAÇÃO CONCLUÍDA COM SUCESSO!")
            self.root.after(0, lambda: messagebox.showinfo(
                "Sucesso", "Dados normalizados e salvos com sucesso!"))
        else:
            self._set_status("❌ Erro na normalização", 0)
