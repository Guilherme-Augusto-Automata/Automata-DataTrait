"""
Tab Consolidação PN — Interface para análise e explosão de partnumbers.
Step 1: Extrai PNs, marcas e quantidades via regex + IA (Claude)
Step 2: Explode linhas multi-PN em linhas individuais
"""

import os
import threading

import customtkinter as ctk
from tkinter import messagebox
from tkinterdnd2 import DND_FILES

from config.colors import COLORS
from domain.consolidacao import processar_analise, processar_explosao
from ui.components import (
    parse_drop_path, validate_file_path, browse_file, browse_directory,
    log_message, clear_log,
)


class TabConsolidacao:
    """Gerencia a aba de Consolidação de PartNumbers."""

    def __init__(self, tab_frame, root, status_callback, progress_bar):
        self.root = root
        self._set_status = status_callback
        self.progress = progress_bar

        # Estado
        self.input_file = None
        self.output_dir = ctk.StringVar(value="")
        self.etapa_var = ctk.StringVar(value="analise")
        self.api_key_var = ctk.StringVar(value="")
        self.processando = False

        # Referências para widgets condicionais
        self._api_key_widgets = []

        self._build(tab_frame)

    # ============================================================
    # BUILD (orquestrador)
    # ============================================================
    def _build(self, tab):
        """Orquestra a construção de todos os painéis."""
        tab.grid_columnconfigure(0, weight=1)
        tab.grid_columnconfigure(1, weight=2)
        tab.grid_rowconfigure(0, weight=1)

        left = ctk.CTkScrollableFrame(tab, fg_color=COLORS["surface"],
                                       corner_radius=12, border_width=1,
                                       border_color=COLORS["border"])
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 8))

        self._build_title(left)
        self._build_separator(left)
        self._build_step_selector(left)
        self._build_separator(left)
        self._build_api_key_section(left)
        self._build_separator(left)
        self._build_file_drop(left)
        self._build_separator(left)
        self._build_output_dir(left)
        self._build_process_button(left)
        self._build_right_panel(tab)

        # Estado inicial
        self._on_step_changed()

    # ============================================================
    # SUB-BUILDERS
    # ============================================================
    def _build_separator(self, parent):
        """Adiciona separador horizontal."""
        ctk.CTkFrame(parent, fg_color=COLORS["border"], height=1).pack(
            fill="x", padx=16, pady=12)

    def _build_title(self, parent):
        """Constrói título e descrição da aba."""
        ctk.CTkLabel(parent, text="🔬  CONSOLIDAÇÃO DE PNs",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(
            anchor="w", padx=16, pady=(16, 4))
        ctk.CTkLabel(parent,
                     text="Extrai partnumbers, marcas e quantidades\n"
                          "via regex + IA, e explode linhas multi-PN.",
                     font=ctk.CTkFont(size=11),
                     text_color=COLORS["text_dim"],
                     justify="left").pack(anchor="w", padx=16, pady=(0, 8))

    def _build_step_selector(self, parent):
        """Constrói seletor de etapa (Análise / Explosão)."""
        ctk.CTkLabel(parent, text="Etapa",
                     font=ctk.CTkFont(size=12, weight="bold"),
                     text_color=COLORS["text"]).pack(
            anchor="w", padx=16, pady=(0, 6))

        self.step_menu = ctk.CTkOptionMenu(
            parent, variable=self.etapa_var,
            values=["analise", "explosao"],
            font=ctk.CTkFont(size=13),
            fg_color=COLORS["card"],
            button_color=COLORS["secondary"],
            button_hover_color=COLORS["primary"],
            dropdown_fg_color=COLORS["surface"],
            dropdown_hover_color=COLORS["card_hover"],
            text_color=COLORS["text"],
            command=lambda _: self._on_step_changed(),
        )
        self.step_menu.pack(fill="x", padx=16, pady=(0, 4))

        self.step_desc_label = ctk.CTkLabel(
            parent, text="",
            font=ctk.CTkFont(size=10),
            text_color=COLORS["text_dim"],
            justify="left", wraplength=260)
        self.step_desc_label.pack(anchor="w", padx=16, pady=(2, 0))

    def _build_api_key_section(self, parent):
        """Constrói campo de chave API (Claude/Anthropic)."""
        self._api_key_label = ctk.CTkLabel(
            parent, text="🔑 Chave API (Claude)",
            font=ctk.CTkFont(size=12, weight="bold"),
            text_color=COLORS["text"])
        self._api_key_label.pack(anchor="w", padx=16, pady=(0, 6))

        self._api_key_entry = ctk.CTkEntry(
            parent, textvariable=self.api_key_var,
            placeholder_text="sk-ant-...",
            show="•",
            fg_color=COLORS["card"], border_color=COLORS["border"],
            text_color=COLORS["text"], font=ctk.CTkFont(size=11))
        self._api_key_entry.pack(fill="x", padx=16, pady=(0, 4))

        self._api_key_hint = ctk.CTkLabel(
            parent,
            text="Necessária para o Fluxo 4 (correção via IA)",
            font=ctk.CTkFont(size=10),
            text_color=COLORS["text_dim"])
        self._api_key_hint.pack(anchor="w", padx=16, pady=(0, 0))

        self._api_key_widgets = [
            self._api_key_label, self._api_key_entry, self._api_key_hint
        ]

    def _build_file_drop(self, parent):
        """Constrói zona de drop para arquivo de entrada."""
        ctk.CTkLabel(parent, text="📁 Arquivo de Entrada",
                     font=ctk.CTkFont(size=12, weight="bold"),
                     text_color=COLORS["text"]).pack(
            anchor="w", padx=16, pady=(0, 6))

        self.drop_frame = ctk.CTkFrame(parent, fg_color=COLORS["card"],
                                        corner_radius=10, height=80,
                                        border_width=2,
                                        border_color=COLORS["border"])
        self.drop_frame.pack(fill="x", padx=16, pady=(0, 4))
        self.drop_frame.pack_propagate(False)

        self.drop_label = ctk.CTkLabel(
            self.drop_frame,
            text="📄 Arraste o Excel aqui\nou clique para buscar",
            font=ctk.CTkFont(size=11),
            text_color=COLORS["text_dim"], justify="center")
        self.drop_label.pack(expand=True)

        # Drag & Drop
        self.drop_frame.drop_target_register(DND_FILES)
        self.drop_frame.dnd_bind("<<Drop>>", self._on_drop)
        self.drop_label.drop_target_register(DND_FILES)
        self.drop_label.dnd_bind("<<Drop>>", self._on_drop)

        # Click to browse
        self.drop_frame.bind("<Button-1>", self._on_browse_click)
        self.drop_label.bind("<Button-1>", self._on_browse_click)

        self.file_label = ctk.CTkLabel(
            parent, text="", font=ctk.CTkFont(size=11),
            text_color=COLORS["success"], wraplength=250)
        self.file_label.pack(anchor="w", padx=16, pady=(2, 0))

    def _build_output_dir(self, parent):
        """Constrói seletor de pasta de saída."""
        ctk.CTkLabel(parent, text="📂 Pasta de Saída",
                     font=ctk.CTkFont(size=12, weight="bold"),
                     text_color=COLORS["text"]).pack(
            anchor="w", padx=16, pady=(0, 6))

        row = ctk.CTkFrame(parent, fg_color="transparent")
        row.pack(fill="x", padx=16, pady=(0, 4))

        ctk.CTkEntry(
            row, textvariable=self.output_dir,
            placeholder_text="Mesma pasta do arquivo de entrada",
            fg_color=COLORS["card"], border_color=COLORS["border"],
            text_color=COLORS["text"], font=ctk.CTkFont(size=11),
        ).pack(side="left", fill="x", expand=True, padx=(0, 4))

        ctk.CTkButton(
            row, text="📂", width=36, height=28,
            font=ctk.CTkFont(size=14),
            fg_color=COLORS["card"], hover_color=COLORS["card_hover"],
            border_width=1, border_color=COLORS["border"],
            corner_radius=6, command=self._on_browse_dir
        ).pack(side="right")

    def _build_process_button(self, parent):
        """Constrói botão de processamento."""
        self.btn_processar = ctk.CTkButton(
            parent, text="▶  PROCESSAR",
            font=ctk.CTkFont(size=14, weight="bold"),
            fg_color=COLORS["secondary"],
            hover_color=COLORS["primary"],
            corner_radius=10, height=42,
            command=self._on_processar)
        self.btn_processar.pack(fill="x", padx=16, pady=(16, 16))

    def _build_right_panel(self, tab):
        """Constrói painel lateral direito com log."""
        right = ctk.CTkFrame(tab, fg_color=COLORS["surface"],
                              corner_radius=12, border_width=1,
                              border_color=COLORS["border"])
        right.grid(row=0, column=1, sticky="nsew")

        ctk.CTkLabel(right, text="📋 Log de Processamento",
                     font=ctk.CTkFont(size=13, weight="bold"),
                     text_color=COLORS["info"]).pack(
            anchor="w", padx=16, pady=(16, 8))

        self.log_text = ctk.CTkTextbox(
            right, fg_color=COLORS["card"],
            text_color=COLORS["text"],
            font=ctk.CTkFont(family="Consolas", size=12),
            corner_radius=8, border_width=1,
            border_color=COLORS["border"],
            state="disabled", wrap="word")
        self.log_text.pack(fill="both", expand=True, padx=12, pady=(0, 12))

    # ============================================================
    # EVENTOS
    # ============================================================
    def _on_step_changed(self):
        """Atualiza UI quando a etapa muda."""
        etapa = self.etapa_var.get()
        if etapa == "analise":
            self.step_desc_label.configure(
                text="Extrai PNs, marcas e quantidades via\n"
                     "regex + IA (Claude). Pipeline completo.")
            self._show_api_key(True)
            self.btn_processar.configure(text="▶  INICIAR ANÁLISE")
        else:
            self.step_desc_label.configure(
                text="Explode linhas com múltiplos PNs em\n"
                     "linhas individuais (1 PN por linha).")
            self._show_api_key(False)
            self.btn_processar.configure(text="▶  INICIAR EXPLOSÃO")

    def _show_api_key(self, show: bool):
        """Mostra ou oculta os widgets de API key."""
        for widget in self._api_key_widgets:
            if show:
                widget.pack(anchor="w", padx=16, pady=(0, 4))
            else:
                widget.pack_forget()

    def _on_drop(self, event):
        """Processa arquivo arrastado para a drop zone."""
        path = parse_drop_path(event)
        self._set_file(path)

    def _on_browse_click(self, event=None):
        """Abre diálogo para selecionar arquivo."""
        path = browse_file("Selecionar Excel para Consolidação")
        if path:
            self._set_file(path)

    def _set_file(self, path: str):
        """Define o arquivo selecionado."""
        if not path:
            return
        ext = path.lower()
        if not (ext.endswith(".xlsx") or ext.endswith(".xls")):
            messagebox.showwarning("Formato inválido",
                                   "Selecione um arquivo Excel (.xlsx / .xls)")
            return
        self.input_file = path
        nome = os.path.basename(path)
        self.file_label.configure(text=f"✓ {nome}")
        self.drop_label.configure(text=f"📄 {nome}")

    def _on_browse_dir(self):
        """Abre diálogo para selecionar pasta de saída."""
        path = browse_directory("Selecionar pasta de saída")
        if path:
            self.output_dir.set(path)

    # ============================================================
    # PROCESSAMENTO
    # ============================================================
    def _on_processar(self):
        """Valida entrada e despacha processamento."""
        erro = self._validar_entrada()
        if erro:
            messagebox.showwarning("Dados incompletos", erro)
            return
        self._despachar_processamento()

    def _validar_entrada(self) -> str | None:
        """Valida campos obrigatórios. Retorna mensagem de erro ou None."""
        if not self.input_file:
            return "Selecione o arquivo Excel de entrada."
        if not os.path.exists(self.input_file):
            return f"Arquivo não encontrado:\n{self.input_file}"
        if self.etapa_var.get() == "analise":
            if not self.api_key_var.get().strip():
                return "Informe a chave API do Claude (Anthropic)."
        return None

    def _despachar_processamento(self):
        """Inicia processamento em thread separada."""
        self.processando = True
        self.btn_processar.configure(state="disabled", text="⏳ Processando...")
        self._set_status("Processando consolidação...", 0.0)
        clear_log(self.log_text)

        etapa = self.etapa_var.get()
        if etapa == "analise":
            thread = threading.Thread(target=self._executar_analise, daemon=True)
        else:
            thread = threading.Thread(target=self._executar_explosao, daemon=True)
        thread.start()

    def _get_output_dir(self) -> str:
        """Retorna pasta de saída (padrão: mesma do input)."""
        out = self.output_dir.get().strip()
        if out:
            return out
        return os.path.dirname(self.input_file)

    def _log(self, msg: str):
        """Log thread-safe para o painel."""
        log_message(self.log_text, msg, self.root)

    def _finalizar(self, sucesso: bool):
        """Restaura estado da UI após processamento."""
        def _update():
            self.processando = False
            etapa = self.etapa_var.get()
            texto = "▶  INICIAR ANÁLISE" if etapa == "analise" else "▶  INICIAR EXPLOSÃO"
            self.btn_processar.configure(state="normal", text=texto)
            if sucesso:
                self._set_status("Concluído com sucesso", 1.0)
            else:
                self._set_status("Erro durante processamento", 0.0)
        self.root.after(0, _update)

    # ============================================================
    # EXECUÇÃO — ANÁLISE (Step 1)
    # ============================================================
    def _executar_analise(self):
        """Executa Step 1 em thread separada."""
        try:
            processar_analise(
                path_in=self.input_file,
                output_dir=self._get_output_dir(),
                api_key=self.api_key_var.get().strip(),
                log_callback=self._log,
            )
            self._finalizar(True)
        except Exception as e:
            self._log(f"\n❌ Erro: {e}")
            self._finalizar(False)

    # ============================================================
    # EXECUÇÃO — EXPLOSÃO (Step 2)
    # ============================================================
    def _executar_explosao(self):
        """Executa Step 2 em thread separada."""
        try:
            processar_explosao(
                path_in=self.input_file,
                output_dir=self._get_output_dir(),
                log_callback=self._log,
            )
            self._finalizar(True)
        except Exception as e:
            self._log(f"\n❌ Erro: {e}")
            self._finalizar(False)
