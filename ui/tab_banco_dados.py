"""
Tab Banco de Dados — Interface para preparação de dados para banco.
Cada método faz uma única coisa.
"""

import os
import threading

import customtkinter as ctk
from tkinter import messagebox
from tkinterdnd2 import DND_FILES

from config.colors import COLORS
from domain.banco_dados import processar_banco_dados
from ui.components import (
    parse_drop_path, validate_file_path, browse_file, browse_directory,
    log_message, clear_log,
)


class TabBancoDados:
    """Gerencia a aba de Banco de Dados."""

    def __init__(self, tab_frame, root, status_callback, progress_bar):
        self.root = root
        self._set_status = status_callback
        self.progress = progress_bar

        # Estado
        self.db_input_file = None
        self.db_formato = ctk.StringVar(value="ambos")
        self.db_output_dir = ctk.StringVar(value="")
        self.db_linhas_var = ctk.StringVar(value="50000")
        self.db_processando = False

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

        self._build_title(left)
        self._build_separator(left)
        self._build_file_drop(left)
        self._build_separator(left)
        self._build_lines_config(left)
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

    def _build_title(self, parent):
        """Constrói título e descrição da aba."""
        ctk.CTkLabel(parent, text="🗄️  PREPARAR PARA BANCO DE DADOS",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", padx=16, pady=(16, 4))
        ctk.CTkLabel(parent,
                     text="Mapeia as colunas do arquivo tratado\npara o formato padrão do banco de dados.",
                     font=ctk.CTkFont(size=11), text_color=COLORS["text_dim"],
                     justify="left").pack(anchor="w", padx=16, pady=(0, 8))

    def _build_file_drop(self, parent):
        """Constrói zona de drop para arquivo de entrada."""
        ctk.CTkLabel(parent, text="📁  ARQUIVO DE ENTRADA",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", padx=16, pady=(0, 4))
        ctk.CTkLabel(parent, text="Excel ou CSV (sep. |) já tratado (Equador ou Argentina)",
                     font=ctk.CTkFont(size=11), text_color=COLORS["text_dim"]
                     ).pack(anchor="w", padx=16, pady=(0, 8))

        self.db_data_drop = ctk.CTkFrame(parent, fg_color=COLORS["card"],
                                          corner_radius=10, height=80,
                                          border_width=2, border_color=COLORS["border"])
        self.db_data_drop.pack(fill="x", padx=16, pady=(0, 4))
        self.db_data_drop.pack_propagate(False)

        self.db_data_label = ctk.CTkLabel(
            self.db_data_drop,
            text="Arraste o arquivo .xlsx / .csv aqui\nou clique para selecionar",
            font=ctk.CTkFont(size=11), text_color=COLORS["text_dim"], justify="center"
        )
        self.db_data_label.pack(expand=True)

        self._register_drop_handlers()

        self.db_data_file_label = ctk.CTkLabel(parent, text="", font=ctk.CTkFont(size=11),
                                                text_color=COLORS["success"], wraplength=250)
        self.db_data_file_label.pack(anchor="w", padx=16)

    def _register_drop_handlers(self):
        """Registra handlers de drag-and-drop e click."""
        self.db_data_drop.drop_target_register(DND_FILES)
        self.db_data_drop.dnd_bind("<<Drop>>", self._on_db_data_drop)
        self.db_data_label.drop_target_register(DND_FILES)
        self.db_data_label.dnd_bind("<<Drop>>", self._on_db_data_drop)
        self.db_data_drop.bind("<Button-1>", self._on_db_data_browse)
        self.db_data_label.bind("<Button-1>", self._on_db_data_browse)

    def _build_lines_config(self, parent):
        """Constrói seção de configuração de linhas por arquivo."""
        ctk.CTkLabel(parent, text="✂️  LINHAS POR ARQUIVO",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", padx=16, pady=(0, 4))
        ctk.CTkLabel(parent, text="Divide o resultado em partes (0 = sem divisão)",
                     font=ctk.CTkFont(size=11), text_color=COLORS["text_dim"]
                     ).pack(anchor="w", padx=16, pady=(0, 8))

        self._build_lines_entry(parent)
        self._build_lines_shortcuts(parent)

    def _build_lines_entry(self, parent):
        """Constrói campo de entrada de linhas."""
        linhas_row = ctk.CTkFrame(parent, fg_color="transparent")
        linhas_row.pack(fill="x", padx=16, pady=(0, 4))

        self.db_linhas_entry = ctk.CTkEntry(
            linhas_row, textvariable=self.db_linhas_var,
            fg_color=COLORS["card"], border_color=COLORS["border"],
            text_color=COLORS["text"], font=ctk.CTkFont(size=12), width=120,
        )
        self.db_linhas_entry.pack(side="left", padx=(0, 8))

        ctk.CTkLabel(linhas_row, text="linhas",
                     font=ctk.CTkFont(size=12), text_color=COLORS["text_dim"]
                     ).pack(side="left")

    def _build_lines_shortcuts(self, parent):
        """Constrói botões de atalho de linhas."""
        atalhos_row = ctk.CTkFrame(parent, fg_color="transparent")
        atalhos_row.pack(fill="x", padx=16, pady=(4, 4))

        for valor, label in [("10000", "10K"), ("50000", "50K"),
                             ("100000", "100K"), ("0", "Tudo")]:
            ctk.CTkButton(
                atalhos_row, text=label, width=55, height=26,
                font=ctk.CTkFont(size=11),
                fg_color=COLORS["card"], hover_color=COLORS["card_hover"],
                border_width=1, border_color=COLORS["border"],
                corner_radius=6,
                command=lambda v=valor: self.db_linhas_var.set(v)
            ).pack(side="left", padx=2)

    def _build_format_selector(self, parent):
        """Constrói seção de seleção de formato de saída."""
        ctk.CTkLabel(parent, text="📄  FORMATO DE SAÍDA",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", padx=16, pady=(0, 8))

        for val, label in [("xlsx", "XLSX (Excel)"), ("csv", "CSV (separado por |)"),
                           ("ambos", "Ambos")]:
            ctk.CTkRadioButton(
                parent, text=label, variable=self.db_formato,
                value=val, font=ctk.CTkFont(size=13),
                fg_color=COLORS["secondary"], hover_color=COLORS["primary"],
                border_color=COLORS["border"], text_color=COLORS["text"]
            ).pack(anchor="w", pady=3, padx=24)

    def _build_output_dir(self, parent):
        """Constrói seção de pasta de saída."""
        ctk.CTkLabel(parent, text="💾  PASTA DE SAÍDA",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", padx=16, pady=(0, 6))

        db_out_row = ctk.CTkFrame(parent, fg_color="transparent")
        db_out_row.pack(fill="x", padx=16, pady=(0, 4))

        self.db_out_entry = ctk.CTkEntry(
            db_out_row, textvariable=self.db_output_dir,
            placeholder_text="Mesma pasta do arquivo de entrada",
            fg_color=COLORS["card"], border_color=COLORS["border"],
            text_color=COLORS["text"], font=ctk.CTkFont(size=11),
        )
        self.db_out_entry.pack(side="left", fill="x", expand=True, padx=(0, 4))

        ctk.CTkButton(
            db_out_row, text="📂", width=36, height=28,
            font=ctk.CTkFont(size=14),
            fg_color=COLORS["card"], hover_color=COLORS["card_hover"],
            border_width=1, border_color=COLORS["border"],
            corner_radius=6, command=self._on_db_browse_output
        ).pack(side="right")

    def _build_process_button(self, parent):
        """Constrói botão de processar."""
        self.db_btn_processar = ctk.CTkButton(
            parent, text="▶  PREPARAR BANCO", font=ctk.CTkFont(size=14, weight="bold"),
            fg_color=COLORS["secondary"], hover_color=COLORS["primary"],
            corner_radius=8, height=44, command=self._on_preparar_banco
        )
        self.db_btn_processar.pack(fill="x", padx=16, pady=(12, 16))

    def _build_right_panel(self, tab):
        """Constrói painel direito com log e info de mapeamento."""
        right = ctk.CTkFrame(tab, fg_color=COLORS["surface"], corner_radius=12,
                              border_width=1, border_color=COLORS["border"])
        right.grid(row=0, column=1, sticky="nsew", padx=(8, 0))

        right_inner = ctk.CTkFrame(right, fg_color="transparent")
        right_inner.pack(fill="both", expand=True, padx=16, pady=16)

        self._build_log_section(right_inner)
        self._build_mapping_info(right_inner)

    def _build_log_section(self, parent):
        """Constrói seção de log."""
        ctk.CTkLabel(parent, text="📋  LOG — BANCO DE DADOS",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=COLORS["info"]).pack(anchor="w", pady=(0, 8))

        self.db_log_text = ctk.CTkTextbox(parent, fg_color=COLORS["card"],
                                           text_color=COLORS["text"],
                                           font=ctk.CTkFont(family="Consolas", size=12),
                                           corner_radius=8, border_width=1,
                                           border_color=COLORS["border"],
                                           state="disabled", wrap="word")
        self.db_log_text.pack(fill="both", expand=True)

    def _build_mapping_info(self, parent):
        """Constrói painel informativo de mapeamento de colunas."""
        info_frame = ctk.CTkFrame(parent, fg_color=COLORS["card"], corner_radius=8,
                                   border_width=1, border_color=COLORS["border"])
        info_frame.pack(fill="x", pady=(8, 0))

        ctk.CTkLabel(info_frame, text="📌  Mapeamento de Colunas",
                     font=ctk.CTkFont(size=12, weight="bold"),
                     text_color=COLORS["warning"]).pack(anchor="w", padx=12, pady=(8, 4))

        mapeamento_texto = (
            "identificador → NUMERO_DE_FORMULARIO\n"
            "importador → RAZON_SOCIAL_IMPORTADOR + IMPORTADORES\n"
            "País de origem → CODIGO_LUGAR_INGRESO_MERCA\n"
            "NANDINA / NCM-SIM → SUBPARTIDA_ARANCELARIA\n"
            "CANTIDAD → CANTIDAD_DCMS + CANTIDAD\n"
            "USD FOB / FOB DOLAR → VALOR_FOB_USD + VALOR_FOB_USD_2\n"
            "Descrição Comercial → DESCRIPCION_MERCANCIA\n"
            "Data → FECHA_LEVANTE  |  Partnumber → PARTNUMBERS\n"
            "MARCA → MARCA"
        )
        ctk.CTkLabel(info_frame, text=mapeamento_texto,
                     font=ctk.CTkFont(family="Consolas", size=10),
                     text_color=COLORS["text_dim"], justify="left"
                     ).pack(anchor="w", padx=12, pady=(0, 8))

    # ============================================================
    # EVENTOS
    # ============================================================
    def _on_db_data_drop(self, event):
        """Trata drop de arquivo de dados."""
        path = parse_drop_path(event)
        self._set_db_data_file(path)

    def _on_db_data_browse(self, _event=None):
        """Abre diálogo para selecionar arquivo de dados."""
        path = browse_file()
        if path:
            self._set_db_data_file(path)

    def _set_db_data_file(self, path):
        """Valida e define o arquivo de dados."""
        if not validate_file_path(path):
            messagebox.showwarning("Arquivo inválido",
                                   "Selecione um arquivo .xlsx, .xls ou .csv")
            return
        self.db_input_file = path
        nome = os.path.basename(path)
        self.db_data_file_label.configure(text=f"✓ {nome}")
        self.db_data_label.configure(text=f"📄 {nome}")
        self.db_data_drop.configure(border_color=COLORS["success"])
        self._log(f"📄 Arquivo selecionado: {nome}")

    def _on_db_browse_output(self):
        """Abre diálogo para selecionar pasta de saída."""
        path = browse_directory()
        if path:
            self.db_output_dir.set(path)
            self._log(f"💾 Pasta de saída: {path}")

    # ============================================================
    # LOG
    # ============================================================
    def _log(self, msg):
        log_message(self.db_log_text, msg, self.root)

    def _clear_log(self):
        clear_log(self.db_log_text)

    # ============================================================
    # PROCESSAMENTO
    # ============================================================
    def _set_processing(self, active):
        """Altera estado visual de processamento."""
        self.db_processando = active
        state = "disabled" if active else "normal"
        self.root.after(0, lambda: self.db_btn_processar.configure(state=state))
        if active:
            self.root.after(0, lambda: self.progress.configure(mode="indeterminate"))
            self.root.after(0, lambda: self.progress.start())
        else:
            self.root.after(0, lambda: self.progress.stop())
            self.root.after(0, lambda: self.progress.configure(mode="determinate"))
            self.root.after(0, lambda: self.progress.set(1.0))

    def _on_preparar_banco(self):
        """Valida entrada e despacha processamento."""
        if self.db_processando:
            return
        erro = self._validar_entrada()
        if erro:
            messagebox.showwarning("Atenção", erro)
            return
        linhas = self._validar_linhas()
        if linhas is None:
            return
        self._despachar_processamento(linhas)

    def _validar_entrada(self) -> str | None:
        """Valida se arquivo e pasta estão corretos. Retorna erro ou None."""
        if not self.db_input_file or not os.path.exists(self.db_input_file):
            return "Selecione o arquivo de dados primeiro."
        output_dir = self.db_output_dir.get().strip() or os.path.dirname(self.db_input_file)
        if not os.path.isdir(output_dir):
            return f"A pasta de saída não existe:\n{output_dir}"
        return None

    def _validar_linhas(self) -> int | None:
        """Valida e retorna o número de linhas por arquivo. None em caso de erro."""
        try:
            linhas = int(self.db_linhas_var.get().strip())
            if linhas < 0:
                raise ValueError
            return linhas
        except ValueError:
            messagebox.showwarning("Valor inválido",
                                   "Informe um número inteiro positivo para linhas por arquivo.\n"
                                   "Use 0 para não dividir.")
            return None

    def _despachar_processamento(self, linhas: int):
        """Inicia thread de processamento do banco."""
        self._clear_log()
        output_dir = self.db_output_dir.get().strip() or os.path.dirname(self.db_input_file)
        formato = self.db_formato.get()

        self._set_processing(True)
        self._set_status("Preparando para banco de dados...")

        t = threading.Thread(target=processar_banco_dados, daemon=True,
                             args=(self.db_input_file, output_dir, formato, linhas,
                                   self._log, self._db_done))
        t.start()

    def _db_done(self, success):
        """Callback de conclusão do processamento."""
        self._set_processing(False)
        if success:
            self._set_status("✅ Banco de dados preparado!", 1.0)
            self._log("\n✅ PREPARAÇÃO CONCLUÍDA COM SUCESSO!")
            self.root.after(0, lambda: messagebox.showinfo(
                "Sucesso", "Arquivo preparado para o banco de dados com sucesso!"))
        else:
            self._set_status("❌ Erro na preparação", 0)
