"""
App principal — Shell da aplicação com TkinterDnD + CustomTkinter.
Cada método _build_* constrói uma única seção da interface.
"""

import os
import sys
import base64
import io

import customtkinter as ctk
from tkinterdnd2 import TkinterDnD
from PIL import Image

from config.colors import COLORS
from ui.tab_tratamento import TabTratamento
from ui.tab_normalizacao import TabNormalizacao
from ui.tab_banco_dados import TabBancoDados


class TkinterDnDCustom(TkinterDnD.DnDWrapper, ctk.CTk):
    """Combina CustomTkinter com drag-and-drop."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.TkdndVersion = TkinterDnD._require(self)


class App:
    def __init__(self):
        self.root = TkinterDnDCustom()
        self.root.title("Tratamento de Dados — Automata Custom")
        self.root.geometry("960x720")
        self.root.minsize(800, 600)
        self.root.configure(fg_color=COLORS["background"])

        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

        self._build_ui()

    # --------------------------------------------------------
    # CONSTRUÇÃO DA INTERFACE (orquestrador)
    # --------------------------------------------------------
    def _build_ui(self):
        """Orquestra a montagem de todas as seções da UI."""
        self._main = ctk.CTkFrame(self.root, fg_color=COLORS["background"])
        self._main.pack(fill="both", expand=True, padx=20, pady=15)

        self._build_header(self._main)
        self._build_tabview(self._main)
        self._build_status_bar(self._main)
        self._build_tabs()

    # --------------------------------------------------------
    # HEADER
    # --------------------------------------------------------
    def _build_header(self, parent):
        """Constrói o cabeçalho com logo e título."""
        header = ctk.CTkFrame(parent, fg_color=COLORS["surface"], corner_radius=12,
                              border_width=1, border_color=COLORS["border"])
        header.pack(fill="x", pady=(0, 12))

        header_inner = ctk.CTkFrame(header, fg_color="transparent")
        header_inner.pack(fill="x", padx=20, pady=15)

        self._logo_image = None
        self._build_logo(header_inner)

        title_frame = ctk.CTkFrame(header_inner, fg_color="transparent")
        title_frame.pack(side="left", fill="x", expand=True)

        ctk.CTkLabel(title_frame, text="Tratamento de Dados",
                     font=ctk.CTkFont(size=22, weight="bold"),
                     text_color=COLORS["text"]).pack(anchor="w")
        ctk.CTkLabel(title_frame, text="Automata Custom",
                     font=ctk.CTkFont(size=13),
                     text_color=COLORS["text_dim"]).pack(anchor="w")

    # --------------------------------------------------------
    # TABVIEW
    # --------------------------------------------------------
    def _build_tabview(self, parent):
        """Constrói o widget de abas."""
        self.tabview = ctk.CTkTabview(
            parent, fg_color=COLORS["background"],
            segmented_button_fg_color=COLORS["surface"],
            segmented_button_selected_color=COLORS["secondary"],
            segmented_button_unselected_color=COLORS["card"],
            segmented_button_selected_hover_color=COLORS["primary"],
            segmented_button_unselected_hover_color=COLORS["card_hover"],
            text_color=COLORS["text"],
            corner_radius=12,
        )
        self.tabview.pack(fill="both", expand=True)
        self.tabview.add("Tratamento")
        self.tabview.add("Normalização")
        self.tabview.add("Banco de Dados")

    # --------------------------------------------------------
    # STATUS BAR
    # --------------------------------------------------------
    def _build_status_bar(self, parent):
        """Constrói a barra de status inferior."""
        status_bar = ctk.CTkFrame(parent, fg_color=COLORS["surface"], corner_radius=8,
                                   height=36, border_width=1, border_color=COLORS["border"])
        status_bar.pack(fill="x", pady=(12, 0))
        status_bar.pack_propagate(False)

        self.status_label = ctk.CTkLabel(status_bar, text="Pronto",
                                          font=ctk.CTkFont(size=12),
                                          text_color=COLORS["text_dim"])
        self.status_label.pack(side="left", padx=12)

        self.progress = ctk.CTkProgressBar(status_bar, fg_color=COLORS["card"],
                                            progress_color=COLORS["secondary"],
                                            height=6, corner_radius=3)
        self.progress.pack(side="right", padx=12, pady=10, fill="x", expand=True)
        self.progress.set(0)

    # --------------------------------------------------------
    # TABS (instancia as abas)
    # --------------------------------------------------------
    def _build_tabs(self):
        """Instancia cada aba passando dependências."""
        self.tab_tratamento = TabTratamento(
            self.tabview.tab("Tratamento"), self.root,
            self._set_status, self.progress
        )
        self.tab_normalizacao = TabNormalizacao(
            self.tabview.tab("Normalização"), self.root,
            self._set_status, self.progress
        )
        self.tab_banco_dados = TabBancoDados(
            self.tabview.tab("Banco de Dados"), self.root,
            self._set_status, self.progress
        )

    # --------------------------------------------------------
    # LOGO
    # --------------------------------------------------------
    def _build_logo(self, parent):
        """Tenta carregar a logo por 3 estratégias em sequência."""
        pil_img = self._load_logo_embedded()
        if pil_img is None:
            pil_img = self._load_logo_from_file()

        if pil_img is not None:
            self._render_logo_image(parent, pil_img)
        else:
            self._render_logo_fallback(parent)

    def _load_logo_embedded(self) -> Image.Image | None:
        """Carrega logo embutida em base64 (funciona no .exe)."""
        try:
            from _logo_data import LOGO_BASE64
            raw = base64.b64decode(LOGO_BASE64)
            return Image.open(io.BytesIO(raw)).convert("RGBA")
        except Exception:
            return None

    def _load_logo_from_file(self) -> Image.Image | None:
        """Carrega logo de arquivo no disco (fallback)."""
        logo_candidates = [
            "LogoOficial_Branco.png",
            "logo.png", "logo.jpg", "logo.ico",
        ]
        base_dir = self._get_base_dir()
        for name in logo_candidates:
            path = os.path.join(base_dir, name)
            if os.path.exists(path):
                try:
                    return Image.open(path).convert("RGBA")
                except Exception:
                    pass
        return None

    def _render_logo_image(self, parent, pil_img: Image.Image):
        """Renderiza imagem PIL como logo no header."""
        max_h = 48
        ratio = max_h / pil_img.height
        new_w = int(pil_img.width * ratio)

        self._logo_image = ctk.CTkImage(
            light_image=pil_img, dark_image=pil_img,
            size=(new_w, max_h)
        )
        ctk.CTkLabel(
            parent, image=self._logo_image, text=""
        ).pack(side="left", padx=(0, 15))

    def _render_logo_fallback(self, parent):
        """Renderiza ícone com letra como fallback de logo."""
        fallback = ctk.CTkFrame(parent, fg_color=COLORS["primary"],
                                corner_radius=8, width=50, height=50)
        fallback.pack(side="left", padx=(0, 15))
        fallback.pack_propagate(False)
        ctk.CTkLabel(fallback, text="A",
                     font=ctk.CTkFont(size=24, weight="bold"),
                     text_color="white").pack(expand=True)

    @staticmethod
    def _get_base_dir():
        """Retorna diretório base da aplicação."""
        if getattr(sys, 'frozen', False):
            return os.path.dirname(sys.executable)
        return os.path.dirname(os.path.abspath(__file__))

    # --------------------------------------------------------
    # STATUS
    # --------------------------------------------------------
    def _set_status(self, text, progresso=None):
        """Atualiza texto e progresso na barra de status."""
        def _update():
            self.status_label.configure(text=text)
            if progresso is not None:
                self.progress.set(progresso)
        self.root.after(0, _update)

    # --------------------------------------------------------
    # RUN
    # --------------------------------------------------------
    def run(self):
        """Inicia o loop principal."""
        self.root.mainloop()
