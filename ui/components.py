"""
Componentes reutilizáveis da interface.
Drop zones, log panels, e utilitários de UI.
"""

import os
import sys
import base64
import io

import customtkinter as ctk
from tkinter import filedialog, messagebox
from tkinterdnd2 import DND_FILES
from PIL import Image

from config.colors import COLORS


def create_drop_zone(parent, label_text: str, on_drop, on_browse,
                     height: int = 80) -> tuple:
    """Cria uma zona de drop com label.
    Retorna (frame, label_widget).
    """
    frame = ctk.CTkFrame(parent, fg_color=COLORS["card"],
                         corner_radius=10, height=height,
                         border_width=2, border_color=COLORS["border"])
    frame.pack_propagate(False)

    label = ctk.CTkLabel(
        frame, text=label_text,
        font=ctk.CTkFont(size=11 if height <= 80 else 12),
        text_color=COLORS["text_dim"], justify="center"
    )
    label.pack(expand=True)

    # Drag & Drop
    frame.drop_target_register(DND_FILES)
    frame.dnd_bind("<<Drop>>", on_drop)
    label.drop_target_register(DND_FILES)
    label.dnd_bind("<<Drop>>", on_drop)

    # Click to browse
    frame.bind("<Button-1>", on_browse)
    label.bind("<Button-1>", on_browse)

    return frame, label


def create_file_label(parent, wraplength: int = 250) -> ctk.CTkLabel:
    """Cria label para exibir o arquivo selecionado."""
    return ctk.CTkLabel(parent, text="", font=ctk.CTkFont(size=11),
                        text_color=COLORS["success"], wraplength=wraplength)


def create_section_label(parent, text: str, padx: int = 16,
                         pady: tuple = (0, 8)) -> ctk.CTkLabel:
    """Cria label de título de seção."""
    lbl = ctk.CTkLabel(parent, text=text,
                       font=ctk.CTkFont(size=14, weight="bold"),
                       text_color=COLORS["info"])
    lbl.pack(anchor="w", padx=padx, pady=pady)
    return lbl


def create_separator(parent, padx: int = 16, pady: int = 12) -> ctk.CTkFrame:
    """Cria linha separadora horizontal."""
    sep = ctk.CTkFrame(parent, fg_color=COLORS["border"], height=1)
    sep.pack(fill="x", padx=padx, pady=pady)
    return sep


def create_log_panel(parent) -> ctk.CTkTextbox:
    """Cria painel de log (textbox readonly)."""
    return ctk.CTkTextbox(parent, fg_color=COLORS["card"],
                          text_color=COLORS["text"],
                          font=ctk.CTkFont(family="Consolas", size=12),
                          corner_radius=8, border_width=1,
                          border_color=COLORS["border"],
                          state="disabled", wrap="word")


def create_output_dir_row(parent, var: ctk.StringVar,
                          browse_command) -> ctk.CTkEntry:
    """Cria linha com entry de pasta de saída + botão browse."""
    row = ctk.CTkFrame(parent, fg_color="transparent")
    row.pack(fill="x", padx=16, pady=(0, 4))

    entry = ctk.CTkEntry(
        row, textvariable=var,
        placeholder_text="Mesma pasta do arquivo de entrada",
        fg_color=COLORS["card"], border_color=COLORS["border"],
        text_color=COLORS["text"], font=ctk.CTkFont(size=11),
    )
    entry.pack(side="left", fill="x", expand=True, padx=(0, 4))

    ctk.CTkButton(
        row, text="📂", width=36, height=28,
        font=ctk.CTkFont(size=14),
        fg_color=COLORS["card"], hover_color=COLORS["card_hover"],
        border_width=1, border_color=COLORS["border"],
        corner_radius=6, command=browse_command
    ).pack(side="right")

    return entry


def create_format_selector(parent, var: ctk.StringVar) -> None:
    """Cria radio buttons de formato de saída (xlsx/csv/ambos)."""
    for val, label in [("xlsx", "XLSX (Excel)"), ("csv", "CSV (separado por |)"),
                       ("ambos", "Ambos")]:
        ctk.CTkRadioButton(
            parent, text=label, variable=var,
            value=val, font=ctk.CTkFont(size=13),
            fg_color=COLORS["secondary"], hover_color=COLORS["primary"],
            border_color=COLORS["border"], text_color=COLORS["text"]
        ).pack(anchor="w", pady=3, padx=24)


def parse_drop_path(event) -> str:
    """Extrai o caminho do arquivo de um evento de drag-and-drop."""
    path = event.data.strip()
    if path.startswith("{"):
        path = path[1:]
    if path.endswith("}"):
        path = path[:-1]
    return path.strip('"').strip("'")


def validate_file_path(path: str) -> bool:
    """Verifica se o arquivo tem extensão válida."""
    return path.lower().endswith((".xlsx", ".xls", ".csv"))


def browse_file(title: str = "Selecionar arquivo Excel ou CSV") -> str | None:
    """Abre diálogo de seleção de arquivo."""
    path = filedialog.askopenfilename(
        title=title,
        filetypes=[("Excel/CSV", "*.xlsx *.xls *.csv"),
                   ("Excel", "*.xlsx *.xls"),
                   ("CSV", "*.csv"),
                   ("Todos", "*.*")]
    )
    return path if path else None


def browse_directory(title: str = "Selecionar pasta de saída") -> str | None:
    """Abre diálogo de seleção de diretório."""
    path = filedialog.askdirectory(title=title)
    return path if path else None


def log_message(textbox: ctk.CTkTextbox, msg: str, root) -> None:
    """Escreve mensagem no log de forma thread-safe."""
    def _update():
        textbox.configure(state="normal")
        textbox.insert("end", msg + "\n")
        textbox.see("end")
        textbox.configure(state="disabled")
    root.after(0, _update)


def clear_log(textbox: ctk.CTkTextbox) -> None:
    """Limpa o conteúdo do textbox de log."""
    textbox.configure(state="normal")
    textbox.delete("1.0", "end")
    textbox.configure(state="disabled")


def build_logo(parent) -> tuple:
    """Carrega a logo da empresa no header. Retorna (logo_label, ctk_image)."""
    pil_img = None

    # 1) Tentar logo embutida (funciona dentro do .exe)
    try:
        from _logo_data import LOGO_BASE64
        raw = base64.b64decode(LOGO_BASE64)
        pil_img = Image.open(io.BytesIO(raw)).convert("RGBA")
    except Exception:
        pass

    # 2) Fallback: arquivo no disco
    if pil_img is None:
        logo_candidates = [
            "LogoOficial_Branco.png",
            "logo.png", "logo.jpg", "logo.ico",
        ]
        base_dir = _get_base_dir()
        for name in logo_candidates:
            path = os.path.join(base_dir, name)
            if os.path.exists(path):
                try:
                    pil_img = Image.open(path).convert("RGBA")
                    break
                except Exception:
                    pass

    if pil_img is not None:
        max_h = 48
        ratio = max_h / pil_img.height
        new_w = int(pil_img.width * ratio)
        ctk_image = ctk.CTkImage(
            light_image=pil_img, dark_image=pil_img,
            size=(new_w, max_h)
        )
        label = ctk.CTkLabel(parent, image=ctk_image, text="")
        label.pack(side="left", padx=(0, 15))
        return label, ctk_image

    # Fallback: ícone com letra
    fallback = ctk.CTkFrame(parent, fg_color=COLORS["primary"],
                            corner_radius=8, width=50, height=50)
    fallback.pack(side="left", padx=(0, 15))
    fallback.pack_propagate(False)
    ctk.CTkLabel(fallback, text="A",
                 font=ctk.CTkFont(size=24, weight="bold"),
                 text_color="white").pack(expand=True)
    return fallback, None


def _get_base_dir() -> str:
    """Retorna o diretório base do executável ou script."""
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))
