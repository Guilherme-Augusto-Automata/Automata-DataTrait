"""Domínio — Tratamento de dados (Equador e Argentina)."""
from domain.tratamento.equador import processar_equador
from domain.tratamento.argentina import processar_argentina, finalizar_argentina

__all__ = ["processar_equador", "processar_argentina", "finalizar_argentina"]
