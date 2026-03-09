"""Domínio — Tratamento de dados (Equador, Argentina e Chile)."""
from domain.tratamento.equador import processar_equador
from domain.tratamento.argentina import processar_argentina, finalizar_argentina
from domain.tratamento.chile import processar_chile

__all__ = ["processar_equador", "processar_argentina", "finalizar_argentina",
           "processar_chile"]
