"""Domínio — Tratamento de dados (Equador, Argentina, Chile e Peru)."""
from domain.tratamento.equador import processar_equador
from domain.tratamento.argentina import processar_argentina, finalizar_argentina
from domain.tratamento.chile import processar_chile
from domain.tratamento.peru import processar_peru

__all__ = ["processar_equador", "processar_argentina", "finalizar_argentina",
           "processar_chile", "processar_peru"]
