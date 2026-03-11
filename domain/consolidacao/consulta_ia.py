"""Fluxo 4 — Consulta IA (Claude/Anthropic) em paralelo para correção de PNs."""

from __future__ import annotations

import json
import logging
import os
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import threading

import pandas as pd

try:
    from anthropic import Anthropic  # type: ignore
except Exception:
    Anthropic = None

logger = logging.getLogger(__name__)


class RateLimiter:
    """Limita tokens de input por janela (segundos)."""

    def __init__(self, limit_per_minute: int = 50_000, window_seconds: int = 60):
        self.limit = int(limit_per_minute)
        self.window = int(window_seconds)
        self.lock = threading.Lock()
        self.window_start = time.time()
        self.used = 0

    def _reset_if_needed(self):
        now = time.time()
        if now - self.window_start >= self.window:
            self.window_start = now
            self.used = 0

    def acquire(self, tokens: int):
        """Bloqueia até que haja tokens suficientes na janela atual."""
        tokens = int(tokens)
        while True:
            with self.lock:
                self._reset_if_needed()
                remaining = self.limit - self.used
                if tokens <= remaining:
                    self.used += tokens
                    return
                wait = max(0.0, self.window_start + self.window - time.time())
            logger.info(f"RateLimiter: aguardando {wait:.2f}s para liberar {tokens} tokens")
            time.sleep(wait + random.uniform(0.1, 0.5))


# ================================
# Prompt & Parsing
# ================================

def _hint_to_str(val: Any) -> str:
    """Normaliza hints para uma string segura."""
    if val is None:
        return ""
    if isinstance(val, (list, tuple)):
        return " ".join(str(x) for x in val if x is not None).strip()
    if isinstance(val, dict):
        return json.dumps(val, ensure_ascii=False)
    return str(val).strip()


def monta_prompt(row: pd.Series) -> str:
    """Cria o prompt de extração para UMA linha."""
    descricao = (
        str(row.get("descricao")
            or row.get("descripcion_mercancia")
            or row.get("description")
            or row.get("texto")
            or "")
    ).strip()

    marca_hint = _hint_to_str(row.get("marca_hint") or row.get("marca") or "")
    qtd_ext_hint = row.get("qtd_ext") or row.get("cantidade_ext") or None
    qtd_dcms_hint = row.get("qtd_dcms") or row.get("cantidade_dcms") or None

    schema = {
        "id": "int",
        "pn": ["string"],
        "marca": "string|null",
        "qtd": ["string|null"],
    }

    prompt = (
        "You are an information extraction engine.\n"
        "Return ONLY a single-line, minified JSON. No prose. No markdown. No keys beyond the schema.\n\n"
        f"Output schema (strict): {json.dumps(schema)}\n\n"
        "Rules (concise):\n"
        "- PNs: derive from descricao; uppercase; keep only [A-Z0-9./-]; split on non-alphanum; dedupe preserving order.\n"
        "- Brand: prefer explicit brand in descricao; else fallback to marca_hint; uppercase; only 1 brand.\n"
        "- Quantity: prefer number in descricao; else qtd_dcms_hint; else qtd_ext_hint; string if possible; if 'X' then add 'x'.\n"
        "- If a value is absent/unclear, use null (or [] for pn).\n\n"
        f"descricao: {json.dumps(descricao, ensure_ascii=False)}\n"
        f"marca_hint: {json.dumps(marca_hint, ensure_ascii=False)}\n"
        f"qtd_ext_hint: {json.dumps(qtd_ext_hint, ensure_ascii=False)}\n"
        f"qtd_dcms_hint: {json.dumps(qtd_dcms_hint, ensure_ascii=False)}\n"
    )
    return prompt


_JSON_OBJECT_RE = re.compile(r"\{[\s\S]*?\}")


def _find_first_json_object(text: str) -> Optional[str]:
    m = _JSON_OBJECT_RE.search(text)
    return m.group(0) if m else None


def extrair_json_da_resposta(text: str) -> Dict[str, Any]:
    """Tenta parsear o primeiro objeto JSON encontrado na resposta."""
    if isinstance(text, list):
        text = text[0] if text else ""
    elif isinstance(text, dict):
        text = json.dumps(text, ensure_ascii=False)
    raw = (text or "").strip()
    if not raw:
        raise ValueError("Resposta vazia do modelo")
    candidate = _find_first_json_object(raw) or raw
    try:
        return json.loads(candidate)
    except Exception as e:
        raise ValueError(f"Falha ao parsear JSON: {e}; resposta=<{raw[:2000]}>\n")


# ================================
# Cliente do Modelo
# ================================

@dataclass
class ModelConfig:
    model: str = "claude-3-5-sonnet-latest"
    max_tokens: int = 1024
    temperature: float = 0.0


class ClaudeClient:
    def __init__(self, api_key: Optional[str], cfg: ModelConfig):
        if Anthropic is None:
            raise RuntimeError("'anthropic' não instalado. pip install anthropic")
        api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError("API key não fornecida (parâmetro ou env ANTHROPIC_API_KEY)")
        self.client = Anthropic(api_key=api_key)
        self.cfg = cfg

    def infer(self, prompt: str) -> str:
        msg = self.client.messages.create(
            model=self.cfg.model,
            max_tokens=self.cfg.max_tokens,
            temperature=self.cfg.temperature,
            messages=[{"role": "user", "content": prompt}],
        )
        content = getattr(msg, "content", None)
        if isinstance(content, list):
            first = content[0] if content else ""
            if hasattr(first, "text"):
                return first.text
            if isinstance(first, dict) and "text" in first:
                return first["text"]
            return str(first)
        if isinstance(content, dict):
            if "text" in content:
                return content["text"]
            if "message" in content:
                return content["message"]
            return json.dumps(content, ensure_ascii=False)
        if isinstance(content, str):
            return content
        return str(getattr(msg, "content", msg))


# ================================
# Execução paralela com retries
# ================================

def _backoff_delay(attempt: int, base: float = 1.0, factor: float = 2.0,
                   jitter: float = 0.5) -> float:
    delay = base * (factor ** max(0, attempt - 1))
    delay += random.uniform(0, jitter * attempt)
    return min(delay, 60.0)


def _process_one(row: pd.Series, client: ClaudeClient, id_column: str,
                 retries: int, rate_limiter: Optional[RateLimiter]) -> Dict[str, Any]:
    rid = row.get(id_column, row.name)
    last_err: Optional[str] = None
    for attempt in range(1, retries + 1):
        try:
            prompt = monta_prompt(row)
            est_input_tokens = max(50, int(len(prompt.split()) * 0.9))
            if rate_limiter:
                rate_limiter.acquire(est_input_tokens)
            text = client.infer(prompt)
            data = extrair_json_da_resposta(text)
            data[id_column] = rid
            data["_raw"] = text
            data["_ok"] = True
            if "id" in data and id_column != "id":
                try:
                    del data["id"]
                except Exception:
                    pass
            return data
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            if "rate_limit" in last_err.lower() or "429" in last_err:
                extra_wait = random.uniform(5.0, 15.0) * attempt
                logger.warning(f"[{rid}] Rate limit detectado, aguardando {extra_wait:.1f}s")
                time.sleep(extra_wait)
            else:
                wait = _backoff_delay(attempt)
                logger.warning(f"[{rid}] Falha tentativa {attempt}/{retries}: {last_err}")
                time.sleep(wait)
    return {id_column: rid, "_ok": False, "_error": last_err}


def _process_parallel(df: pd.DataFrame, id_column: str, workers: int,
                      retries: int, client: ClaudeClient,
                      rate_limiter: Optional[RateLimiter]) -> pd.DataFrame:
    results: List[Dict[str, Any]] = []
    total = len(df)
    logger.info(f"Processando {total} linhas em paralelo com {workers} worker(s)...")

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {
            ex.submit(_process_one, row, client, id_column, retries, rate_limiter): int(row.get(id_column, idx))
            for idx, row in df.iterrows()
        }
        done = 0
        for fut in as_completed(futures):
            rid = futures[fut]
            try:
                res = fut.result()
                results.append(res)
            except Exception as e:
                results.append({id_column: rid, "_ok": False,
                                "_error": f"{type(e).__name__}: {e}"})
            finally:
                done += 1
                if done % 10 == 0 or done == total:
                    logger.info(f"Progresso: {done}/{total} ({done * 100.0 / total:.1f}%)")

    out = pd.DataFrame(results)
    if id_column in out.columns:
        try:
            out = out.sort_values(id_column)
        except Exception:
            pass
    return out


# ================================
# Função pública
# ================================

def consulta_ia(
    df: pd.DataFrame,
    api_key: str,
    *,
    id_column: str = "numero_formulario",
    workers: int = 8,
    retries: int = 3,
    model: str = "claude-haiku-4-5",
    max_tokens: int = 1024,
    temperature: float = 0.0,
    input_token_limit_per_minute: int = 50_000
) -> pd.DataFrame:
    """Processa o DataFrame em paralelo, chamando a IA por linha."""
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df deve ser um pandas.DataFrame")

    cfg = ModelConfig(model=model, max_tokens=max_tokens, temperature=temperature)
    client = ClaudeClient(api_key=api_key, cfg=cfg)
    rate_limiter = RateLimiter(limit_per_minute=input_token_limit_per_minute)

    if id_column not in df.columns:
        tmp = df.copy()
        tmp[id_column] = tmp.index
        df_to_use = tmp
    else:
        df_to_use = df

    out = _process_parallel(df=df_to_use, id_column=id_column, workers=workers,
                            retries=retries, client=client, rate_limiter=rate_limiter)

    cols = list(out.columns)
    aux = [c for c in ["_ok", "_error", "_raw"] if c in cols]
    prim = [c for c in cols if c not in aux]
    out = out[prim + aux]
    for c in ("pn", "marca", "qtd", "_ok"):
        if c not in out.columns:
            out[c] = None

    base_cols = ["part_numbers", "cantidade_ext", "cantidade_dcms",
                 "descripcion_mercancia", "marca"]
    df_base = df_to_use.copy()
    for c in base_cols:
        if c not in df_base.columns:
            df_base[c] = None

    left = df_base[[id_column] + base_cols].set_index(id_column)
    right = out[[id_column, "pn", "marca", "qtd", "_ok"]].set_index(id_column)
    merged = left.join(right, how="left", rsuffix="_ia").reset_index()

    if "marca_ia" in merged.columns:
        merged["marca"] = merged["marca_ia"].where(
            merged["marca_ia"].notna(), merged["marca"])
        merged = merged.drop(columns=["marca_ia"])

    COLUMNS = [
        "numero_formulario", "part_numbers", "pn", "marca", "cantidade_ext",
        "qtd", "cantidade_dcms", "descripcion_mercancia", "_ok",
    ]
    if id_column != "numero_formulario":
        merged = merged.rename(columns={id_column: "numero_formulario"})
    for c in COLUMNS:
        if c not in merged.columns:
            merged[c] = None

    return merged[COLUMNS]
