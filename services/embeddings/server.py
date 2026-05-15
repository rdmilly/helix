"""helix-embeddings — ONNX inference server.

BGE-large-en-v1.5 via onnxruntime. No PyTorch, no sentence-transformers.
Model loaded from pre-downloaded ONNX at MODEL_PATH.
3-4x faster on CPU than PyTorch, ~50MB runtime vs ~2GB.

API (unchanged — vector_store.py calls these):
  POST /embed   { texts: [...], normalize: bool } -> { embeddings: [[...]], model, dim }
  GET  /health  -> { status, model, dim, backend }
"""
import json
import logging
import os
import time
from typing import List

import numpy as np
import onnxruntime as ort
from fastapi import FastAPI
from pydantic import BaseModel
from tokenizers import Tokenizer

logger = logging.getLogger(__name__)

MODEL_PATH = os.environ.get(
    "MODEL_PATH",
    "/models/models--qdrant--bge-large-en-v1.5-onnx/snapshots/dc76b2c078fc38f0d243233d0ab0b51de925557e"
)
MODEL_NAME = "BAAI/bge-large-en-v1.5"
MAX_LENGTH = 512
DIM = 1024

print(f"Loading ONNX model from {MODEL_PATH}")
t0 = time.time()

# Load tokenizer from model dir
_tokenizer = Tokenizer.from_file(os.path.join(MODEL_PATH, "tokenizer.json"))
_tokenizer.enable_padding(pad_id=0, pad_token="[PAD]", length=MAX_LENGTH)
_tokenizer.enable_truncation(max_length=MAX_LENGTH)

# Load ONNX session — use all available CPU cores
_sess_opts = ort.SessionOptions()
_sess_opts.intra_op_num_threads = int(os.environ.get("OMP_NUM_THREADS", "2"))
_sess_opts.inter_op_num_threads = 1
_sess_opts.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL

_session = ort.InferenceSession(
    os.path.join(MODEL_PATH, "model.onnx"),
    sess_options=_sess_opts,
    providers=["CPUExecutionProvider"],
)

# Warm-up
_warmup = _session.run(None, {
    "input_ids": np.zeros((1, MAX_LENGTH), dtype=np.int64),
    "attention_mask": np.zeros((1, MAX_LENGTH), dtype=np.int64),
    "token_type_ids": np.zeros((1, MAX_LENGTH), dtype=np.int64),
})
print(f"ONNX model ready in {time.time()-t0:.2f}s — dim={_warmup[0].shape[-1]}")

app = FastAPI()


def _mean_pool(last_hidden: np.ndarray, attention_mask: np.ndarray) -> np.ndarray:
    """Mean pool over token dimension, respecting attention mask."""
    mask = attention_mask[:, :, np.newaxis].astype(np.float32)
    summed = (last_hidden * mask).sum(axis=1)
    counts = mask.sum(axis=1).clip(min=1e-9)
    return summed / counts


def _embed_batch(texts: List[str], normalize: bool = True) -> np.ndarray:
    encoded = _tokenizer.encode_batch(texts)
    input_ids      = np.array([e.ids              for e in encoded], dtype=np.int64)
    attention_mask = np.array([e.attention_mask   for e in encoded], dtype=np.int64)
    token_type_ids = np.array([e.type_ids         for e in encoded], dtype=np.int64)

    outputs = _session.run(None, {
        "input_ids":      input_ids,
        "attention_mask": attention_mask,
        "token_type_ids": token_type_ids,
    })
    # outputs[0] = last_hidden_state (batch, seq, 1024)
    pooled = _mean_pool(outputs[0], attention_mask)

    if normalize:
        norms = np.linalg.norm(pooled, axis=1, keepdims=True).clip(min=1e-9)
        pooled = pooled / norms

    return pooled


class EmbedRequest(BaseModel):
    texts: List[str]
    normalize: bool = True


@app.post("/embed")
def embed(req: EmbedRequest):
    if not req.texts:
        return {"embeddings": [], "model": MODEL_NAME, "dim": DIM}
    # Cap individual texts at 8000 chars (tokenizer handles truncation)
    texts = [t[:8000] for t in req.texts]
    vecs = _embed_batch(texts, normalize=req.normalize)
    return {
        "embeddings": vecs.tolist(),
        "model": MODEL_NAME,
        "dim": vecs.shape[1],
        "backend": "onnxruntime",
    }


@app.get("/health")
def health():
    return {"status": "ok", "model": MODEL_NAME, "dim": DIM, "backend": "onnxruntime"}
