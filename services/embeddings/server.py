from fastapi import FastAPI
from sentence_transformers import SentenceTransformer
from pydantic import BaseModel
import os

MODEL = os.environ.get("EMBEDDING_MODEL", "BAAI/bge-large-en-v1.5")
print(f"Loading model: {MODEL}")
model = SentenceTransformer(MODEL)
print(f"Model loaded: {MODEL}")

app = FastAPI()

class EmbedRequest(BaseModel):
    texts: list[str]
    normalize: bool = True

@app.post("/embed")
def embed(req: EmbedRequest):
    embeddings = model.encode(
        req.texts,
        normalize_embeddings=req.normalize,
        show_progress_bar=False
    ).tolist()
    return {"embeddings": embeddings, "model": MODEL, "dim": len(embeddings[0])}

@app.get("/health")
def health():
    return {"status": "ok", "model": MODEL}
