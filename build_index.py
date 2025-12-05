# build_index.py
import json
from tqdm import tqdm
import faiss
from sentence_transformers import SentenceTransformer
import numpy as np
import os

IN_PATH = "generated_docs/documents.jsonl"
INDEX_DIR = "index"
os.makedirs(INDEX_DIR, exist_ok=True)

CHUNK_SIZE = 1000
OVERLAP = 200

def chunk_text(text):
    chunks = []
    start = 0
    while start < len(text):
        end = start + CHUNK_SIZE
        chunks.append(text[start:end])
        start += CHUNK_SIZE - OVERLAP
    return chunks

def main():
    model = SentenceTransformer("all-MiniLM-L6-v2")

    texts = []
    metadata = []

    # === LOAD DOCS ===
    with open(IN_PATH, "r", encoding="utf-8") as f:
        for line in f:
            doc = json.loads(line)
            for c in chunk_text(doc["text"]):
                texts.append(c)
                metadata.append(doc)

    print("Generando embeddings…")
    vecs = model.encode(texts, batch_size=32, show_progress_bar=True)
    vecs = np.array(vecs).astype("float32")

    index = faiss.IndexFlatL2(vecs.shape[1])
    index.add(vecs)

    faiss.write_index(index, os.path.join(INDEX_DIR, "faiss.index"))

    # metadata
    with open(os.path.join(INDEX_DIR, "metadata.jsonl"), "w", encoding="utf-8") as f:
        for m in metadata:
            f.write(json.dumps(m, ensure_ascii=False) + "\n")

    print("Índice creado correctamente.")


if __name__ == "__main__":
    main()
