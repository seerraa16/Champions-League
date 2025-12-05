# query_rag.py
import json
import faiss
import numpy as np
from sentence_transformers import SentenceTransformer
import os
from openai import OpenAI

INDEX_DIR = "index"

client = OpenAI()

def load_index():
    index = faiss.read_index(os.path.join(INDEX_DIR, "faiss.index"))
    metadata = []
    with open(os.path.join(INDEX_DIR, "metadata.jsonl"), "r", encoding="utf-8") as f:
        for line in f:
            metadata.append(json.loads(line))
    return index, metadata

def retrieve(query, k=5):
    model = SentenceTransformer("all-MiniLM-L6-v2")
    qvec = model.encode([query]).astype("float32")
    D, I = index.search(qvec, k)
    return [(metadata[i], D[0][rank]) for rank, i in enumerate(I[0])]

def answer(query, k=5):
    retrieved = retrieve(query, k)
    context = "\n\n".join([r[0]["text"] for r in retrieved])

    prompt = f"""
Contesta a la pregunta usando SOLO este contexto:

{context}

Pregunta: {query}
Respuesta:
"""

    completion = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[{"role": "user", "content": prompt}]
    )

    return completion.choices[0].message["content"]

if __name__ == "__main__":
    index, metadata = load_index()

    while True:
        q = input("\n❓ Pregunta: ")
        print("\n📌 Respuesta:")
        print(answer(q))
