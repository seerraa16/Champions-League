# ingest.py
import os
from utils import list_csv, read_csv_safe, save_jsonl
from tqdm import tqdm
import glob

DATA_DIR = "data"
DOCS_DIR = "docs"
OUT_DIR = "generated_docs"

os.makedirs(OUT_DIR, exist_ok=True)

def ingest_csv(path):
    df = read_csv_safe(path)
    filename = os.path.basename(path)

    docs = []

    # === DOCUMENTO 1: SCHEMA ===
    schema = f"# Schema del archivo {filename}\nColumnas:\n- " + "\n- ".join(df.columns)
    docs.append({
        "doc_id": f"{filename}_schema",
        "source": path,
        "type": "schema",
        "text": schema
    })

    # === DOCUMENTO 2: RESUMEN GENERAL ===
    preview = df.head(10).fillna("").astype(str)
    preview_text = "\n".join([" | ".join(row) for row in preview.values])

    summary = f"# Resumen del archivo {filename}\nFilas: {len(df)}\nMuestra de datos:\n{preview_text}"
    docs.append({
        "doc_id": f"{filename}_summary",
        "source": path,
        "type": "summary",
        "text": summary
    })

    # === DOCUMENTO 3: FILAS (solo si es archivo de partidos) ===
    if {"HomeTeam", "AwayTeam", "Score"} & set(df.columns):
        for i, row in df.iterrows():
            t = f"Partido | Local: {row.get('HomeTeam','')} | Visitante: {row.get('AwayTeam','')} | Score: {row.get('Score','')} | Fecha: {row.get('Date','')} | Temporada Archivo: {filename}"
            docs.append({
                "doc_id": f"{filename}_row_{i}",
                "source": path,
                "type": "match_row",
                "text": t
            })

    return docs


def ingest_md(path):
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()

    filename = os.path.basename(path)

    return [{
        "doc_id": filename,
        "source": path,
        "type": "markdown",
        "text": text
    }]


def main():
    all_docs = []

    # === CSV ===
    for root, dirs, files in os.walk(DATA_DIR):
        for f in list_csv(root):
            all_docs.extend(ingest_csv(f))

    # === MARKDOWN ===
    for md in glob.glob(os.path.join(DOCS_DIR, "*.md")):
        all_docs.extend(ingest_md(md))

    save_jsonl(os.path.join(OUT_DIR, "documents.jsonl"), all_docs)
    print(f"Generados {len(all_docs)} documentos.")


if __name__ == "__main__":
    main()
