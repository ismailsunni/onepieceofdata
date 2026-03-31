"""Generate embeddings for wiki chunks using sentence-transformers."""

from sentence_transformers import SentenceTransformer

MODEL_NAME = "all-MiniLM-L6-v2"  # 384 dims, ~80MB


def load_model():
    """Load the embedding model (downloads on first use, ~80MB)."""
    return SentenceTransformer(MODEL_NAME)


def embed_chunks(chunks: list, model=None, batch_size: int = 64) -> list:
    """Generate embeddings for all chunks.

    Adds 'embedding' key (list of floats) to each chunk dict.
    Shows progress bar via tqdm.
    Returns the same chunks list with embeddings added.
    """
    if not chunks:
        return chunks

    if model is None:
        model = load_model()

    try:
        from tqdm import tqdm
        show_progress = True
    except ImportError:
        show_progress = False

    texts = [chunk["text"] for chunk in chunks]

    # Encode in batches with optional progress bar
    if show_progress:
        all_embeddings = []
        for i in tqdm(range(0, len(texts), batch_size), desc="Embedding chunks"):
            batch = texts[i : i + batch_size]
            batch_embeddings = model.encode(batch, show_progress_bar=False)
            all_embeddings.extend(batch_embeddings)
    else:
        all_embeddings = model.encode(texts, batch_size=batch_size, show_progress_bar=False)

    for chunk, embedding in zip(chunks, all_embeddings):
        chunk["embedding"] = embedding.tolist()

    return chunks
