from __future__ import annotations

from typing import Protocol


class EmbeddingModel(Protocol):
    def encode(self, texts: list[str]) -> list[list[float]]: ...


class HashFallbackEmbedder:
    """
    Deterministic fallback when sentence-transformers is unavailable.
    Produces fixed-size pseudo-embeddings for pipeline testing only.
    """

    def __init__(self, dimensions: int = 384):
        self.dimensions = dimensions

    def encode(self, texts: list[str]) -> list[list[float]]:
        import hashlib
        import math

        vectors: list[list[float]] = []
        for text in texts:
            digest = hashlib.sha256(text.encode("utf-8")).digest()
            values = []
            for idx in range(self.dimensions):
                byte = digest[idx % len(digest)]
                values.append((byte / 127.5) - 1.0)
            norm = math.sqrt(sum(value * value for value in values)) or 1.0
            vectors.append([value / norm for value in values])
        return vectors


class _WrapSentenceTransformer:
    def __init__(self, model):
        self.model = model

    def encode(self, texts: list[str]) -> list[list[float]]:
        vectors = self.model.encode(
            texts,
            batch_size=min(len(texts), 256) if texts else 1,
            show_progress_bar=False,
            normalize_embeddings=True,
        )
        return [vector.tolist() for vector in vectors]


def load_embedder(model_name: str, allow_fallback: bool = True) -> EmbeddingModel:
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError as exc:
        if not allow_fallback:
            raise ImportError(
                "sentence-transformers is required for real embeddings. "
                "Install it with: pip install sentence-transformers"
            ) from exc
        return HashFallbackEmbedder()

    if allow_fallback:
        try:
            model = SentenceTransformer(model_name, local_files_only=True)
        except Exception:
            return HashFallbackEmbedder()
        return _WrapSentenceTransformer(model)

    try:
        model = SentenceTransformer(model_name)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to load embedding model {model_name!r}: {exc}"
        ) from exc
    return _WrapSentenceTransformer(model)
