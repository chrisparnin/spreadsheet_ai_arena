from pathlib import Path

def dataset_path(base: Path, did: str) -> Path:
    return (base / did).resolve()

def ensure_dir(p: Path): p.mkdir(parents=True, exist_ok=True)

def write_marker(dst: Path, text: str):
    (dst / ".arena_placeholder").write_text(text + "\n", encoding="utf-8")
