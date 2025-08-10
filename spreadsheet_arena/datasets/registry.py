from dataclasses import dataclass

@dataclass(frozen=True)
class Source:
    type: str        # "huggingface" | "git_lfs" | "s3"
    repo_id: str | None = None
    uri: str | None = None

@dataclass(frozen=True)
class DatasetMeta:
    name: str
    tasks: int
    version: str
    source: Source

DATASET_REGISTRY: dict[str, DatasetMeta] = {
    "benchmark-tasks/basic": DatasetMeta(
        name="Basic Tasks", tasks=10, version="1.0.0",
        source=Source(type="huggingface", repo_id="your-org/spreadsheet-basic")
    ),
    "benchmark-tasks/advanced": DatasetMeta(
        name="Advanced Tasks", tasks=25, version="1.1.0",
        source=Source(type="huggingface", repo_id="your-org/spreadsheet-advanced")
    ),
}

def normalize_id(i: str) -> str:
    return i if "/" in i else f"benchmark-tasks/{i}"
