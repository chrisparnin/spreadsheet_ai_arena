from __future__ import annotations
import os, shutil, subprocess, tempfile
from pathlib import Path

def fetch_git(url: str, ref: str | None, subdir: str | None, tmp_dir: Path) -> Path:
    """
    Clone a git repo shallowly and return the path to either the repo root
    or the requested subdir inside the checkout.
    """
    repo_dir = tmp_dir / "repo"
    # Prefer shallow clone
    subprocess.run(
        ["git", "clone", "--depth", "1"] + (["--branch", ref] if ref else []) + [url, str(repo_dir)],
        check=True, capture_output=True
    )
    if ref and not _is_branch_or_tag_checked(repo_dir, ref):
        # Fallback: fetch ref if it's not a branch/tag (e.g., commit SHA)
        subprocess.run(["git", "-C", str(repo_dir), "fetch", "origin", ref], check=True, capture_output=True)
        subprocess.run(["git", "-C", str(repo_dir), "checkout", ref], check=True, capture_output=True)

    root = repo_dir if not subdir else (repo_dir / subdir)
    if not root.exists():
        raise FileNotFoundError(f"subdir '{subdir}' not found in repo {url}")
    return root

def _is_branch_or_tag_checked(repo_dir: Path, ref: str) -> bool:
    try:
        out = subprocess.run(["git", "-C", str(repo_dir), "rev-parse", "--abbrev-ref", "HEAD"],
                             check=True, capture_output=True, text=True)
        if out.stdout.strip() == ref:
            return True
    except Exception:
        pass
    try:
        # Is current commit equal to ref?
        out = subprocess.run(["git", "-C", str(repo_dir), "rev-parse", "HEAD"],
                             check=True, capture_output=True, text=True)
        cur = out.stdout.strip()
        out2 = subprocess.run(["git", "-C", str(repo_dir), "rev-parse", ref],
                              check=True, capture_output=True, text=True)
        return cur == out2.stdout.strip()
    except Exception:
        return False
