# src/gcs_uploader.py
from __future__ import annotations
import os, sys, mimetypes, concurrent.futures
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional
from google.cloud import storage
from google.api_core.exceptions import NotFound

IGNORES = {".DS_Store"}

@dataclass
class UploadResult:
    status: str   # "upload" | "exist" | "skip"
    local: str
    remote: str
    size: int

def ensure_bucket(project_id: str, bucket_name: str, location: str = "us-east1") -> storage.Bucket:
    client = storage.Client(project=project_id) if project_id else storage.Client()
    try:
        return client.get_bucket(bucket_name)
    except NotFound:
        bucket = storage.Bucket(client, name=bucket_name)
        bucket.location = location
        return client.create_bucket(bucket)

def _upload_one(args) -> UploadResult:
    bucket_name, project_id, local, remote, force, dry_run, verbose = args
    client = storage.Client(project=project_id) if project_id else storage.Client()
    bucket = client.bucket(bucket_name)
    p = Path(local)
    size = p.stat().st_size

    if p.name in IGNORES:
        return UploadResult("skip", local, remote, size)

    blob = bucket.blob(remote)

    exists = False
    try:
        blob.reload()
        exists = True
    except NotFound:
        exists = False

    if dry_run:
        if verbose:
            print(f"[dry-run] {'REUPLOAD' if exists else 'UPLOAD'} gs://{bucket_name}/{remote}  <-- {local}")
        return UploadResult("exist" if exists else "upload", local, remote, size)

    if exists and not force:
        # light rsync heuristic: skip if same size
        if blob.size == size:
            if verbose:
                print(f"[skip=same-size] gs://{bucket_name}/{remote}")
            return UploadResult("exist", local, remote, size)

    ctype, _ = mimetypes.guess_type(local)
    if ctype:
        blob.content_type = ctype
    blob.cache_control = "no-cache"

    if verbose:
        action = "REUPLOAD" if exists else "UPLOAD"
        print(f"[{action}] gs://{bucket_name}/{remote}  <-- {local}")

    blob.upload_from_filename(local)
    return UploadResult("upload", local, remote, size)

def _norm_prefix(prefix: str) -> str:
    if not prefix:
        return ""
    return prefix.strip("/").rstrip("/") + "/"

def upload_tree(
    local_root: Path | str,
    bucket_name: str,
    prefix: str = "",
    project_id: Optional[str] = None,
    max_workers: int = 8,
    create_bucket_if_missing: bool = True,
    force: bool = False,
    dry_run: bool = False,
    verbose: bool = True,
) -> dict:
    """
    Mirrors a local folder into gs://<bucket>/<prefix>.
    Returns a summary: {"upload": N, "exist": M, "skip": K}
    """
    local_root = Path(local_root).expanduser().resolve()
    assert local_root.exists() and local_root.is_dir(), f"Missing folder: {local_root}"

    if create_bucket_if_missing:
        ensure_bucket(project_id, bucket_name)

    prefix = _norm_prefix(prefix)
    tasks = []
    for p in local_root.rglob("*"):
        if not p.is_file():
            continue
        rel = p.relative_to(local_root).as_posix()
        remote = f"{prefix}{rel}" if prefix else rel
        tasks.append((bucket_name, project_id, p.as_posix(), remote, force, dry_run, verbose))

    summary = {"upload": 0, "exist": 0, "skip": 0}
    if not tasks:
        if verbose:
            print("[info] no files found to upload.")
        return summary

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, max_workers)) as ex:
        for res in ex.map(_upload_one, tasks):
            summary[res.status] = summary.get(res.status, 0) + 1
    if verbose:
        print(f"[summary] uploaded={summary['upload']} exist={summary['exist']} skip={summary['skip']}")
    return summary

def upload_multiple(
    roots: Iterable[str | Path],
    bucket_name: str,
    project_id: Optional[str] = None,
    base_prefix: str = "dow30/",
    **kwargs,
):
    totals = {"upload": 0, "exist": 0, "skip": 0}
    base_prefix = _norm_prefix(base_prefix)
    for root in roots:
        root = Path(root).expanduser()
        if not root.exists():
            print(f"[warn] missing folder: {root}")
            continue
        print(f">>> Uploading {root} -> gs://{bucket_name}/{base_prefix}")
        s = upload_tree(root, bucket_name, prefix=base_prefix, project_id=project_id, **kwargs)
        for k, v in s.items():
            totals[k] = totals.get(k, 0) + v
    print(f"[total] uploaded={totals['upload']} exist={totals['exist']} skip={totals['skip']}")
    return totals

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-id", default=os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT") or "")
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", default="dow30/")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--force", action="store_true", help="Reupload even if object exists (overwrites).")
    ap.add_argument("--dry-run", action="store_true", help="Show what would be uploaded without doing it.")
    ap.add_argument("--quiet", action="store_true", help="Minimal logging.")
    ap.add_argument("roots", nargs="+", help="Local folders to mirror.")
    args = ap.parse_args()

    upload_multiple(
        roots=args.roots,
        bucket_name=args.bucket,
        project_id=args.project_id,
        base_prefix=args.prefix,
        max_workers=args.workers,
        force=args.force,
        dry_run=args.dry_run,
        verbose=not args.quiet,
        create_bucket_if_missing=True,
    )


# python ~/Desktop/Big_Data_2/src/gcs_uploader.py \--bucket fintrust-dow30-riya-20251010 \--project-id thinking-device-474702-m5 \--prefix dow30/ \--workers 12 \~/Desktop/Big_Data_2/downloads \
# python ~/Desktop/Big_Data_2/src/gcs_uploader.py \--bucket fintrust-dow30-riya-20251010 \--prefix dow30/ \ --workers 8 \ --force \~/Desktop/Big_Data_2/downloads