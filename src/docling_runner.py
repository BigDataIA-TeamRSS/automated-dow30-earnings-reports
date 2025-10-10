#!/usr/bin/env python3
"""
Docling batch parser for Big Data Assignment 2
-------------------------------------------------
• Scans an input directory (optionally recursive) for PDFs
• Converts each PDF with Docling to JSON/TXT/MD
• Exports table CSVs (with page/table indices)
• Emits lightweight provenance metadata as JSONL
• Writes a manifest.csv summarizing results across all files

Default layout (relative to repo root):
  downloads/                         # input PDFs (from your earlier steps)
  data/parsed/docling/               # outputs

Usage (from repo root):
  python3 -m src.docling_runner --input downloads --output data/parsed/docling --recursive
  # or
  python3 src/docling_runner.py --input downloads --output data/parsed/docling --recursive

Requires:
  pip install "docling>=1.7" pydantic>=2.4
"""
from __future__ import annotations
import argparse
import csv
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, List, Tuple

# --- Docling imports ---
from docling.document_converter import DocumentConverter
from docling.datamodel.document import DoclingDocument


# ============ Helpers: filesystem & printing ============

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p


# ============ Minimal provenance metadata (self-contained) ============

@dataclass
class ProvenanceRecord:
    pdf_path: str
    page: int
    block_type: str  # text | table | figure | heading | paragraph | etc.
    text: str
    bbox: dict
    section: str | None
    confidence: float | None
    extraction_method: str = "docling"
    additional_metadata: dict = None

    def to_json(self) -> str:
        d = asdict(self)
        # Keep JSON lines compact but readable
        return json.dumps(d, ensure_ascii=False)


def create_bbox(x0: float = 0, y0: float = 0, x1: float = 0, y1: float = 0) -> dict:
    return {"x0": float(x0 or 0), "y0": float(y0 or 0), "x1": float(x1 or 0), "y1": float(y1 or 0)}


def guess_section(text: str | None) -> str | None:
    if not text:
        return None
    t = text.strip().lower()
    # very light heuristics; extend for your use-case
    if "forward-looking" in t:
        return "forward_looking"
    if "management's discussion" in t or "management discussion" in t:
        return "md&a"
    if "risk factors" in t:
        return "risk_factors"
    if t.startswith("table "):
        return "table"
    return None


# ============ Docling table coercion & provenance helpers ============

def _coerce_table_to_list_of_lists(table_obj: Any) -> List[List[str]]:
    table_data: List[List[str]] = []
    try:
        data = getattr(table_obj, "data", None)
        if data:
            if isinstance(data, list):
                # Already rows
                return [["" if c is None else str(c) for c in row] for row in data]
            rows = getattr(data, "rows", None)
            if rows is not None:
                for r in rows:
                    table_data.append(["" if c is None else str(c) for c in r])
                return table_data
        # Sparse cells fallback
        cells = getattr(table_obj, "cells", None)
        if cells:
            temp = {}
            max_r = 0
            max_c = 0
            for c in cells:
                r = getattr(c, "row", 0)
                co = getattr(c, "col", 0)
                txt = getattr(c, "text", "")
                temp.setdefault(r, {})[co] = "" if txt is None else str(txt)
                max_r = max(max_r, r)
                max_c = max(max_c, co)
            for r in range(max_r + 1):
                row_vals = [temp.get(r, {}).get(c, "") for c in range(max_c + 1)]
                table_data.append(row_vals)
            return table_data
    except Exception:
        pass
    return table_data


def _first_prov(prov_list: Any) -> Tuple[int, dict]:
    """Return (page_no, bbox) from a Docling provenance list/dict/object."""
    try:
        if not prov_list:
            return 1, create_bbox()
        prov = prov_list[0]
        # page_no can be attribute or dict key
        page_no = getattr(prov, "page_no", None)
        if page_no is None and isinstance(prov, dict):
            page_no = prov.get("page_no")
        # bbox may have l,t,r,b or x0,y0,x1,y1 depending on version
        bbox_obj = getattr(prov, "bbox", None)
        if bbox_obj is None and isinstance(prov, dict):
            bbox_obj = prov.get("bbox")
        if bbox_obj is not None:
            l = getattr(bbox_obj, "l", None) if not isinstance(bbox_obj, dict) else bbox_obj.get("l")
            t = getattr(bbox_obj, "t", None) if not isinstance(bbox_obj, dict) else bbox_obj.get("t")
            r = getattr(bbox_obj, "r", None) if not isinstance(bbox_obj, dict) else bbox_obj.get("r")
            b = getattr(bbox_obj, "b", None) if not isinstance(bbox_obj, dict) else bbox_obj.get("b")
            if None not in (l, t, r, b):
                return int(page_no or 1), create_bbox(l, t, r, b)
            # try x0/y0/x1/y1
            x0 = getattr(bbox_obj, "x0", None) if not isinstance(bbox_obj, dict) else bbox_obj.get("x0")
            y0 = getattr(bbox_obj, "y0", None) if not isinstance(bbox_obj, dict) else bbox_obj.get("y0")
            x1 = getattr(bbox_obj, "x1", None) if not isinstance(bbox_obj, dict) else bbox_obj.get("x1")
            y1 = getattr(bbox_obj, "y1", None) if not isinstance(bbox_obj, dict) else bbox_obj.get("y1")
            if None not in (x0, y0, x1, y1):
                return int(page_no or 1), create_bbox(x0, y0, x1, y1)
        return int(page_no or 1), create_bbox()
    except Exception:
        return 1, create_bbox()


# ============ Core runner ============

class DoclingRunner:
    def __init__(self, input_dir: Path, output_dir: Path, recursive: bool = False, pattern: str = "*.pdf"):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.recursive = recursive
        self.pattern = pattern

        # output structure
        self.out_docs = ensure_dir(self.output_dir)
        self.out_tables = ensure_dir(self.output_dir / "extracted_tables")
        self.out_meta = ensure_dir(self.output_dir / "metadata_jsonl")

        self.converter = DocumentConverter()

    # ---------- Discovery ----------
    def discover_pdfs(self) -> List[Path]:
        if self.recursive:
            pdfs = sorted(self.input_dir.rglob(self.pattern))
        else:
            pdfs = sorted(self.input_dir.glob(self.pattern))
        return [p for p in pdfs if p.is_file()]

    # ---------- Conversion & exports ----------
    def convert_one(self, pdf_path: Path) -> Tuple[bool, dict]:
        info = {
            "pdf": str(pdf_path),
            "base_name": pdf_path.stem,
            "json": None,
            "txt": None,
            "md": None,
            "tables_exported": 0,
            "num_pages": None,
            "num_texts": 0,
            "num_tables": 0,
            "num_pictures": 0,
            "error": None,
        }
        try:
            result = self.converter.convert(str(pdf_path))
            doc: DoclingDocument = result.document

            # counts
            info["num_pages"] = len(getattr(doc, "pages", []) or [])
            info["num_texts"] = len(getattr(doc, "texts", []) or [])
            info["num_tables"] = len(getattr(doc, "tables", []) or [])
            info["num_pictures"] = len(getattr(doc, "pictures", []) or [])

            # exports: json/txt/md
            info["json"] = self._export_doc(doc, pdf_path.stem, "json")
            info["txt"] = self._export_doc(doc, pdf_path.stem, "txt")
            info["md"] = self._export_doc(doc, pdf_path.stem, "md")

            # metadata JSONL
            self._export_metadata_jsonl(doc, pdf_path)

            # tables CSVs
            exported = self._export_tables(doc, pdf_path.stem)
            info["tables_exported"] = exported
            return True, info
        except Exception as e:
            info["error"] = f"{type(e).__name__}: {e}"
            return False, info

    def _export_doc(self, doc: DoclingDocument, base: str, kind: str) -> str:
        out_path = self.out_docs / f"{base}.{kind}"
        if kind == "json":
            with out_path.open("w", encoding="utf-8") as f:
                if hasattr(doc, "model_dump_json"):
                    f.write(doc.model_dump_json(indent=2))
                elif hasattr(doc, "model_dump"):
                    f.write(json.dumps(doc.model_dump(), indent=2))
                else:
                    f.write(str(doc))
        elif kind == "txt":
            with out_path.open("w", encoding="utf-8") as f:
                f.write(doc.export_to_text())
        elif kind == "md":
            with out_path.open("w", encoding="utf-8") as f:
                f.write(doc.export_to_markdown())
        return str(out_path)

    # ---------- Metadata ----------
    def _export_metadata_jsonl(self, doc: DoclingDocument, pdf_path: Path) -> None:
        base = pdf_path.stem
        out_jsonl = self.out_meta / f"{base}.jsonl"
        with out_jsonl.open("w", encoding="utf-8") as f:
            # Top-level texts
            texts = getattr(doc, "texts", []) or []
            for tb in texts:
                text = getattr(tb, "text", "") or ""
                if not text.strip():
                    continue
                page, bbox = _first_prov(getattr(tb, "prov", []) or [])
                rec = ProvenanceRecord(
                    pdf_path=str(pdf_path),
                    page=page,
                    block_type=(getattr(tb, "label", None) or "text").lower(),
                    text=text,
                    bbox=bbox,
                    section=guess_section(text),
                    confidence=getattr(tb, "confidence", None),
                    additional_metadata={"label": getattr(tb, "label", None)},
                )
                f.write(rec.to_json() + "\n")

            # Top-level tables
            tables = getattr(doc, "tables", []) or []
            for idx, table in enumerate(tables, start=1):
                page, bbox = _first_prov(getattr(table, "prov", []) or [])
                text = getattr(table, "text", "") or ""
                rec = ProvenanceRecord(
                    pdf_path=str(pdf_path),
                    page=page,
                    block_type="table",
                    text=text,
                    bbox=bbox,
                    section=None,
                    confidence=getattr(table, "confidence", None),
                    additional_metadata={
                        "table_index": idx,
                        "rows": len(getattr(table, "data", []) or []),
                    },
                )
                f.write(rec.to_json() + "\n")

            # Top-level pictures
            pics = getattr(doc, "pictures", []) or []
            for idx, pic in enumerate(pics, start=1):
                page, bbox = _first_prov(getattr(pic, "prov", []) or [])
                text = getattr(pic, "text", "") or ""
                rec = ProvenanceRecord(
                    pdf_path=str(pdf_path),
                    page=page,
                    block_type="figure",
                    text=text,
                    bbox=bbox,
                    section=None,
                    confidence=getattr(pic, "confidence", None),
                    additional_metadata={"figure_index": idx, "figure_type": getattr(pic, "type", "unknown")},
                )
                f.write(rec.to_json() + "\n")

            # Page-level fallbacks (texts/tables/figures)
            for p_idx, page in enumerate(getattr(doc, "pages", []) or [], start=1):
                # texts
                for t_idx, tb in enumerate(getattr(page, "texts", []) or [], start=1):
                    text = getattr(tb, "text", "") or ""
                    if not text.strip():
                        continue
                    bbox = create_bbox()
                    if hasattr(tb, "bbox") and getattr(tb, "bbox") is not None:
                        b = tb.bbox
                        bbox = create_bbox(getattr(b, "x0", 0), getattr(b, "y0", 0), getattr(b, "x1", 0), getattr(b, "y1", 0))
                    rec = ProvenanceRecord(
                        pdf_path=str(pdf_path),
                        page=p_idx,
                        block_type=(getattr(tb, "label", None) or "text").lower(),
                        text=text,
                        bbox=bbox,
                        section=guess_section(text),
                        confidence=getattr(tb, "confidence", None),
                        additional_metadata={"text_index": t_idx, "label": getattr(tb, "label", None)},
                    )
                    f.write(rec.to_json() + "\n")
                # tables
                for t_idx, table in enumerate(getattr(page, "tables", []) or [], start=1):
                    text = getattr(table, "text", "") or ""
                    bbox = create_bbox()
                    if hasattr(table, "bbox") and getattr(table, "bbox") is not None:
                        b = table.bbox
                        bbox = create_bbox(getattr(b, "x0", 0), getattr(b, "y0", 0), getattr(b, "x1", 0), getattr(b, "y1", 0))
                    rec = ProvenanceRecord(
                        pdf_path=str(pdf_path),
                        page=p_idx,
                        block_type="table",
                        text=text,
                        bbox=bbox,
                        section=None,
                        confidence=getattr(table, "confidence", None),
                        additional_metadata={"table_index": t_idx},
                    )
                    f.write(rec.to_json() + "\n")
                # figures
                for g_idx, fig in enumerate(getattr(page, "figures", []) or [], start=1):
                    text = getattr(fig, "text", "") or ""
                    bbox = create_bbox()
                    if hasattr(fig, "bbox") and getattr(fig, "bbox") is not None:
                        b = fig.bbox
                        bbox = create_bbox(getattr(b, "x0", 0), getattr(b, "y0", 0), getattr(b, "x1", 0), getattr(b, "y1", 0))
                    rec = ProvenanceRecord(
                        pdf_path=str(pdf_path),
                        page=p_idx,
                        block_type="figure",
                        text=text,
                        bbox=bbox,
                        section=None,
                        confidence=getattr(fig, "confidence", None),
                        additional_metadata={"figure_index": g_idx, "figure_type": getattr(fig, "type", "unknown")},
                    )
                    f.write(rec.to_json() + "\n")

    # ---------- Tables ----------
    def _export_tables(self, doc: DoclingDocument, base: str) -> int:
        exported = 0
        tables = getattr(doc, "tables", []) or []
        if tables:
            for idx, table in enumerate(tables, start=1):
                rows = _coerce_table_to_list_of_lists(table)
                page, _ = _first_prov(getattr(table, "prov", []) or [])
                if rows:
                    name = f"{base}_page{page}_table{idx}.csv"
                    with (self.out_tables / name).open("w", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        writer.writerows(rows)
                    exported += 1
        else:
            # page-level fallback
            for p_idx, page in enumerate(getattr(doc, "pages", []) or [], start=1):
                for t_idx, table in enumerate(getattr(page, "tables", []) or [], start=1):
                    rows = _coerce_table_to_list_of_lists(table)
                    if rows:
                        name = f"{base}_page{p_idx}_table{t_idx}.csv"
                        with (self.out_tables / name).open("w", newline="", encoding="utf-8") as f:
                            writer = csv.writer(f)
                            writer.writerows(rows)
                        exported += 1
        # markdown fallback (coarse)
        if exported == 0:
            try:
                md = doc.export_to_markdown()
                md_tables = _parse_markdown_tables(md)
                for idx, (page_hint, rows) in enumerate(md_tables, start=1):
                    pg = page_hint if page_hint is not None else 1
                    name = f"{base}_page{pg}_table{idx}.csv"
                    with (self.out_tables / name).open("w", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        writer.writerows(rows)
                    exported += 1
            except Exception:
                pass
        return exported


# ============ Markdown table fallback parser ============

def _parse_markdown_tables(markdown_text: str) -> List[Tuple[int | None, List[List[str]]]]:
    """
    Very simple GitHub-style pipe-table parser.
    Returns list of (page_hint, rows[list[list[str]]]).
    """
    tables: List[Tuple[int | None, List[List[str]]]] = []
    lines = markdown_text.splitlines()
    current_page: int | None = None
    i = 0
    import re
    while i < len(lines):
        line = lines[i].strip()
        # Update page hint if heading mentions 'page N'
        if line.startswith('#') and 'page' in line.lower():
            m = re.search(r'(?:page|pg)\s*(\d+)', line, flags=re.I)
            if m:
                current_page = int(m.group(1))
        if '|' in line and not line.startswith('```'):
            if i + 1 < len(lines):
                sep = lines[i + 1].strip()
                if set(sep.replace('|', '').replace(' ', '').replace(':', '')) <= set('-') and '|' in sep:
                    rows: List[List[str]] = []
                    header = line
                    rows.append([c.strip() for c in header.strip('|').split('|')])
                    i += 2
                    while i < len(lines):
                        row = lines[i].strip()
                        if not row or '|' not in row or row.startswith('```'):
                            break
                        rows.append([c.strip() for c in row.strip('|').split('|')])
                        i += 1
                    # normalize
                    max_cols = max((len(r) for r in rows), default=0)
                    norm = [r + [''] * (max_cols - len(r)) for r in rows]
                    tables.append((current_page, norm))
                    continue
        i += 1
    return tables


# ============ Manifest writer ============

def write_manifest(rows: Iterable[dict], out_csv: Path) -> None:
    rows = list(rows)
    if not rows:
        return
    keys = [
        "pdf", "base_name", "num_pages", "num_texts", "num_tables", "num_pictures",
        "json", "txt", "md", "tables_exported", "error"
    ]
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in keys})


# ============ CLI ============

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Batch-parse PDFs with Docling.")
    p.add_argument("--input", type=str, default="downloads", help="Input directory containing PDFs")
    p.add_argument("--output", type=str, default="data/parsed/docling", help="Output directory for parsed artifacts")
    p.add_argument("--recursive", action="store_true", help="Recurse into subdirectories")
    p.add_argument("--pattern", type=str, default="*.pdf", help="Glob pattern for files (default: *.pdf)")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    input_dir = Path(args.input).resolve()
    output_dir = Path(args.output).resolve()

    if not input_dir.exists():
        eprint(f"[error] Input directory not found: {input_dir}")
        return 2

    runner = DoclingRunner(input_dir=input_dir, output_dir=output_dir, recursive=args.recursive, pattern=args.pattern)

    pdfs = runner.discover_pdfs()
    print(f"Looking for PDFs in: {input_dir}")
    print(f"Found {len(pdfs)} PDF file(s)")
    if not pdfs:
        return 0

    manifest_rows = []
    for i, pdf in enumerate(pdfs, start=1):
        print(f"[{i}/{len(pdfs)}] Processing: {pdf.relative_to(input_dir)}")
        ok, info = runner.convert_one(pdf)
        if ok:
            print(f"   ✔ OK — tables: {info['tables_exported']}, pages: {info['num_pages']}")
        else:
            print(f"   ✖ ERROR — {info['error']}")
        manifest_rows.append(info)

    ensure_dir(output_dir)
    out_manifest = output_dir / "manifest.csv"
    write_manifest(manifest_rows, out_manifest)
    print(f"\nSummary manifest written to: {out_manifest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
