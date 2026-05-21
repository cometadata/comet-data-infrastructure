from dataclasses import dataclass
import glob
import json
import os
import re

import pyarrow.parquet as pq


def normalize(text: str) -> str:
    text = re.sub(r"\$\$.*?\$\$", " MATH ", text, flags=re.DOTALL)
    text = re.sub(r"\$[^$]*?\$", " MATH ", text)
    text = text.lower()
    text = re.sub(r"[^a-z0-9 ]", " ", text)
    return re.sub(r" +", " ", text).strip()


def make_ngrams(text: str, n: int = 5) -> set[tuple[str, ...]]:
    words = normalize(text).split()
    return {tuple(words[i : i + n]) for i in range(len(words) - n + 1)}


def normalize_for_funding(text: str) -> str:
    text = text.lower()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^a-z0-9 ]", " ", text)
    return re.sub(r" +", " ", text).strip()


def statement_retained(statement: str, extracted: str, threshold: float = 0.8) -> bool:
    norm_stmt = normalize_for_funding(statement)
    norm_ext = normalize_for_funding(extracted)

    stmt_words = norm_stmt.split()
    if len(stmt_words) < 5:
        return norm_stmt in norm_ext

    stmt_ng = {tuple(stmt_words[i : i + 5]) for i in range(len(stmt_words) - 4)}
    ext_words = norm_ext.split()
    ext_ng = {tuple(ext_words[i : i + 5]) for i in range(len(ext_words) - 4)}

    if not stmt_ng:
        return True
    overlap = len(stmt_ng & ext_ng) / len(stmt_ng)
    return overlap >= threshold


@dataclass
class PaperResult:
    arxiv_id: str
    split: str
    recall: float = 0.0
    precision: float = 0.0
    f1: float = 0.0
    our_words: int = 0
    vlm_words: int = 0
    n_statements: int = 0
    statements_retained: int = 0
    failed: bool = False


def load_vlm_references(dataset_dir: str) -> dict:
    refs = {}
    for split_name in ("train.jsonl", "test.jsonl"):
        path = os.path.join(dataset_dir, split_name)
        if not os.path.exists(path):
            continue
        split_label = split_name.replace(".jsonl", "")
        with open(path) as f:
            for line in f:
                rec = json.loads(line)
                if rec["category"] == ["clean"]:
                    aid = rec["file"].replace("md/", "").replace(".md", "")
                    refs[aid] = {
                        "text": rec["text"],
                        "statements": rec.get("statements", []),
                        "split": split_label,
                    }
    return refs


def load_extractions_from_dir(extract_dir: str) -> dict[str, str]:
    """Load extracted texts from a directory of .txt files."""
    extractions: dict[str, str] = {}
    for fname in sorted(os.listdir(extract_dir)):
        if not fname.endswith(".txt"):
            continue
        aid = fname.replace(".txt", "")
        with open(os.path.join(extract_dir, fname)) as f:
            extractions[aid] = f.read()
    return extractions


def load_extractions_from_parquet(parquet_path: str, text_column: str = "markdown") -> dict[str, str]:
    """Load extracted texts from Parquet file(s) with arxiv_id and a text column."""
    if os.path.isdir(parquet_path):
        files = sorted(glob.glob(os.path.join(parquet_path, "*.parquet")))
    else:
        files = parquet_path
    table = pq.read_table(files, columns=["arxiv_id", text_column])
    ids = table.column("arxiv_id").to_pylist()
    texts = table.column(text_column).to_pylist()
    return {aid: (t if t is not None else "") for aid, t in zip(ids, texts)}


def evaluate(extractions: dict[str, str], dataset_dir: str) -> list[PaperResult]:
    refs = load_vlm_references(dataset_dir)
    results = []

    for aid in sorted(extractions):
        if aid not in refs:
            continue

        our_text = extractions[aid]
        ref = refs[aid]
        pr = PaperResult(arxiv_id=aid, split=ref["split"])

        our_norm_words = len(normalize(our_text).split())
        vlm_norm_words = len(normalize(ref["text"]).split())
        pr.our_words = our_norm_words
        pr.vlm_words = vlm_norm_words

        if our_norm_words < 100:
            pr.failed = True
            results.append(pr)
            continue

        vlm_ng = make_ngrams(ref["text"])
        our_ng = make_ngrams(our_text)

        if vlm_ng and our_ng:
            overlap = len(our_ng & vlm_ng)
            pr.recall = overlap / len(vlm_ng)
            pr.precision = overlap / len(our_ng)
            pr.f1 = 2 * pr.recall * pr.precision / (pr.recall + pr.precision) if (pr.recall + pr.precision) else 0.0

        stmts = ref["statements"]
        pr.n_statements = len(stmts)
        if stmts:
            pr.statements_retained = sum(1 for s in stmts if statement_retained(s, our_text))

        results.append(pr)

    return results


def print_report(results: list[PaperResult]):
    active = [r for r in results if not r.failed]
    failed = [r for r in results if r.failed]

    for split in ("train", "test", "all"):
        subset = active if split == "all" else [r for r in active if r.split == split]
        if not subset:
            continue

        n = len(subset)
        recalls = sorted(r.recall for r in subset)
        precs = sorted(r.precision for r in subset)
        f1s = sorted(r.f1 for r in subset)
        mean = lambda xs: sum(xs) / len(xs) if xs else 0
        median = lambda xs: xs[len(xs) // 2] if xs else 0
        p25 = lambda xs: xs[len(xs) // 4] if xs else 0
        p75 = lambda xs: xs[3 * len(xs) // 4] if xs else 0

        print(f"\n{'=' * 66}")
        print(
            f"  {split.upper()} split — {n} papers (+ {len(failed) if split == 'all' else sum(1 for r in failed if r.split == split)} pandoc failures)"
        )
        print(f"{'=' * 66}")
        print(f"  {'Metric':<12} {'Mean':>8} {'Median':>8} {'P25':>8} {'P75':>8} {'Min':>8} {'Max':>8}")
        for label, vals in [("Recall", recalls), ("Precision", precs), ("F1", f1s)]:
            print(
                f"  {label:<12} {mean(vals):>8.3f} {median(vals):>8.3f}"
                f" {p25(vals):>8.3f} {p75(vals):>8.3f}"
                f" {min(vals):>8.3f} {max(vals):>8.3f}"
            )

        buckets = [
            (0, 0.2),
            (0.2, 0.4),
            (0.4, 0.6),
            (0.6, 0.7),
            (0.7, 0.8),
            (0.8, 0.9),
            (0.9, 1.01),
        ]
        print("\n  Recall distribution:")
        for lo, hi in buckets:
            count = sum(1 for r in recalls if lo <= r < hi)
            bar = "#" * count
            print(f"    {lo * 100:>3.0f}-{hi * 100:>3.0f}%: {count:>3}  {bar}")

    print(f"\n{'=' * 66}")
    print("  FUNDING STATEMENT PRESERVATION")
    print(f"{'=' * 66}")

    for split in ("train", "test", "all"):
        subset = (
            [r for r in active if r.n_statements > 0]
            if split == "all"
            else [r for r in active if r.n_statements > 0 and r.split == split]
        )
        if not subset:
            continue

        total_stmts = sum(r.n_statements for r in subset)
        retained_stmts = sum(r.statements_retained for r in subset)
        papers_full = sum(1 for r in subset if r.statements_retained == r.n_statements)
        papers_partial = sum(1 for r in subset if 0 < r.statements_retained < r.n_statements)
        papers_none = sum(1 for r in subset if r.statements_retained == 0)

        stmt_rate = retained_stmts / total_stmts if total_stmts else 0
        paper_rate = papers_full / len(subset) if subset else 0

        print(f"\n  {split.upper()} ({len(subset)} papers with statements)")
        print(f"    Statements: {retained_stmts}/{total_stmts} retained ({stmt_rate:.1%})")
        print(
            f"    Papers — all retained: {papers_full} ({paper_rate:.1%})"
            f"  partial: {papers_partial}  none: {papers_none}"
        )

    lost = [r for r in active if r.n_statements > 0 and r.statements_retained < r.n_statements]
    if lost:
        lost.sort(key=lambda r: r.statements_retained / r.n_statements)
        print(f"\n  Papers with missing statements ({len(lost)}):")
        print(f"    {'ID':<22} {'Split':<6} {'Kept':>5} {'Total':>6} {'F1':>6}")
        for r in lost[:20]:
            print(f"    {r.arxiv_id:<22} {r.split:<6} {r.statements_retained:>5} {r.n_statements:>6} {r.f1:>6.3f}")

    worst = sorted(active, key=lambda r: r.f1)[:10]
    print("\n  Bottom 10 by F1:")
    print(f"    {'ID':<22} {'Split':<6} {'Recall':>8} {'Prec':>8} {'F1':>8} {'Words':>12}")
    for r in worst:
        print(
            f"    {r.arxiv_id:<22} {r.split:<6}"
            f" {r.recall:>8.3f} {r.precision:>8.3f} {r.f1:>8.3f}"
            f" {r.our_words:>5}/{r.vlm_words:<5}"
        )

    if failed:
        print(f"\n  Failures ({len(failed)}):")
        for r in sorted(failed, key=lambda r: r.arxiv_id):
            print(f"    {r.arxiv_id} ({r.split}, {r.our_words} words)")
