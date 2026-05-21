"""Parse arXiv tar archives and extract LaTeX files to disk."""

from collections.abc import Iterator
from dataclasses import dataclass
import enum
import gzip
import io
import logging
from pathlib import Path
import tarfile

import magic

log = logging.getLogger(__name__)

TEX_EXTENSIONS: frozenset[str] = frozenset({".tex", ".bbl", ".ltx", ".latex"})


class FileType(enum.Enum):
    """File type detected from submission content."""

    TEX = "tex"
    PDF = "pdf"
    POSTSCRIPT = "postscript"
    HTML = "html"
    UNKNOWN = "unknown"


@dataclass
class PaperArchive:
    """A paper extracted from an arXiv source tar archive."""

    arxiv_id: str
    path: Path
    tex_files: list[Path]
    file_type: FileType
    entry_name: str


def detect_file_type(data: bytes) -> FileType:
    """Detect file type from raw bytes using libmagic.

    Args:
        data: Raw file content bytes.

    Returns:
        Detected FileType.
    """
    if not data:
        return FileType.UNKNOWN
    mime = magic.from_buffer(data, mime=True)
    if mime == "application/pdf":
        return FileType.PDF
    if mime == "application/postscript":
        return FileType.POSTSCRIPT
    if mime == "text/html":
        return FileType.HTML
    if mime in ("text/x-tex", "application/x-tex"):
        return FileType.TEX
    if mime.startswith("text/"):
        return FileType.TEX
    return FileType.UNKNOWN


def derive_arxiv_id(path: str) -> str:
    """Derive an arXiv ID from a tar entry path.

    Takes the basename and strips known archive extensions in order.

    Args:
        path: Tar entry path, e.g. "2401/2401.00001.tar.gz".

    Returns:
        The arXiv ID, e.g. "2401.00001".
    """
    name = path.rsplit("/", 1)[-1]
    for suffix in (".tar.gz", ".tgz", ".gz", ".tex", ".pdf"):
        name = name.removesuffix(suffix)
    return name


def extract_inner_tar_gz(raw: bytes, output_dir: Path) -> list[Path]:
    """Extract files from an inner .tar.gz archive to disk.

    Extracts all files to output_dir. Returns only the paths to TeX-related
    files (.tex, .bbl, .ltx, .latex).

    Args:
        raw: Raw bytes of the .tar.gz archive.
        output_dir: Directory to extract files into.

    Returns:
        Sorted list of paths to extracted TeX files.
    """
    try:
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as tar:
            members = tar.getmembers()
            tar.extractall(output_dir, filter="data")
    except (tarfile.TarError, gzip.BadGzipFile, EOFError, OSError):
        return []

    tex_files = [
        output_dir / member.name
        for member in members
        if member.isfile() and Path(member.name).suffix.lower() in TEX_EXTENSIONS
    ]
    return sorted(tex_files)


def classify_gz(raw: bytes, arxiv_id: str, output_dir: Path) -> tuple[list[Path], FileType]:
    """Decompress a .gz file, detect its type, and write to disk if TeX.

    Args:
        raw: Raw gzip-compressed bytes.
        arxiv_id: The arXiv ID for naming the output file.
        output_dir: Directory to write the decompressed file into.

    Returns:
        Tuple of (list of TeX file paths, detected FileType).
    """
    try:
        decompressed = gzip.decompress(raw)
    except (gzip.BadGzipFile, EOFError, OSError):
        return [], FileType.UNKNOWN

    file_type = detect_file_type(decompressed)
    if file_type == FileType.TEX:
        output_dir.mkdir(parents=True, exist_ok=True)
        file_path = output_dir / f"{arxiv_id}.tex"
        file_path.write_bytes(decompressed)
        return [file_path], FileType.TEX
    return [], file_type


def classify_and_extract(raw_bytes: bytes, entry_name: str, arxiv_id: str, paper_dir: Path) -> PaperArchive:
    """Classify raw bytes by file type and extract to disk.

    Branches on the entry name extension to determine how to handle the
    content. For .tar.gz/.tgz/.gz entries, attempts inner tar extraction
    first, then falls back to single-file gzip classification.

    Args:
        raw_bytes: Raw file content bytes.
        entry_name: Original filename or tar entry path (used for extension matching).
        arxiv_id: The arXiv ID for this paper.
        paper_dir: Directory to extract files into.

    Returns:
        A PaperArchive with extracted files and detected file type.
    """
    if entry_name.endswith(".pdf"):
        return PaperArchive(arxiv_id, paper_dir, [], FileType.PDF, entry_name)
    if entry_name.endswith((".tar.gz", ".tgz", ".gz")):
        tex_files = extract_inner_tar_gz(raw_bytes, paper_dir)
        if tex_files:
            return PaperArchive(arxiv_id, paper_dir, tex_files, FileType.TEX, entry_name)
        tex_files, file_type = classify_gz(raw_bytes, arxiv_id, paper_dir)
        return PaperArchive(arxiv_id, paper_dir, tex_files, file_type, entry_name)
    if entry_name.endswith(".tex"):
        file_path = paper_dir / Path(entry_name).name
        file_path.write_bytes(raw_bytes)
        return PaperArchive(arxiv_id, paper_dir, [file_path], FileType.TEX, entry_name)
    return PaperArchive(arxiv_id, paper_dir, [], FileType.UNKNOWN, entry_name)


def extract_papers(archive_path: Path, output_dir: Path) -> Iterator[PaperArchive]:
    """Extract papers from an arXiv source tar archive.

    Reads the outer tar and yields one PaperArchive per entry. Files are
    extracted to output_dir/<arxiv_id>/. The caller manages the output_dir
    lifecycle (e.g. via tempfile.TemporaryDirectory(dir="/dev/shm")).

    Args:
        archive_path: Path to an arXiv source tar file.
        output_dir: Base directory for extracted paper files.

    Yields:
        PaperArchive for each entry in the archive.
    """
    with tarfile.open(archive_path, "r:") as tar:
        for entry in tar:
            path = entry.name
            if path.endswith("/") or not entry.isfile():
                continue

            arxiv_id = derive_arxiv_id(path)
            paper_dir = output_dir / arxiv_id
            paper_dir.mkdir(parents=True, exist_ok=True)

            try:
                fileobj = tar.extractfile(entry)
                if fileobj is None:
                    continue
                raw_bytes = fileobj.read()
            except (OSError, tarfile.TarError):
                log.warning("Failed to read entry %s", path, exc_info=True)
                continue

            try:
                yield classify_and_extract(raw_bytes, path, arxiv_id, paper_dir)
            except (OSError, tarfile.TarError, EOFError):
                log.warning("Failed to process entry %s", path, exc_info=True)
                continue


def count_tar_entries(archive_path: Path) -> int:
    """Count file entries in an uncompressed tar (header scan only).

    Uses the same filtering as extract_papers — skips directories and
    non-file entries. Cheap for uncompressed tars (reads 512-byte headers).

    Args:
        archive_path: Path to an uncompressed tar file.

    Returns:
        Number of file entries in the tar.
    """
    with tarfile.open(archive_path, "r:") as tar:
        return sum(1 for entry in tar if entry.isfile() and not entry.name.endswith("/"))


def extract_single_archive(archive_path: Path, output_dir: Path) -> PaperArchive:
    """Extract a single paper archive file to disk.

    Handles standalone archive files (.tar.gz, .gz, .tex, .pdf) — the same
    formats found as entries inside outer tars, but as individual files on disk.

    Args:
        archive_path: Path to a paper archive file.
        output_dir: Base directory for extracted paper files.

    Returns:
        A PaperArchive with extracted files and detected file type.
    """
    arxiv_id = derive_arxiv_id(archive_path.name)
    paper_dir = output_dir / arxiv_id
    paper_dir.mkdir(parents=True, exist_ok=True)
    raw_bytes = archive_path.read_bytes()
    return classify_and_extract(raw_bytes, archive_path.name, arxiv_id, paper_dir)


def find_main_file(files: list[Path]) -> int:
    r"""Find the index of the main TeX file among candidates.

    Reads each file to check for LaTeX markers. Priority:
        1. First file with both \\documentclass and \\begin{document}
        2. First file with \\documentclass
        3. First file with \\begin{document}
        4. Index 0 (first file)

    Args:
        files: List of paths to TeX files.

    Returns:
        Index of the main file in the input list.
    """
    docclass_indices: list[int] = []
    begindoc_indices: list[int] = []

    for i, file_path in enumerate(files):
        content = file_path.read_text(errors="replace")
        if "\\documentclass" in content:
            docclass_indices.append(i)
        if "\\begin{document}" in content:
            begindoc_indices.append(i)

    begindoc_set = set(begindoc_indices)
    for i in docclass_indices:
        if i in begindoc_set:
            return i

    if docclass_indices:
        return docclass_indices[0]

    if begindoc_indices:
        return begindoc_indices[0]

    return 0
