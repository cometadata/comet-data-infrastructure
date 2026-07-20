"""Shared test helpers for arXiv archive tests."""

import gzip
import io
from pathlib import Path
import tarfile

TEX_CONTENT = rb"\documentclass{article}" + b"\n\\begin{document}\nHello world.\n\\end{document}\n"
APPENDIX_CONTENT = rb"\section{Appendix}" + b"\nExtra details.\n"
BBL_CONTENT = rb"\begin{thebibliography}{1}" + b"\n\\bibitem{ref1} Author, Title.\n\\end{thebibliography}\n"


def gzip_bytes(data: bytes) -> bytes:
    """Gzip-compress raw bytes."""
    return gzip.compress(data)


def make_inner_tar_gz(files: dict[str, bytes]) -> bytes:
    """Create an in-memory .tar.gz containing the given files."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for name, content in files.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))
    return buf.getvalue()


def make_outer_tar(path: Path, entries: dict[str, bytes]) -> Path:
    """Create an outer tar file containing the given name -> content entries."""
    with tarfile.open(path, "w:") as tar:
        for name, content in entries.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))
    return path
