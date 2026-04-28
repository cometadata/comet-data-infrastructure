"""Tests for arxiv tar archive parsing."""

import io
import tarfile
from pathlib import Path

import pytest

from comet.arxiv.archive import (
    FileType,
    count_tar_entries,
    derive_arxiv_id,
    detect_file_type,
    extract_papers,
    extract_single_archive,
    find_main_file,
)

from .conftest import (
    APPENDIX_CONTENT,
    BBL_CONTENT,
    TEX_CONTENT,
    gzip_bytes,
    make_inner_tar_gz,
    make_outer_tar,
)


class TestExtractPapers:
    def test_extracts_tex_from_inner_tar_gz(self, tmp_path: Path):
        inner = make_inner_tar_gz({
            "main.tex": TEX_CONTENT,
            "appendix.tex": APPENDIX_CONTENT,
            "refs.bbl": BBL_CONTENT,
            "figure.png": b"\x89PNG\r\n",
        })
        tar_path = make_outer_tar(tmp_path / "src.tar", {"2401/2401.00001.tar.gz": inner})
        output = tmp_path / "out"
        output.mkdir()

        papers = list(extract_papers(tar_path, output))

        assert len(papers) == 1
        paper = papers[0]
        assert paper.arxiv_id == "2401.00001"
        assert paper.file_type == FileType.TEX
        tex_names = sorted(p.name for p in paper.tex_files)
        assert tex_names == ["appendix.tex", "main.tex", "refs.bbl"]
        assert (paper.path / "figure.png").exists()

    def test_extracts_single_gz_compressed_tex(self, tmp_path: Path):
        compressed = gzip_bytes(TEX_CONTENT)
        tar_path = make_outer_tar(tmp_path / "src.tar", {"2401/2401.00002.gz": compressed})
        output = tmp_path / "out"
        output.mkdir()

        papers = list(extract_papers(tar_path, output))

        assert len(papers) == 1
        paper = papers[0]
        assert paper.arxiv_id == "2401.00002"
        assert paper.file_type == FileType.TEX
        assert len(paper.tex_files) == 1
        assert paper.tex_files[0].read_text() == TEX_CONTENT.decode()

    def test_reads_bare_tex_entry(self, tmp_path: Path):
        tar_path = make_outer_tar(tmp_path / "src.tar", {"2401/2401.00003.tex": TEX_CONTENT})
        output = tmp_path / "out"
        output.mkdir()

        papers = list(extract_papers(tar_path, output))

        assert len(papers) == 1
        paper = papers[0]
        assert paper.arxiv_id == "2401.00003"
        assert paper.file_type == FileType.TEX
        assert len(paper.tex_files) == 1
        assert paper.tex_files[0].read_bytes() == TEX_CONTENT

    def test_identifies_pdf_entry(self, tmp_path: Path):
        pdf_bytes = b"%PDF-1.4 fake pdf content"
        tar_path = make_outer_tar(tmp_path / "src.tar", {"2401/2401.00004.pdf": pdf_bytes})
        output = tmp_path / "out"
        output.mkdir()

        papers = list(extract_papers(tar_path, output))

        assert len(papers) == 1
        assert papers[0].file_type == FileType.PDF
        assert papers[0].tex_files == []

    @pytest.mark.parametrize(
        "data, expected",
        [
            (b"%PDF-1.4 content", FileType.PDF),
            (b"%!PS-Adobe-3.0", FileType.POSTSCRIPT),
            (b"<html><body>hi</body></html>", FileType.HTML),
            (b"<!DOCTYPE html><html></html>", FileType.HTML),
            (TEX_CONTENT, FileType.TEX),
            (b"", FileType.UNKNOWN),
        ],
        ids=["pdf", "postscript", "html_tag", "html_doctype", "tex", "empty"],
    )
    def test_detects_file_type(self, data: bytes, expected: FileType):
        assert detect_file_type(data) == expected

    @pytest.mark.parametrize(
        "path, expected",
        [
            ("2401.00001.tar.gz", "2401.00001"),
            ("2401.00001.gz", "2401.00001"),
            ("2401.00001.tex", "2401.00001"),
            ("2401.00001.pdf", "2401.00001"),
            ("2401/2401.00001.tar.gz", "2401.00001"),
            ("hep-ph/0001001.gz", "0001001"),
        ],
    )
    def test_derives_arxiv_id_from_path(self, path: str, expected: str):
        assert derive_arxiv_id(path) == expected

    def test_skips_directory_entries(self, tmp_path: Path):
        tar_path = tmp_path / "src.tar"
        with tarfile.open(tar_path, "w:") as tar:
            # Add a directory entry
            dir_info = tarfile.TarInfo(name="2401/")
            dir_info.type = tarfile.DIRTYPE
            tar.addfile(dir_info)
            # Add a real file
            info = tarfile.TarInfo(name="2401/2401.00001.tex")
            info.size = len(TEX_CONTENT)
            tar.addfile(info, io.BytesIO(TEX_CONTENT))
        output = tmp_path / "out"
        output.mkdir()

        papers = list(extract_papers(tar_path, output))

        assert len(papers) == 1
        assert papers[0].arxiv_id == "2401.00001"


class TestCountTarEntries:
    def test_counts_file_entries(self, tmp_path: Path):
        inner1 = make_inner_tar_gz({"main.tex": TEX_CONTENT})
        inner2 = make_inner_tar_gz({"paper.tex": TEX_CONTENT})
        tar_path = make_outer_tar(
            tmp_path / "test.tar",
            {"2401/2401.00001.tar.gz": inner1, "2401/2401.00002.tar.gz": inner2},
        )

        assert count_tar_entries(tar_path) == 2

    def test_skips_directory_entries(self, tmp_path: Path):
        tar_path = tmp_path / "src.tar"
        with tarfile.open(tar_path, "w:") as tar:
            dir_info = tarfile.TarInfo(name="2401/")
            dir_info.type = tarfile.DIRTYPE
            tar.addfile(dir_info)
            info = tarfile.TarInfo(name="2401/2401.00001.tex")
            info.size = len(TEX_CONTENT)
            tar.addfile(info, io.BytesIO(TEX_CONTENT))

        assert count_tar_entries(tar_path) == 1


class TestExtractSingleArchive:
    def test_extracts_tar_gz_archive(self, tmp_path: Path):
        inner = make_inner_tar_gz({
            "main.tex": TEX_CONTENT,
            "appendix.tex": APPENDIX_CONTENT,
        })
        archive = tmp_path / "2401.00001.tar.gz"
        archive.write_bytes(inner)
        output = tmp_path / "out"
        output.mkdir()

        paper = extract_single_archive(archive, output)

        assert paper.arxiv_id == "2401.00001"
        assert paper.file_type == FileType.TEX
        assert len(paper.tex_files) == 2

    def test_extracts_gz_compressed_tex(self, tmp_path: Path):
        archive = tmp_path / "2401.00002.gz"
        archive.write_bytes(gzip_bytes(TEX_CONTENT))
        output = tmp_path / "out"
        output.mkdir()

        paper = extract_single_archive(archive, output)

        assert paper.arxiv_id == "2401.00002"
        assert paper.file_type == FileType.TEX
        assert len(paper.tex_files) == 1

    def test_classifies_pdf(self, tmp_path: Path):
        archive = tmp_path / "2401.00003.pdf"
        archive.write_bytes(b"%PDF-1.4 fake pdf")
        output = tmp_path / "out"
        output.mkdir()

        paper = extract_single_archive(archive, output)

        assert paper.arxiv_id == "2401.00003"
        assert paper.file_type == FileType.PDF
        assert paper.tex_files == []


class TestFindMainFile:
    def test_prefers_file_with_both_markers(self, tmp_path: Path):
        (tmp_path / "appendix.tex").write_text("\\section{Appendix}\n")
        (tmp_path / "main.tex").write_text("\\documentclass{article}\n\\begin{document}\nHello\n\\end{document}\n")
        (tmp_path / "intro.tex").write_text("\\section{Introduction}\n")

        files = [tmp_path / "appendix.tex", tmp_path / "main.tex", tmp_path / "intro.tex"]
        assert find_main_file(files) == 1

    def test_falls_back_to_documentclass_only(self, tmp_path: Path):
        (tmp_path / "preamble.tex").write_text("\\documentclass{article}\n")
        (tmp_path / "body.tex").write_text("\\begin{document}\nHello\n\\end{document}\n")

        files = [tmp_path / "preamble.tex", tmp_path / "body.tex"]
        assert find_main_file(files) == 0

    def test_falls_back_to_begin_document_only(self, tmp_path: Path):
        (tmp_path / "appendix.tex").write_text("\\section{Appendix}\n")
        (tmp_path / "body.tex").write_text("\\begin{document}\nHello\n\\end{document}\n")

        files = [tmp_path / "appendix.tex", tmp_path / "body.tex"]
        assert find_main_file(files) == 1

    def test_falls_back_to_first_file(self, tmp_path: Path):
        (tmp_path / "a.tex").write_text("No markers here.\n")
        (tmp_path / "b.tex").write_text("Also no markers.\n")

        files = [tmp_path / "a.tex", tmp_path / "b.tex"]
        assert find_main_file(files) == 0
