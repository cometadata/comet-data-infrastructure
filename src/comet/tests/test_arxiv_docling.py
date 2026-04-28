"""Tests for arXiv docling conversion."""

import logging
from pathlib import Path

import pytest

from comet.arxiv.archive import FileType, PaperArchive
from comet.arxiv.docling import DOCLING_LATEX_LOGGER, convert_paper, make_converter

TEX_CONTENT = "\\documentclass{article}\n\\begin{document}\nHello world.\n\\end{document}\n"


@pytest.fixture
def converter():
    return make_converter(parse_timeout=30.0)


class TestConvertPaper:
    def test_converts_tex_paper_to_markdown(self, tmp_path: Path, converter):
        tex_file = tmp_path / "main.tex"
        tex_file.write_text(TEX_CONTENT)
        paper = PaperArchive(
            arxiv_id="2401.00001",
            path=tmp_path,
            tex_files=[tex_file],
            file_type=FileType.TEX,
            entry_name="2401/2401.00001.gz",
        )

        result = convert_paper(paper, converter)

        assert result.status == "success"
        assert result.arxiv_id == "2401.00001"
        assert result.file_type == FileType.TEX
        assert result.markdown is not None
        assert "Hello world" in result.markdown

    @pytest.mark.parametrize(
        "file_type",
        [FileType.PDF, FileType.UNKNOWN],
        ids=["pdf", "unknown"],
    )
    def test_skips_non_tex_papers(self, tmp_path: Path, converter, file_type: FileType):
        paper = PaperArchive(
            arxiv_id="2401.00002",
            path=tmp_path,
            tex_files=[],
            file_type=file_type,
            entry_name="2401/2401.00002.pdf",
        )

        result = convert_paper(paper, converter)

        assert result.status == "skipped"
        assert result.markdown is None

    def test_handles_empty_tex_files(self, tmp_path: Path, converter):
        paper = PaperArchive(
            arxiv_id="2401.00003",
            path=tmp_path,
            tex_files=[],
            file_type=FileType.TEX,
            entry_name="2401/2401.00003.gz",
        )

        result = convert_paper(paper, converter)

        assert result.status == "failure"
        assert result.markdown is None

    def test_converts_multi_file_paper(self, tmp_path: Path, converter):
        intro = tmp_path / "intro.tex"
        intro.write_text("\\section{Introduction}\nThis is the introduction.\n")
        main = tmp_path / "main.tex"
        main.write_text(
            "\\documentclass{article}\n"
            "\\begin{document}\n"
            "\\input{intro}\n"
            "\\end{document}\n"
        )

        paper = PaperArchive(
            arxiv_id="2401.00004",
            path=tmp_path,
            tex_files=[main, intro],
            file_type=FileType.TEX,
            entry_name="2401/2401.00004.tar.gz",
        )

        result = convert_paper(paper, converter)

        assert result.status == "success"
        assert result.markdown is not None
        assert "Introduction" in result.markdown


    def test_detects_fallback_to_raw_text(self, tmp_path: Path, converter):
        tex_file = tmp_path / "main.tex"
        tex_file.write_text(TEX_CONTENT)
        paper = PaperArchive(
            arxiv_id="2401.00099",
            path=tmp_path,
            tex_files=[tex_file],
            file_type=FileType.TEX,
            entry_name="2401/2401.00099.gz",
        )

        # Simulate docling emitting a fallback warning during conversion
        latex_logger = logging.getLogger(DOCLING_LATEX_LOGGER)
        original_convert = converter.convert

        def fake_convert(path, **kwargs):
            latex_logger.warning("LaTeX parsing failed: test error. Using fallback text extraction.")
            return original_convert(path, **kwargs)

        converter.convert = fake_convert
        try:
            result = convert_paper(paper, converter)
        finally:
            converter.convert = original_convert

        assert result.status == "fallback"
        assert result.markdown is not None


class TestMakeConverter:
    def test_returns_cached_instance(self):
        a = make_converter(parse_timeout=30.0)
        b = make_converter(parse_timeout=30.0)
        assert a is b
