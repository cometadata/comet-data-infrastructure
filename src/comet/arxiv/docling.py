"""Convert arXiv papers from LaTeX to Markdown using docling."""

import functools
import logging
from dataclasses import dataclass

from docling.datamodel.backend_options import LatexBackendOptions
from docling.datamodel.base_models import ConversionStatus, InputFormat
from docling.document_converter import DocumentConverter, LatexFormatOption

from comet.arxiv.archive import FileType, PaperArchive, find_main_file

log = logging.getLogger(__name__)

DOCLING_LATEX_LOGGER = "docling.backend.latex.backend"


class FallbackDetector(logging.Handler):
    """Captures docling latex backend warnings to detect fallback to raw text."""

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.detected = False

    def emit(self, record: logging.LogRecord) -> None:
        if "fallback" in record.getMessage().lower():
            self.detected = True


@dataclass
class ConversionResult:
    """Result of converting a single arXiv paper to Markdown."""

    arxiv_id: str
    file_type: FileType
    markdown: str | None
    status: str


@functools.lru_cache
def make_converter(parse_timeout: float = 60.0) -> DocumentConverter:
    """Create a docling DocumentConverter configured for LaTeX.

    Cached so the same timeout always returns the same instance.

    Args:
        parse_timeout: Timeout in seconds for the LaTeX parser.

    Returns:
        A reusable DocumentConverter instance.
    """
    latex_options = LatexBackendOptions(parse_timeout=parse_timeout)
    return DocumentConverter(
        format_options={
            InputFormat.LATEX: LatexFormatOption(backend_options=latex_options)
        }
    )


def convert_paper(paper: PaperArchive, converter: DocumentConverter) -> ConversionResult:
    """Convert a single PaperArchive to Markdown using docling.

    Non-TeX papers are returned with status "skipped". TeX papers are
    converted via the provided DocumentConverter. The caller is responsible
    for extracting papers to disk first (via extract_papers from arxiv.py).

    Args:
        paper: A PaperArchive with files already extracted to disk.
        converter: A DocumentConverter instance (from make_converter).

    Returns:
        ConversionResult with arxiv_id, file_type, markdown, and status.
    """
    if paper.file_type != FileType.TEX:
        return ConversionResult(
            arxiv_id=paper.arxiv_id,
            file_type=paper.file_type,
            markdown=None,
            status="skipped",
        )

    if not paper.tex_files:
        return ConversionResult(
            arxiv_id=paper.arxiv_id,
            file_type=paper.file_type,
            markdown=None,
            status="failure",
        )

    main_idx = find_main_file(paper.tex_files)
    main_path = paper.tex_files[main_idx]

    detector = FallbackDetector()
    latex_logger = logging.getLogger(DOCLING_LATEX_LOGGER)
    latex_logger.addHandler(detector)
    try:
        result = converter.convert(main_path, raises_on_error=False)
    except Exception:
        log.warning("Docling conversion raised for %s", paper.arxiv_id, exc_info=True)
        return ConversionResult(
            arxiv_id=paper.arxiv_id,
            file_type=paper.file_type,
            markdown=None,
            status="failure",
        )
    finally:
        latex_logger.removeHandler(detector)

    if detector.detected:
        status = "fallback"
    elif result.status == ConversionStatus.SUCCESS:
        status = "success"
    elif result.status == ConversionStatus.PARTIAL_SUCCESS:
        status = "partial"
    else:
        log.warning(
            "Docling conversion failed for %s: %s",
            paper.arxiv_id,
            [e.error_message for e in result.errors],
        )
        status = "failure"

    markdown = None
    if status in ("success", "partial", "fallback"):
        try:
            markdown = result.document.export_to_markdown()
        except Exception:
            log.warning("Markdown export failed for %s", paper.arxiv_id, exc_info=True)
            status = "failure"

    return ConversionResult(
        arxiv_id=paper.arxiv_id,
        file_type=paper.file_type,
        markdown=markdown,
        status=status,
    )
