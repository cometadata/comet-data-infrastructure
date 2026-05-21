from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import datetime


@dataclass
class ZenodoFile:
    """A file entry in a Zenodo record.

    Attributes:
        link: Download link for the file.
        file_hash: MD5 checksum string (md5:...).
        file_name: File name.
        file_type: File type/extension.
    """

    link: str | None = None
    file_hash: str | None = None
    file_name: str | None = None
    file_type: str | None = None


@dataclass
class ZenodoRecord:
    """A Zenodo record with publication date and file list.

    Attributes:
        publication_date: Publication date of the record.
        files: List of files attached to this record.
    """

    publication_date: datetime.date
    files: list[ZenodoFile] = field(default_factory=list)
