"""Parse arXiv manifest XML files."""

import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class ManifestEntry:
    """A single file entry from an arXiv manifest."""

    content_md5sum: str
    filename: str
    first_item: str
    last_item: str
    md5sum: str
    num_items: int
    seq_num: int
    size: int
    timestamp: datetime
    yymm: str


def parse_file_element(elem: ET.Element) -> ManifestEntry:
    """Extract a ManifestEntry from a <file> XML element."""
    return ManifestEntry(
        content_md5sum=elem.findtext("content_md5sum", "").strip(),
        filename=elem.findtext("filename", "").strip(),
        first_item=elem.findtext("first_item", "").strip(),
        last_item=elem.findtext("last_item", "").strip(),
        md5sum=elem.findtext("md5sum", "").strip(),
        num_items=int(elem.findtext("num_items", "0").strip()),
        seq_num=int(elem.findtext("seq_num", "0").strip()),
        size=int(elem.findtext("size", "0").strip()),
        timestamp=datetime.strptime(
            elem.findtext("timestamp", "").strip(), "%Y-%m-%d %H:%M:%S"
        ),
        yymm=elem.findtext("yymm", "").strip(),
    )


def parse_manifest(input_file: Path) -> list[ManifestEntry]:
    """Parse an arXiv manifest XML file into a list of ManifestEntry objects.

    Args:
        input_file: Path to an arXiv manifest XML file.

    Returns:
        List of ManifestEntry objects, one per <file> element.
    """
    entries: list[ManifestEntry] = []
    for _event, elem in ET.iterparse(input_file, events=("end",)):
        if elem.tag != "file":
            continue
        entries.append(parse_file_element(elem))
        elem.clear()
    return entries
