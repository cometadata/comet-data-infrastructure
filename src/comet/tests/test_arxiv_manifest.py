"""Tests for comet.arxiv_manifest."""

import textwrap
from datetime import datetime

import pytest

from comet.arxiv.manifest import ManifestEntry, parse_manifest

SAMPLE_XML = textwrap.dedent("""\
    <?xml version='1.0' standalone='yes'?>
    <arXivSRC>
      <file>
        <content_md5sum>aaa111</content_md5sum>
        <filename>src/arXiv_src_0001_001.tar</filename>
        <first_item>astro-ph0001001</first_item>
        <last_item>quant-ph0001119</last_item>
        <md5sum>bbb222</md5sum>
        <num_items>2364</num_items>
        <seq_num>1</seq_num>
        <size>225605507</size>
        <timestamp>2010-12-23 00:13:59</timestamp>
        <yymm>0001</yymm>
      </file>
      <file>
        <content_md5sum>ccc333</content_md5sum>
        <filename>src/arXiv_src_0002_001.tar</filename>
        <first_item>astro-ph0002001</first_item>
        <last_item>quant-ph0002094</last_item>
        <md5sum>ddd444</md5sum>
        <num_items>2365</num_items>
        <seq_num>1</seq_num>
        <size>227036528</size>
        <timestamp>2010-12-23 00:18:09</timestamp>
        <yymm>0002</yymm>
      </file>
      <timestamp>Sat Mar  7 05:47:43 2026</timestamp>
    </arXivSRC>
""")

EMPTY_XML = textwrap.dedent("""\
    <?xml version='1.0' standalone='yes'?>
    <arXivPDF>
      <timestamp>Sat Mar  7 05:47:43 2026</timestamp>
    </arXivPDF>
""")


class TestParseManifest:
    def test_parses_entries_with_correct_attributes(self, tmp_path):
        xml_file = tmp_path / "manifest.xml"
        xml_file.write_text(SAMPLE_XML)

        entries = parse_manifest(xml_file)

        assert len(entries) == 2
        assert all(isinstance(e, ManifestEntry) for e in entries)
        assert entries[0].content_md5sum == "aaa111"
        assert entries[0].filename == "src/arXiv_src_0001_001.tar"
        assert entries[1].content_md5sum == "ccc333"

    def test_typed_fields_are_correct(self, tmp_path):
        xml_file = tmp_path / "manifest.xml"
        xml_file.write_text(SAMPLE_XML)

        entries = parse_manifest(xml_file)

        assert entries[0].num_items == 2364
        assert entries[0].seq_num == 1
        assert entries[0].size == 225605507
        assert entries[0].timestamp == datetime(2010, 12, 23, 0, 13, 59)

    def test_empty_manifest_returns_empty_list(self, tmp_path):
        xml_file = tmp_path / "empty.xml"
        xml_file.write_text(EMPTY_XML)

        assert parse_manifest(xml_file) == []

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            parse_manifest(tmp_path / "nonexistent.xml")
