from __future__ import annotations

from dataclasses import dataclass, field
import datetime


@dataclass(frozen=True)
class DatasetRelease:
    """Canonical return type for all dataset version detectors.

    Attributes:
        release_date: Provider release date.
        file_name: File to download, if applicable.
        download_url: Direct download URL, if applicable.
        file_hash: MD5 checksum (md5:...) for the file, if applicable.
        run_id: Run that ingested this release, if known. None from detectors (assigned at
            ingest); populated when a persisted record is converted back to a DatasetRelease.
        metadata: Arbitrary extra key/value pairs for dataset-specific data.
    """

    release_date: datetime.date
    file_name: str | None = None
    download_url: str | None = None
    file_hash: str | None = None
    run_id: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Return a JSON-serializable dict (release_date as an ISO date string)."""
        return {
            "release_date": self.release_date.isoformat(),
            "file_name": self.file_name,
            "download_url": self.download_url,
            "file_hash": self.file_hash,
            "run_id": self.run_id,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: dict) -> DatasetRelease:
        """Rebuild a DatasetRelease from a dict produced by to_dict."""
        return cls(
            release_date=datetime.date.fromisoformat(data["release_date"]),
            file_name=data.get("file_name"),
            download_url=data.get("download_url"),
            file_hash=data.get("file_hash"),
            run_id=data.get("run_id"),
            metadata=data.get("metadata") or {},
        )
