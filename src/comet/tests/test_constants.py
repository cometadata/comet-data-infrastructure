from __future__ import annotations

import pytest

from comet.constants import Enrichment, Source


class TestRetentionValidation:
    @pytest.mark.parametrize("releases_to_keep", [0, -1])
    def test_rejects_retention_counts_below_one(self, releases_to_keep):
        with pytest.raises(ValueError, match="releases_to_keep"):
            Source("ror", releases_to_keep)
        with pytest.raises(ValueError, match="releases_to_keep"):
            Enrichment(Source("ror", 1), "funders", releases_to_keep)
