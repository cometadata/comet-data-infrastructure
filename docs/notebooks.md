# Preparing data for notebooks

Three steps to make `notebooks/arXiv.ipynb` runnable.

## 1. Place source files in `data/sources/`

- `arxiv-metadata-oai-snapshot.json`
- `arXiv_pdf_manifest.xml`
- `arXiv_src_manifest.xml`

## 2. Convert manifest XMLs to Parquet

```bash
uv run comet arxiv manifest-parquet data/sources/arXiv_src_manifest.xml data/output
uv run comet arxiv manifest-parquet data/sources/arXiv_pdf_manifest.xml data/output
```

## 3. Symlink the extract results directory

```bash
ln -s /path/to/results-<release-date> data/output/
```
