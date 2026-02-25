# Wayback Gap Detector

A command-line tool that compares a local list of URLs against the [Wayback Machine](https://web.archive.org/) CDX API to identify URLs that have **not** been archived.

---

## Features

- Fetches archived URL data from the Wayback Machine CDX API with automatic pagination (`resumeKey`)
- Streams API results and caches them locally in [JSON Lines](https://jsonlines.org/) format to avoid redundant requests
- Normalizes URLs before comparison (strips trailing slashes, removes default ports, optionally unifies `http`/`https`, optionally sorts query parameters)
- Outputs a plain-text file of unarchived URLs
- Optionally outputs a separate file of archived URLs
- Supports resuming an interrupted fetch with `--resume-key`

---

## Requirements

- Python 3.9+
- See [requirements.txt](requirements.txt) for library dependencies

---

## Installation

```bash
# 1. Clone the repository
git clone https://github.com/c3974/wayback-gap-detector.git
cd wayback-gap-detector

# 2. Create and activate a virtual environment (recommended)
python -m venv venv
# Windows
venv\Scripts\activate
# macOS / Linux
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt
```

---

## Usage

```
python wbgap.py TARGET_URL [OPTIONS]
```

`TARGET_URL` is a wildcard URL passed to the CDX API (e.g. `https://example.com/blog/*`).

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--input FILE` | `urls.txt` | Path to the local URL list file |
| `--output FILE` | `not_archived.txt` | Output file for unarchived URLs |
| `--cache FILE` | `archived_cdx.jsonl` | CDX response cache file (JSONL) |
| `--output-archived [FILE]` | `archived.txt` | Also write archived URLs to a file |
| `--resume-key KEY` | *(none)* | Resume an interrupted CDX fetch |
| `--limit N` | `25000` | Max records per API request |
| `--ignore-protocol` | enabled | Treat `http` and `https` as equivalent |
| `--no-ignore-protocol` | — | Distinguish `http` from `https` |
| `--sort-query` | disabled | Sort query parameters alphabetically |
| `--no-sort-query` | — | Preserve original query parameter order |
| `-v`, `--verbose` | — | Enable DEBUG-level logging |

---

## Examples

### Basic usage

Fetch all CDX records for `https://example.com/blog/*` and write URLs not found in the archive to `not_archived.txt`:

```bash
python wbgap.py "https://example.com/blog/*" --input urls.txt
```

### Custom cache and output files

```bash
python wbgap.py "https://example.com/blog/*" \
  --input urls.txt \
  --cache my_cache.jsonl \
  --output missing.txt
```

### Collect archived URLs to a separate file

```bash
python wbgap.py "https://example.com/blog/*" \
  --input urls.txt \
  --output not_archived.txt \
  --output-archived archived.txt
```

### Resume an interrupted fetch

If a previous run failed mid-way, the tool prints a resume command. Use `--resume-key` to continue from where it left off:

```bash
python wbgap.py "https://example.com/blog/*" \
  --input urls.txt \
  --resume-key "20230601120000,https://example.com/blog/post-42"
```

---

## Input file format

`urls.txt` (or the file specified via `--input`) should contain one URL per line:

```
https://example.com/blog/post-1
https://example.com/blog/post-2
https://example.com/blog/post-3
```

Blank lines are ignored.

---

## Running the tests

```bash
python -m unittest discover -v
```

Or run the test file directly:

```bash
python test_wbgap.py
```

All tests are pure-Python unit tests with no external network calls (API calls are mocked).

---

## Project structure

```
wayback-gap-detector/
├── wbgap.py          # Main script
├── exceptions.py     # Custom exception classes
├── test_wbgap.py     # Unit tests
├── requirements.txt  # Python dependencies
└── README.md         # This file
```

---

## Contributing

Contributions are welcome!

1. Fork the repository and create a feature branch.
2. Make your changes and add or update tests as appropriate.
3. Ensure all tests pass: `python -m unittest discover -v`
4. Open a pull request with a clear description of what changed and why.

Please keep pull requests focused — one logical change per PR.

---

## License

This project is released under the [MIT License](https://opensource.org/licenses/MIT).
