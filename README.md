# Wikipedia Importer

This project provides a set of scripts to import and search Wikipedia articles from a Wikipedia XML dump file into an SQLite database.

## Features

- Count total pages in the Wikipedia dump
- Import articles, categories, and sections into an SQLite database
- Resume import from the last checkpoint in case of interruptions
- Search articles by title, text, or category
- Optional Dockerfile for importer

## Installation

1. Clone the repository:

```bash
git clone https://github.com/yourusername/wikipedia-importer.git
cd wikipedia-importer
```

2. Install dependencies:

    ```bash
    pip install -r requirements.txt
    ```

3. Usage:

    Set up the database:
    ```bash
    python scripts/starter.py
    ```

    Count pages in the dump file:

    ```bash
    python scripts/page_counter.py path/to/your/wikipedia-dump.xml
    ```

    Import pages into the database:

    ```bash
    python scripts/wiki_parser.py path/to/your/wikipedia-dump.xml
    ```

    This step currently needs to be executed multiple times due to what I suspect to be a memory leak related to a requirement: mwxml. The library wasn't built to injest such large files. I suggest running the Docker-Compose and playing with the 250,000 article limiter in `wiki_parser.py`.

    Search for articles:

    ```bash
    python scripts/wiki_searcher.py "search query"
    ```