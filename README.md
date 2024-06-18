# Wikipedia Importer

This project provides a set of scripts to import and search Wikipedia articles from a Wikipedia XML dump file into an SQLite database.

## Features

- Count total pages in the Wikipedia dump
- Import articles, categories, and sections into an SQLite database
- Resume import from the last checkpoint in case of interruptions
- Search articles by title, text, or category

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

    Search for articles:

    ```bash
    python scripts/wiki_searcher.py "search query"
    ```