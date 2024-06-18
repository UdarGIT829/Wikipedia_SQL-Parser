"""
MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import mwxml
import mwparserfromhell
import sqlite3
import sys
import time
import re
import json

import argparse

from utils import get_connection, remove_specific_tags
from page_counter import counter_
from starter import table_exists


def save_checkpoint(last_page_id, conn, c):
    c.execute('INSERT OR REPLACE INTO checkpoints (id, last_page_id) VALUES (1, ?)', (last_page_id,))
    conn.commit()

def load_checkpoint():
    conn = sqlite3.connect('wikipedia.db')
    c = conn.cursor()
    c.execute('SELECT last_page_id FROM checkpoints WHERE id = 1')
    result = c.fetchone()
    conn.close()
    return result[0] if result else None

def get_category_id(name, c):
    c.execute('SELECT category_id FROM categories WHERE name = ?', (name,))
    row = c.fetchone()
    if row:
        return row[0]
    else:
        c.execute('INSERT INTO categories (name) VALUES (?)', (name,))
        return c.lastrowid


def parse_sections(parsed_content, is_redirect):
    sections = []
    if is_redirect:
        clean_content, json_tables_infoboxes = remove_specific_tags(parsed_content.strip_code())
        sections.append(("redirect", clean_content, json_tables_infoboxes))
    else:
        current_section_title = "Introduction"
        current_section_content = []

        for node in parsed_content.nodes:
            if node.__class__.__name__ == 'Heading':
                if current_section_content:
                    clean_content, json_tables_infoboxes = remove_specific_tags('\n'.join(current_section_content))
                    sections.append((current_section_title, clean_content, json_tables_infoboxes))
                    current_section_content = []
                current_section_title = node.title.strip_code().strip()
            else:
                current_section_content.append(str(node))

        if current_section_content:
            clean_content, json_tables_infoboxes = remove_specific_tags('\n'.join(current_section_content))
            sections.append((current_section_title, clean_content, json_tables_infoboxes))

    return sections

def parse_dump(file_path, total_pages_file=None):
    conn = get_connection()
    c = conn.cursor()
    
    dump = mwxml.Dump.from_file(open(file_path, 'rb'))
    count = 0
    start_time = time.time()
    prev_time = start_time

    # Load total pages from JSON file
    if total_pages_file:
        json_path = total_pages_file
        with open(json_path, 'r') as f:
            total_pages = json.load(f)["total_pages"]
    else:
        total_pages = 0

    last_page_id = load_checkpoint()
    skip = last_page_id is not None

    for page in dump:
        if skip:
            if page.id <= last_page_id:
                print(f"Jumping from {page.id} to {last_page_id}\r", end="")
                continue
            skip = False

        latest_revision = None
        for revision in page:
            latest_revision = revision

        if latest_revision:
            title = page.title
            content = latest_revision.text
            if content:
                parsed_content = mwparserfromhell.parse(content)
                
                # Detect categories
                categories = [str(link.title) for link in parsed_content.filter_wikilinks() if link.title.startswith("Category:")]
                
                # Detect redirects
                is_redirect = content.strip().lower().startswith("#redirect")

                # Determine type
                if is_redirect and categories:
                    type_ = 'redirect_and_categories'
                elif is_redirect:
                    type_ = 'redirect'
                elif categories:
                    type_ = 'categories'
                else:
                    type_ = 'text'

                try:
                    c.execute('INSERT INTO articles (article_id, title, is_redirect, type) VALUES (?, ?, ?, ?)', 
                              (page.id, title, is_redirect, type_))
                    article_id = c.lastrowid

                    # Insert sections
                    sections = parse_sections(parsed_content, is_redirect)
                    for section_title, section_content, wikitables in sections:
                        c.execute('INSERT INTO article_sections (article_id, section_title, section_content, wikitables) VALUES (?, ?, ?, ?)', 
                                  (article_id, section_title, section_content, wikitables))

                    for category in categories:
                        category_id = get_category_id(category, c)
                        c.execute('INSERT INTO article_categories (article_id, category_id) VALUES (?, ?)', 
                                  (article_id, category_id))
                except UnicodeEncodeError as e:
                    print(f"UnicodeEncodeError: {e} - Skipping page ID {page.id} with title {title}")
                    continue
                count += 1
                # Print the count with carriage return and commit every 100 pages
                if count % 100 == 0:
                    current_time = time.time()
                    elapsed_time = current_time - start_time
                    pages_per_second = count / elapsed_time
                    

                    percentage_complete = (count / total_pages) * 100
                    print(f'\rImported {count} pages so far, {pages_per_second:.2f} pages per second, {percentage_complete:.2f}% complete')
                    
                    # Update previous time and count for the next derivative calculation
                    prev_time = current_time
                    prev_count = count
                    
                    save_checkpoint(page.id, conn, c)
                    conn.commit()
                
                # Vacuum the database every 100,000 entries
                if count % 100000 == 0:
                    print(f"\nVacuuming the database after {count} entries...")
                    conn.execute('VACUUM;')
                    print("Vacuum completed.")
                
                # Print a page title and the first 500 characters of its content every 1000 pages
                if count % 1000 == 0:
                    print(f'\nPage {count}: {title}')
                    print(''.join(parsed_content[:500]))

    save_checkpoint(page.id, conn, c)
    conn.commit()
    conn.close()

def validate_checkpoint(file_path):
    """
    Validate and debug that the importer starts on the correct article after an unexpected termination.
    """
    last_page_id = load_checkpoint()
    print(f"Last imported page ID: {last_page_id}")
    conn = get_connection()
    c = conn.cursor()
    
    dump = mwxml.Dump.from_file(open(file_path, 'rb'))
    for page in dump:
        if page.id > last_page_id:
            latest_revision = None
            for revision in page:
                latest_revision = revision
            if latest_revision:
                title = page.title
                content = latest_revision.text
                print(f"Next article to be processed: Page ID = {page.id}, Title = {title}")
                print(f"Content (first 500 characters): {content[:500]}")
                break

    conn.close()

if __name__ == "__main__":

    # Connect to the existing SQLite database
    conn = get_connection()
    c = conn.cursor()

    # Check and create the articles table with FTS5 if it doesn't exist
    if not table_exists(c, 'articles'):
        c.execute('''
        CREATE TABLE articles (
            article_id INTEGER PRIMARY KEY,
            title TEXT,
            is_redirect INTEGER,
            type TEXT
        )
        ''')

    # Check and create the categories table if it doesn't exist
    if not table_exists(c, 'categories'):
        c.execute('''
        CREATE TABLE categories (
            category_id INTEGER PRIMARY KEY,
            name TEXT UNIQUE
        )
        ''')

    # Check and create the article_categories table if it doesn't exist
    if not table_exists(c, 'article_categories'):
        c.execute('''
        CREATE TABLE article_categories (
            article_id INTEGER,
            category_id INTEGER,
            FOREIGN KEY(article_id) REFERENCES articles(article_id),
            FOREIGN KEY(category_id) REFERENCES categories(category_id)
        )
        ''')

    # Check and create the checkpoints table if it doesn't exist
    if not table_exists(c, 'checkpoints'):
        c.execute('''
        CREATE TABLE checkpoints (
            id INTEGER PRIMARY KEY,
            last_page_id INTEGER
        )
        ''')

    # Check and create the article_sections table if it doesn't exist
    if not table_exists(c, 'article_sections'):
        c.execute('''
        CREATE TABLE article_sections (
            id INTEGER PRIMARY KEY,
            article_id INTEGER,
            section_title TEXT,
            section_content TEXT,
            wikitables TEXT,
            FOREIGN KEY(article_id) REFERENCES articles(article_id)
        )
        ''')

    conn.commit()
    conn.close()


    parser = argparse.ArgumentParser(description="Process Wikipedia dump file.")
    parser.add_argument("file_path", help="Path to the extracted XML file")
    args = parser.parse_args()

    file_path = args.file_path
    output_path = file_path.replace(".xml", "_pageCount.json")

    counter_(input_file_path=file_path, output_json_path=output_path)

    # Import pages
    parse_dump(file_path=file_path, total_pages_file=output_path)

    # Validate checkpoint
    validate_checkpoint(file_path)

    print('\nValidation completed.')