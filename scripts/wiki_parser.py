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
import os
import argparse
import numpy as np
import gc
import psutil
import os.path
import csv

from utils import remove_specific_tags
from page_counter import counter_
from starter import table_exists

DB_DIR = "db_files/"
def get_highest_db_index():
    db_index = 1
    while os.path.exists(DB_DIR+f'wikipedia_{db_index}.db'):
        db_index += 1
    return db_index - 1

def get_connection(db_index):
    db_path = DB_DIR+ f'wikipedia_{db_index}.db'
    conn = sqlite3.connect(db_path)
    conn.execute('PRAGMA journal_mode=WAL;')
    conn.execute('PRAGMA synchronous=NORMAL;')
    conn.execute('PRAGMA temp_store=MEMORY;')
    conn.execute('PRAGMA mmap_size=30000000000;')  # Adjust based on your system's memory

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
            section_order INTEGER,
            section_title TEXT,
            section_content TEXT,
            wikitables TEXT,
            embedding BLOB,
            FOREIGN KEY(article_id) REFERENCES articles(article_id)
        )
        ''')

    conn.commit()
    return conn, db_path

def load_checkpoint():
    db_index = get_highest_db_index()
    conn, _ = get_connection(db_index=db_index)
    c = conn.cursor()

    # Check if the checkpoints table exists
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='checkpoints';")
    if c.fetchone() is None:
        conn.close()
        return None

    c.execute('SELECT last_page_id FROM checkpoints WHERE id = 1')
    result = c.fetchone()
    conn.close()
    return result[0] if result else None

def get_db_size(db_path):
    return os.path.getsize(db_path)

def save_checkpoint(last_page_id, conn, c):
    c.execute('INSERT OR REPLACE INTO checkpoints (id, last_page_id) VALUES (1, ?)', (last_page_id,))
    conn.commit()

def get_category_id(name, c):
    c.execute('SELECT category_id FROM categories WHERE name = ?', (name,))
    row = c.fetchone()
    if row:
        return row[0]
    else:
        c.execute('INSERT INTO categories (name) VALUES (?)', (name,))
        return c.lastrowid

def validate_last_entry(file_path):
    db_index = get_highest_db_index()
    conn, _ = get_connection(db_index=db_index)
    c = conn.cursor()

    # Get the last entry in the articles table
    c.execute('SELECT article_id, title FROM articles ORDER BY article_id DESC LIMIT 1')
    last_entry = c.fetchone()
    conn.close()

    if last_entry:
        last_article_id, last_title = last_entry
        print(f"Last entry in articles table: ID = {last_article_id}, Title = {last_title}")

        dump = mwxml.Dump.from_file(open(file_path, 'rb'))
        for page in dump:
            if page.id > last_article_id:
                next_article_id = page.id
                next_title = page.title
                print(f"Next article to be processed: Page ID = {next_article_id}, Title = {next_title}")
                return last_article_id < next_article_id
    else:
        print("No entries found in the articles table.")
        return False

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

def parse_dump(file_path, total_pages_file=None, db_splinter_size=10):
    db_index = get_highest_db_index()
    if db_index < 1:
        db_index = 1
    conn, db_path = get_connection(db_index)
    c = conn.cursor()
    
    # Create import_log.csv
    if not os.path.isfile("db_files/import_log.csv"):
        with open("db_files/import_log.csv","w", newline="") as fi:
            writer = csv.writer(fi)
            writer.writerow(
                ["count", "pages_per_second", "percentage_complete", "memory in use"]
            )

    with open(file_path, 'rb') as dump_file:
        dump = mwxml.Dump.from_file(dump_file)
        count = 0
        start_time = time.time()
        prev_time = start_time
        _printing_text = ""

        # Load total pages from JSON file
        if total_pages_file:
            json_path = total_pages_file
            with open(json_path, 'r') as f:
                total_pages = json.load(f)["total_pages"]
        else:
            total_pages = 0

        last_page_id = load_checkpoint()
        skip = last_page_id is not None
        comparison = 0.0

        for page in dump:
            if skip:
                if page.id <= last_page_id:
                    if last_page_id - page.id > 1000:
                        if int(last_page_id - page.id) % 1000 == 0.0:
                            print(f"Jumping from {page.id} to {last_page_id}\r", end="")
                    else:
                        print(f"Jumping from {page.id} to {last_page_id}\r", end="")
                    continue
                skip = False


                # Perform the validation check
                if validate_last_entry(file_path):
                    print("Validation successful: The last entry in the articles table matches the one after the skip loop.")
                else:
                    print("Validation failed: The last entry in the articles table does not match the one after the skip loop.")
                    raise(Exception("Validation Error"))
                
                print()
                print(f"Commencing intake at Page {page.id}")
                print()

            
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

                    # Check database size and switch to a new one if it exceeds the limit
                    comparison = get_db_size(db_path) / float(db_splinter_size * 1024 * 1024 * 1024)

                    while comparison > 1:  # 10 GB limit
                        conn.commit()

                        print(f"\nVacuuming the database after {count} entries...")
                        conn.execute('VACUUM;')
                        print("Vacuum completed.")
                        
                        conn.close()
                        
                        db_index += 1
                        conn, db_path = get_connection(db_index)
                        c = conn.cursor()
                        comparison = get_db_size(db_path) / float(db_splinter_size * 1024 * 1024 * 1024)

                    try:
                        c.execute('INSERT INTO articles (article_id, title, is_redirect, type) VALUES (?, ?, ?, ?)', 
                                (page.id, title, is_redirect, type_))
                        article_id = c.lastrowid

                        # Insert sections
                        sections = parse_sections(parsed_content, is_redirect)
                        
                        # Create blank embedding variable with identity matrix, process this later
                        blank_embedding = np.identity(n=3, dtype=np.float32)
                        embedding_blob = blank_embedding.tobytes()
                        
                        for section_order in range(len(sections)):
                            section_title, section_content, wikitables = sections[section_order]
                            c.execute('INSERT INTO article_sections (article_id, section_order, section_title, section_content, wikitables, embedding) VALUES (?, ?, ?, ?, ?, ?)', 
                                    (article_id, section_order, section_title, section_content, wikitables, embedding_blob))

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
                        
                        # Monitor memory usage
                        process = psutil.Process(os.getpid())
                        memory_info = process.memory_info()
                        mem_mb = memory_info.rss / (1024 ** 2)

                        percentage_complete = (page.id / total_pages) * 100
                        print(" "*len(_printing_text),
                            end="\r")

                        _printing_text = f'Imported {count} pages so far, {pages_per_second:.2f} pages per second, {percentage_complete:.2f}% complete; Memory usage: {mem_mb:.2f} MB'
                        
                        print(_printing_text,
                            end="\r")
                        
                        # Update previous time and count for the next derivative calculation
                        prev_time = current_time
                        prev_count = count
                        
                        save_checkpoint(page.id, conn, c)
                        conn.commit()
                        



                    # Print a page title and the first 500 characters of its content every 1000 pages
                    if count % 1000 == 0:
                        print(f'\nPage {count}: {title}')
                        print(''.join(parsed_content[:500]))

                    if count % 10000 == 0:
                        with open("db_files/import_log.csv","a",newline="") as fi:
                            writer = csv.writer(fi)
                            writer.writerow(
                                [count, pages_per_second, percentage_complete, psutil.virtual_memory().percent]
                            )
                        print("Running GC cleaning.")
                        gc.collect()
                    
                    if count % 400000 == 0:
                        print("Early return to reset Memory usage!")
                        return False
                    
    save_checkpoint(page.id, conn, c)
    conn.commit()
    conn.close()
    gc.collect()
    return True

def validate_checkpoint(file_path):
    """
    Validate and debug that the importer starts on the correct article after an unexpected termination.
    """
    last_page_id = load_checkpoint()
    print(f"Last imported page ID: {last_page_id}")
    conn, _ = get_connection(db_index=1)
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

    parser = argparse.ArgumentParser(description="Process Wikipedia dump file.")
    parser.add_argument("file_path", help="Path to the extracted XML file")
    parser.add_argument("db_splinter_size", help="Size of each of the Database splits (Total est: 120Gb)")
    
    args = parser.parse_args()

    file_path = args.file_path
    output_path = file_path.replace(".xml", "_pageCount.json")


    # The code is meant to exit after 200,000 entries (or whatever the last count % conditional is)
    counter_(input_file_path=file_path, output_json_path=output_path)

    # Import pages
    isDone = parse_dump(file_path=file_path, total_pages_file=output_path, db_splinter_size=int(args.db_splinter_size))

    print(f"IS DONE: {isDone}")

    # Validate checkpoint
    validate_checkpoint(file_path)

    print('\nValidation completed.')