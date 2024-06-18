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
import sqlite3

def get_connection():
    conn = sqlite3.connect('wikipedia.db')
    conn.execute('PRAGMA journal_mode=WAL;')
    conn.execute('PRAGMA synchronous=NORMAL;')
    conn.execute('PRAGMA temp_store=MEMORY;')
    conn.execute('PRAGMA mmap_size=30000000000;')  # Adjust based on your system's memory
    return conn

def table_exists(cursor, table_name):
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    return cursor.fetchone() is not None

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
