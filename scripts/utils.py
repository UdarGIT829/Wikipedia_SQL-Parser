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
import re
import json
import os
import glob
from collections import defaultdict

def get_connection():
    conn = sqlite3.connect('wikipedia.db')
    conn.execute('PRAGMA journal_mode=WAL;')
    conn.execute('PRAGMA synchronous=NORMAL;')
    conn.execute('PRAGMA temp_store=MEMORY;')
    conn.execute('PRAGMA mmap_size=30000000000;')  # Adjust based on your system's memory
    return conn

def get_db_files():
    # Get the absolute path of the current script
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    # Move up a level and point to db_files folder
    db_files_path = os.path.join(current_script_dir, '..', 'db_files', 'wikipedia_*.db')

    db_files = glob.glob(db_files_path)
    return db_files

def remove_specific_tags(text):
    """
    Remove specific tags like {{...}}, <!-- ... -->, [[File:...]], <ref ... /> and <ref></ref> from the text.

    Also calls strip() to remove leading and trailing spaces and newlines (this is common and a waste of storage)
    """
    # Remove {{...}} tags
    clean_text = re.sub(r'\{\{.*?\}\}', '', text)
    # Remove <!-- ... --> comments
    clean_text = re.sub(r'<!--.*?-->', '', clean_text)
    # Remove [[File:...]] tags handling nested brackets
    def remove_file_tags(text):
        while True:
            start = text.find('[[File:')
            if start == -1:
                break
            end = start
            nested = 1
            while nested > 0 and end < len(text) - 1:
                end += 1
                if text[end:end+2] == '[[':
                    nested += 1
                elif text[end:end+2] == ']]':
                    nested -= 1
            text = text[:start] + text[end+2:]
        return text

    clean_text = remove_file_tags(clean_text)
    # Remove <ref ... /> and <ref>...</ref> tags
    clean_text = re.sub(r'<ref\b[^>]*\/>', '', clean_text)
    clean_text = re.sub(r'<ref\b[^>]*>.*?<\/ref>', '', clean_text)
    # Extract and remove wikitables
    json_wikitables, clean_text = extract_wikitables_and_infoboxes(clean_text)
    clean_text = clean_text.strip()
    return clean_text, json_wikitables

def extract_wikitables_and_infoboxes(text):
    """
    Extract wikitables and infoboxes from the text and convert them to a JSON string.
    Remove wikitables and infoboxes from the text.
    """
    # Pattern to match wikitables
    wikitable_pattern = re.compile(r'\{\|.*?\|\}', re.DOTALL)
    wikitables = wikitable_pattern.findall(text)
    json_wikitables = []

    for wikitable in wikitables:
        table_dict = {"headers": [], "rows": []}
        table_lines = wikitable.strip().split('\n')
        headers = []
        rows = []
        current_row = []
        in_header = False
        
        for line in table_lines:
            if line.startswith('{|'):
                continue
            elif line.startswith('!'):
                if not headers:
                    headers = [header.strip() for header in re.split(r'!!|\|\|', line.strip()[1:])]
                    table_dict["headers"] = headers
                in_header = True
            elif line.startswith('|-'):
                if current_row:
                    rows.append(current_row)
                current_row = []
                in_header = False
            elif line.startswith('|'):
                if in_header:
                    headers = [header.strip() for header in re.split(r'!!|\|\|', line.strip()[1:])]
                    table_dict["headers"] = headers
                else:
                    cells = [cell.strip() for cell in re.split(r'\|\|', line.strip()[1:])]
                    current_row.extend(cells)
        
        if current_row:
            rows.append(current_row)
        
        for row in rows:
            row_dict = {}
            for i, cell in enumerate(row):
                header = headers[i] if i < len(headers) else f"Column_{i+1}"
                row_dict[header] = cell
            table_dict["rows"].append(row_dict)
        
        json_wikitables.append(table_dict)

    clean_text = re.sub(wikitable_pattern, '', text)

    # Pattern to match infoboxes
    infobox_pattern = re.compile(r'\{\{Infobox.*?\}\}', re.DOTALL)
    infoboxes = infobox_pattern.findall(text)
    json_infoboxes = []

    for infobox in infoboxes:
        infobox_dict = {}
        lines = infobox.strip().split('\n')
        for line in lines:
            if line.startswith('{{Infobox'):
                continue
            elif '=' in line:
                key, value = line.split('=', 1)
                infobox_dict[key.strip()] = value.strip()
        json_infoboxes.append(infobox_dict)

    clean_text = re.sub(infobox_pattern, '', clean_text)

    combined_json = {
        "wikitables": json_wikitables,
        "infoboxes": json_infoboxes
    }

    return json.dumps(combined_json), clean_text

def get_categories_for_article(article_id):
    conn = get_connection()
    c = conn.cursor()
    
    query = '''
    SELECT c.name
    FROM categories c
    JOIN article_categories ac ON c.category_id = ac.category_id
    WHERE ac.article_id = ?
    '''
    
    c.execute(query, (article_id,))
    categories = [row[0] for row in c.fetchall()]
    
    conn.close()
    return categories

def get_column_names(table_name):
    db_file = get_db_files()[0]
    """Retrieve and print column names for a specified table."""
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    
    # Use PRAGMA to get the column information
    c.execute(f"PRAGMA table_info({table_name});")
    columns = c.fetchall()
    
    # Print column names and types
    print(f"Columns in table '{table_name}':")
    for column in columns:
        print(f"Name: {column[1]}, Type: {column[2]}")
    
    conn.close()
def get_article_counts():
    """
    Retrieves the total number of articles and the total number of articles with a category.
    
    Parameters:
        db_files (list): List of database files to search in.
    
    Returns:
        dict: A dictionary with the total article count and the count of articles with a category.
    """
    db_files = get_db_files()

    total_articles = 0
    articles_with_category = 0

    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        c = conn.cursor()

        # Query to get the total number of articles
        query_total_articles = 'SELECT COUNT(*) FROM articles'
        c.execute(query_total_articles)
        total_articles += c.fetchone()[0]

        # Query to get the total number of articles with a category
        query_articles_with_category = '''
        SELECT COUNT(DISTINCT a.article_id)
        FROM articles a
        JOIN article_categories ac ON a.article_id = ac.article_id
        '''
        c.execute(query_articles_with_category)
        articles_with_category += c.fetchone()[0]

        conn.close()

    return {
        "total_articles": total_articles,
        "articles_with_category": articles_with_category
    }


def get_article_counts_by_type():
    """
    Retrieves the total number of articles and the total number of articles with a category,
    grouped by type.
    
    Parameters:
        db_files (list): List of database files to search in.
    
    Returns:
        dict: A dictionary with the total article count and the count of articles with a category, grouped by type.
    """
    db_files = get_db_files()
    total_articles_by_type = defaultdict(int)
    articles_with_category_by_type = defaultdict(int)

    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        c = conn.cursor()

        # Query to get the total number of articles grouped by type
        query_total_articles = '''
        SELECT type, COUNT(*) 
        FROM articles
        GROUP BY type
        '''
        c.execute(query_total_articles)
        for row in c.fetchall():
            article_type, count = row
            total_articles_by_type[article_type] += count

        # Query to get the number of articles with a category grouped by type
        query_articles_with_category = '''
        SELECT a.type, COUNT(DISTINCT a.article_id)
        FROM articles a
        JOIN article_categories ac ON a.article_id = ac.article_id
        GROUP BY a.type
        '''
        c.execute(query_articles_with_category)
        for row in c.fetchall():
            article_type, count = row
            articles_with_category_by_type[article_type] += count

        conn.close()

    # Combine the results in a structured dictionary
    result = {
        "total_articles_by_type": dict(total_articles_by_type),
        "articles_with_category_by_type": dict(articles_with_category_by_type)
    }

    return result



# Example usage
if __name__ == "__main__":
    article_id = 12345  # Replace with the actual article ID
    categories = get_categories_for_article(article_id)
    print(f"Categories for article {article_id}: {categories}")
