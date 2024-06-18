import mwxml
import mwparserfromhell
import sqlite3
import sys
import time
import re
from utils import get_connection, remove_specific_tags


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

def parse_dump(file_path):
    conn = get_connection()
    c = conn.cursor()
    
    dump = mwxml.Dump.from_file(open(file_path, 'rb'))
    count = 0
    start_time = time.time()
    prev_time = start_time
    prev_count = 0

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
                    c.execute('INSERT INTO articles (title, is_redirect, type) VALUES (?, ?, ?)', 
                              (title, is_redirect, type_))
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
                    
                    print(f'\rImported {count} pages so far, {pages_per_second:.2f} pages per second',end='')
                    
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
                    print(f'\nPage {page.id}: {title}')
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

# Path to your extracted XML file
file_path = r'data\example_wikipedia-articles.xml'

# Import pages
parse_dump(file_path)

# Validate checkpoint
validate_checkpoint(file_path)

print('\nValidation completed.')

