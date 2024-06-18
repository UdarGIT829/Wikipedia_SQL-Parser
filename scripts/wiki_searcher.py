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
import argparse

from utils import get_connection

# Search functions
def search_articles_by_title(title, limit=100):
    conn = get_connection()
    c = conn.cursor()
    
    query = '''
    SELECT title, article_id
    FROM articles
    WHERE title LIKE ?
    LIMIT ?
    '''
    
    c.execute(query, (f'%{title}%', limit))
    results = c.fetchall()
    conn.close()
    return results

def search_articles_by_text(text, limit=100):
    conn = get_connection()
    c = conn.cursor()
    
    query = '''
    SELECT title, article_id
    FROM articles
    WHERE article_id IN (
        SELECT article_id
        FROM article_sections
        WHERE section_content LIKE ?
    )
    LIMIT ?
    '''
    
    c.execute(query, (f'%{text}%', limit))
    results = c.fetchall()
    conn.close()
    return results

def search_articles_by_category(category, limit=100):
    conn = get_connection()
    c = conn.cursor()
    
    query = '''
    SELECT a.title, a.article_id
    FROM articles a
    JOIN article_categories ac ON a.article_id = ac.article_id
    JOIN categories c ON ac.category_id = c.category_id
    WHERE c.name LIKE ?
    LIMIT ?
    '''
    
    c.execute(query, (f'%{category}%', limit))
    results = c.fetchall()
    conn.close()
    return results

def general_search(query, limit=100):
    title_results = search_articles_by_title(query, limit)
    text_results = search_articles_by_text(query, limit)
    category_results = search_articles_by_category(query, limit)
    
    return {
        "title_results": [{'title':iter_title_results[0], 'page_id':iter_title_results[1]} for iter_title_results in title_results],
        "text_results": [{'title':iter_text_results[0], 'page_id':iter_text_results[1]} for iter_text_results in text_results],
        "category_results": [{'title':iter_cat_results[0], 'page_id':iter_cat_results[1]} for iter_cat_results in category_results],
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Search Wikipedia articles.")
    parser.add_argument("query", help="Search query for Wikipedia articles")
    args = parser.parse_args()

    query = args.query
    results = general_search(query)
    print(results)