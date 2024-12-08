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
import glob
import sqlite3
import os
from datetime import timedelta, datetime
from functools import wraps
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

from utils import get_connection, get_db_files, get_column_names, get_article_counts, get_article_counts_by_type


def time_execution(func):
    """Decorator to time the execution of a function."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()  # Start timing
        result = func(*args, **kwargs)  # Execute the function
        end_time = time.time()  # End timing
        
        # Calculate duration and format it
        duration = timedelta(seconds=end_time - start_time)
        formatted_duration = str(duration)
        
        print(f"Execution Time for {func.__name__}: {formatted_duration}")
        
        return result
    return wrapper

def search_article_by_id(article_id):
    """
    Searches for all information about an article by its ID.
    
    Parameters:
        article_id (int): The ID of the article to search for.
        db_files (list): List of database files to search in.
    
    Returns:
        dict: A dictionary containing all information about the article.
    """
    article_data = None
    db_files = get_db_files()

    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        c = conn.cursor()
        
        # Retrieve the article's main details
        query_article = '''
        SELECT article_id, title, is_redirect
        FROM articles
        WHERE article_id = ?
        '''
        c.execute(query_article, (article_id,))
        article = c.fetchone()

        if article:
            article_data = {
                'id': article[0],
                'title': article[1],
                'is_redirect': bool(article[2]),
                'sections': [],
                'categories': [],
                'redirects_to': None
            }

            # Retrieve sections for the article
            query_sections = '''
            SELECT section_title, section_content, wikitables
            FROM article_sections
            WHERE article_id = ?
            ORDER BY section_order
            '''
            c.execute(query_sections, (article_id,))
            sections = c.fetchall()
            article_data['sections'] = [
                {'title': section[0], 'content': section[1], 'wikitables': section[2]} for section in sections
            ]

            # Retrieve categories for the article
            query_categories = '''
            SELECT c.name
            FROM categories c
            JOIN article_categories ac ON c.category_id = ac.category_id
            WHERE ac.article_id = ?
            '''
            c.execute(query_categories, (article_id,))
            categories = c.fetchall()
            article_data['categories'] = [category[0] for category in categories]

            # Check if the article is a redirect and find its target
            if article_data['is_redirect']:
                redirect_query = '''
                SELECT article_id
                FROM articles
                WHERE title = ?
                '''
                c.execute(redirect_query, (article_data['title'],))
                redirect_targets = c.fetchall()
                article_data['redirects_to'] = [redirect[0] for redirect in redirect_targets]

        conn.close()

        # If article data was found in this database, no need to check further databases
        if article_data:
            break

    return article_data if article_data else f"Article with ID {article_id} not found."

@time_execution
def search_articles_by_title(title, limit=1000, introductionOnly=True):
    results = []

    # Get DB Files
    db_files = get_db_files()
        
    print(len(db_files))
    print(f"*"*20)
    print(f"Searching for Articles with string({title}):")

    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        c = conn.cursor()
        _text = f"Checking {db_file}, querying..."
        print(_text)
        
        # Adjusted query to match only standalone occurrences of the word "cat"
        query_articles = '''
        SELECT article_id, title FROM articles
        WHERE title = ? 
        OR title LIKE ? 
        OR title LIKE ? 
        OR title LIKE ?
        LIMIT ?
        '''

        # Execute the article query with specific patterns to match "cat" as a standalone word
        c.execute(query_articles, (title, f'{title} %', f'% {title}', f'% {title} %', limit))
        articles = c.fetchall()

        print(type(articles))
        _text = f"Found {len(articles)} articles..."
        print(_text)

        counter = 1
        for article_id, article_title in articles:
            _text = f"Gathering data for article #{counter}"
            print(_text)
            print(" "*len(_text))
            counter += 1
            article_data = {'id': article_id, 'title': article_title, 'sections': [], 'categories': [], 'redirects_to': None}


            # Query to get sections for the article
            if introductionOnly:
                query_sections = '''
                SELECT section_title, section_content, wikitables FROM article_sections
                WHERE article_id = ?
                ORDER BY id LIMIT 1
                '''
            else:
                query_sections = '''
                SELECT section_title, section_content, wikitables FROM article_sections
                WHERE article_id = ?
                '''
            
            # Execute the sections query
            c.execute(query_sections, (article_id,))
            sections = c.fetchall()
            article_data['sections'] = [{'title': section[0], 'content': section[1], 'wikitables': section[2]} for section in sections]

            # Query to get categories for the article
            query_categories = '''
            SELECT c.name FROM categories c
            INNER JOIN article_categories ac ON c.category_id = ac.category_id
            WHERE ac.article_id = ?
            '''

            # Execute the categories query
            c.execute(query_categories, (article_id,))
            categories = c.fetchall()
            article_data['categories'] = [category[0] for category in categories]

            # Check if the article is a redirect
            redirect_query = '''
            SELECT article_id FROM articles
            WHERE title = ?
            '''

            # Execute the redirect query
            c.execute(redirect_query, (article_title,))
            redirect_articles = c.fetchall()

            if redirect_articles:
                # Include immediate redirects (for disambiguations and such)
                if len(redirect_articles) > 1:
                    print(f"Design Flaw: Redirect for '{article_title}' resolves to multiple articles: {redirect_articles}")
                article_data['redirects_to'] = [redirect_article[0] for redirect_article in redirect_articles]
                results.append(article_data)
                break  # Break after the first article with a redirect
            else:
                # Else just add the article
                results.append(article_data)

        conn.close()

    return results

@time_execution
def search_articles_by_title_grouped_by_category(title, limit=1000, introductionOnly=False):
    results = {}
    db_files = get_db_files()
    
    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        c = conn.cursor()
        
        # Query to get articles by title and group them by category
        query_articles = '''
        SELECT a.article_id, a.title, c.name as category
        FROM articles a
        LEFT JOIN article_categories ac ON a.article_id = ac.article_id
        LEFT JOIN categories c ON ac.category_id = c.category_id
        WHERE a.title = ?
           OR a.title LIKE ? 
           OR a.title LIKE ? 
           OR a.title LIKE ?
        LIMIT ?
        '''
        
        # Execute the query with patterns to match standalone words
        c.execute(query_articles, (title, f'{title} %', f'% {title}', f'% {title} %', limit))
        articles = c.fetchall()
        
        for article_id, article_title, category in articles:
            if category not in results:
                results[category] = []
            article_data = {'id': article_id, 'title': article_title, 'sections': []}

            # Query to get sections if required
            if introductionOnly:
                query_sections = '''
                SELECT section_title, section_content, wikitables FROM article_sections
                WHERE article_id = ?
                ORDER BY id LIMIT 1
                '''
            else:
                query_sections = '''
                SELECT section_title, section_content, wikitables FROM article_sections
                WHERE article_id = ?
                '''

            c.execute(query_sections, (article_id,))
            sections = c.fetchall()
            article_data['sections'] = [{'title': section[0], 'content': section[1], 'wikitables': section[2]} for section in sections]

            results[category].append(article_data)

        conn.close()

    return results


@time_execution
def search_articles_by_text(text, limit=100):
    results = []
    db_files = get_db_files()

    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        c = conn.cursor()

        query = '''
        SELECT a.title, a.article_id, GROUP_CONCAT(c.name) as categories
        FROM articles a
        LEFT JOIN article_categories ac ON a.article_id = ac.article_id
        LEFT JOIN categories c ON ac.category_id = c.category_id
        WHERE a.article_id IN (
            SELECT article_id
            FROM article_sections
            WHERE section_content = ?
               OR section_content LIKE ?
               OR section_content LIKE ?
               OR section_content LIKE ?
        )
        GROUP BY a.article_id
        LIMIT ?
        '''
        
        # Execute the query with patterns for matching the standalone word
        c.execute(query, (text, f'{text} %', f'% {text}', f'% {text} %', limit))
        results.extend(c.fetchall())
        conn.close()

    return results

@time_execution
def search_articles_by_text_grouped_by_category(text, limit=100):
    results = {}
    db_files = get_db_files()

    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        c = conn.cursor()

        # Query to get articles by text and group by category
        query = '''
        SELECT a.title, a.article_id, c.name as category
        FROM articles a
        LEFT JOIN article_categories ac ON a.article_id = ac.article_id
        LEFT JOIN categories c ON ac.category_id = c.category_id
        WHERE a.article_id IN (
            SELECT article_id
            FROM article_sections
            WHERE section_content = ?
               OR section_content LIKE ?
               OR section_content LIKE ?
               OR section_content LIKE ?
        )
        GROUP BY a.article_id, c.name
        LIMIT ?
        '''
        
        # Execute the query with patterns to match standalone words
        c.execute(query, (text, f'{text} %', f'% {text}', f'% {text} %', limit))
        articles = c.fetchall()
        
        for article_title, article_id, category in articles:
            if category not in results:
                results[category] = []
            article_data = {'title': article_title, 'id': article_id}
            results[category].append(article_data)

        conn.close()

    return results

@time_execution
def search_articles_by_category(category, limit=100):
    results = []
    # Get DB Files
    db_files = get_db_files()
    print()
    for db_file in db_files:
        _ = f"Checking DB: {db_file}"
        print(_,end="\r")
        conn = sqlite3.connect(db_file)
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
        results.extend(c.fetchall())
        conn.close()
        print(" "*len(_),end="\r")
    

    return results

from collections import defaultdict

@time_execution
def group_articles_by_category(articles):
    """Groups articles by category and filters out categories with only one article."""
    # Dictionary to group articles by category
    category_groups = defaultdict(list)

    # Iterate through each article and assign it to its categories
    for article in articles:
        for category in article['categories']:
            category_groups[category].append(article)

    # Filter out categories with only one article
    filtered_category_groups = {category: articles for category, articles in category_groups.items() if len(articles) > 1}

    return filtered_category_groups


def general_search(query, limit=100):
    title_results = search_articles_by_title(query, limit)
    text_results = search_articles_by_text(query, limit)
    category_results = search_articles_by_category(query, limit)
    
    return {
        "title_results": [{'title': iter_title_results[0], 'page_id': iter_title_results[1]} for iter_title_results in title_results],
        "text_results": [{'title': iter_text_results[0], 'page_id': iter_text_results[1]} for iter_text_results in text_results],
        "category_results": [{'title': iter_cat_results[0], 'page_id': iter_cat_results[1]} for iter_cat_results in category_results],
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Search Wikipedia articles.")
    parser.add_argument("query", help="Search query for Wikipedia articles")
    args = parser.parse_args()

    query = args.query
    results1 = search_articles_by_title(query)
    # results2 = search_articles_by_text(query)
    # results3 = search_articles_by_text_old(query)
    # results4 = search_articles_by_category(query)
    print(results1)
    # results5 = search_articles_by_title_grouped_by_category(query)
    # results = search_article_by_id(query)

    # res = group_articles_by_category(results6)

    # import pprint
    # # pprint.pprint(results)
    # print(results)
    # print(get_column_names('articles'))
    # print(get_column_names('article_sections'))
    # print(get_column_names('article_categories'))
    # print(get_column_names('categories'))
    # print()


    # pprint.pprint(res)
    # Usage
    # counts_by_type = get_article_counts_by_type()

    # print("Total articles by type:")
    # for article_type, count in counts_by_type["total_articles_by_type"].items():
    #     print(f"{article_type}: {count}")

    # print("\nArticles with at least one category by type:")
    # for article_type, count in counts_by_type["articles_with_category_by_type"].items():
    #     print(f"{article_type}: {count}")