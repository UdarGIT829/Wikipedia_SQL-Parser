import sqlite3
import glob
import wiki_searcher

def get_db_files():
    return glob.glob('wikipedia_*.db')

def print_table_names():
    db_files = get_db_files()
    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = c.fetchall()
        print(f"Tables in {db_file}:")
        for table in tables:
            print(table[0])
        conn.close()

def check_categories():
    db_files = get_db_files()
    total_categories = 0
    invalid_categories = 0
    skip_list = set()
    found_categories = []
    matching_articles_global = {}

    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        c = conn.cursor()
        
        query = "SELECT name FROM categories;"
        c.execute(query)
        categories = c.fetchall()
        
        for category in categories:
            total_categories += 1
            category_name = category[0]
            if category_name in skip_list:
                continue
            if category_name.replace("Category:","") != category_name[len("Category:"):]:
                invalid_categories += 1
                print(f"Invalid category: {category_name}")
                
                print(f"Articles with this category:")
                matching_articles = []
                matching_articles = wiki_searcher.search_articles_by_category(category_name)
                if len(matching_articles) > 0:
                    if matching_articles_global.get(category_name) == None:
                        matching_articles_global[category_name] = []
                
                for m_art in matching_articles:
                    print(f"\t{m_art}")
                    matching_articles_global[category_name].append(m_art)
                print()
                found_categories.append(category_name)
                
            
            skip_list.add(category_name)
        print("_=_"*6)
        conn.close()
    

    if total_categories > 0:
        invalid_percentage = (invalid_categories / total_categories) * 100
        print(f"Checked {total_categories} categories, {invalid_percentage:.2f}% are invalid.")
    return found_categories, matching_articles_global

def main():
    # print_table_names()
    found_categories, matching_articles_global = check_categories()
    import csv
    with open("testing_output.csv","w",newline="") as fi:
        writer = csv.writer(fi)
        for row in found_categories:
            writer.writerow([row.replace("\n","  ")])

    import json
    with open("testing_output.json","w") as fi:
        fi.write(json.dumps(matching_articles_global))
    found_categories, matching_articles_global = check_categories()
    import json
    with open("testing_output.json","r") as fi:
        fi.write(json.dumps(matching_articles_global))


if __name__ == "__main__":
    main()
