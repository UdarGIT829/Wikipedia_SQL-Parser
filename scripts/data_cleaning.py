import sqlite3
import glob

def generate_cleaning_report():
    db_files = glob.glob('wikipedia_*.db')
    changes = []

    counter_db = 1

    # Save changes to a file for user review
    with open('cleaning_report.txt', 'w') as report_file:
        pass

    for db_file in db_files:
        changes = []
        print(f"Parsing DB# {counter_db}")
        counter_db += 1
        conn = sqlite3.connect(db_file)
        c = conn.cursor()

        query_sections = '''
        SELECT id, article_id, section_content FROM article_sections
        '''

        c.execute(query_sections)
        sections = c.fetchall()

        counter = 1
        _tot = len(sections)
        for section_id, article_id, section_content in sections:
            _text = f"Parsing section {counter}/{_tot} | Changes found: {len(changes)}"
            print(_text, end="\r")
            print(" "*len(_text))

            counter += 1

            stripped_content = section_content.strip()
            if section_content != stripped_content:
                changes.append((db_file, section_id, article_id, len(section_content)- len(stripped_content)))
                # print((db_file, section_id, article_id, len(section_content)- len(stripped_content)))
                # print(stripped_content[:50])
        print()

        conn.close()

        # Save changes to a file for user review
        with open('cleaning_report.txt', 'a') as report_file:
            for change in changes:
                db_file, section_id, article_id, stripped_difference = change
                report_file.write(f"DB File: {db_file}, Article ID: {article_id}, Section ID: {section_id}\n")
                report_file.write(f"Difference: '{stripped_difference}'\n")

    print("Cleaning report generated: cleaning_report.txt")

def apply_cleaning_changes():
    with open('cleaning_report.txt', 'r') as report_file:
        changes = report_file.readlines()

    db_file_changes = {}
    current_db_file = None

    for line in changes:
        if line.startswith("DB File:"):
            current_db_file = line.split(",")[0].split(":")[1].strip()
            if current_db_file not in db_file_changes:
                db_file_changes[current_db_file] = []
        elif line.startswith("Original:"):
            original = line.split("'", 1)[1].rsplit("'", 1)[0]
        elif line.startswith("Stripped:"):
            stripped = line.split("'", 1)[1].rsplit("'", 1)[0]
        elif line.startswith("Section ID:"):
            section_id = int(line.split(":")[1].strip())
            article_id = int(line.split(",")[1].split(":")[1].strip())
            db_file_changes[current_db_file].append((section_id, stripped))

    for db_file, changes in db_file_changes.items():
        conn = sqlite3.connect(db_file)
        c = conn.cursor()

        for section_id, stripped_content in changes:
            update_query = '''
            UPDATE article_sections
            SET section_content = ?
            WHERE id = ?
            '''
            c.execute(update_query, (stripped_content, section_id))
        
        conn.commit()
        conn.close()

    print("All approved changes have been applied.")

# To generate the report
generate_cleaning_report()

# After reviewing the report, to apply the changes
# apply_cleaning_changes()
