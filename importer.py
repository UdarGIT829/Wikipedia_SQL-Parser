import mwxml
import time

def count_pages(file_path):
    dump = mwxml.Dump.from_file(open(file_path, 'rb'))
    count = 0
    start_time = time.time()
    
    for page in dump:
        count += 1
        
        if count % 10000 == 0:
            elapsed_time = time.time() - start_time
            pages_per_second = count / elapsed_time
            print(f'Counted {count} pages so far, {pages_per_second:.2f} pages per second\r',end="")

    print()

    return count

# Path to your extracted XML file
file_path = 'data\enwiki-20240501-pages-meta-current.xml'

# Count total pages
print('Counting total pages...')
total_pages = count_pages(file_path)
print(f'Total pages in the dump: {total_pages}')
