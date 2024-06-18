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
import time
import json
import os

def count_pages(file_path):
    dump = mwxml.Dump.from_file(open(file_path, 'rb'))
    count = 0
    start_time = time.time()
    
    for page in dump:
        count += 1
        
        if count % 25000 == 0:
            elapsed_time = time.time() - start_time
            pages_per_second = count / elapsed_time
            print(f'Counted {count} pages so far, {pages_per_second:.2f} pages per second\r', end="")

    return count

def counter_(input_file_path, output_json_path):
    # Path to your extracted XML file
    file_path = input_file_path
    json_path = output_json_path

    # Check if the JSON file exists
    if not os.path.exists(json_path):
        # Count total pages
        print('Counting total pages...')
        total_pages = count_pages(file_path)
        print()
        print(f'Total pages in the dump: {total_pages}')

        # Save to JSON file
        with open(json_path, 'w') as f:
            json.dump({"total_pages": total_pages}, f)
    else:
        # Load total pages from JSON file
        with open(json_path, 'r') as f:
            total_pages = json.load(f)["total_pages"]

    print(f'Total pages expected: {total_pages}')
