import tkinter as tk
from tkinter import ttk
from wiki_searcher import search_articles_by_title, search_articles_by_text

class DataViewer:
    def __init__(self, root):
        self.root = root
        self.root.title("Wikipedia Article Viewer")

        # Configure the root window to be resizable
        self.root.rowconfigure(0, weight=1)
        self.root.columnconfigure(0, weight=1)

        # Frame for table with scrollbar
        self.frame = tk.Frame(self.root)
        self.frame.grid(row=0, column=0, sticky="nsew")

        # Scrollbar
        self.scrollbar = ttk.Scrollbar(self.frame, orient=tk.VERTICAL)
        self.scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # Table setup
        self.columns = ("Title", "Text", "Categories")
        self.table = ttk.Treeview(self.frame, columns=self.columns, show="headings", yscrollcommand=self.scrollbar.set)
        for col in self.columns:
            self.table.heading(col, text=col)
            self.table.column(col, width=200, anchor=tk.CENTER)
        self.table.pack(fill=tk.BOTH, expand=True)

        self.scrollbar.config(command=self.table.yview)

        # Search boxes and button
        self.search_frame = tk.Frame(self.root)
        self.search_frame.grid(row=1, column=0, sticky="ew", padx=10, pady=5)
        
        self.root.columnconfigure(0, weight=1)
        self.search_frame.columnconfigure(1, weight=1)
        self.search_frame.columnconfigure(3, weight=1)

        tk.Label(self.search_frame, text="Title:").grid(row=0, column=0, sticky="w")
        self.title_search = tk.Entry(self.search_frame)
        self.title_search.grid(row=0, column=1, sticky="ew", padx=5)

        tk.Label(self.search_frame, text="Text:").grid(row=0, column=2, sticky="w")
        self.text_search = tk.Entry(self.search_frame)
        self.text_search.grid(row=0, column=3, sticky="ew", padx=5)

        self.search_button = tk.Button(self.search_frame, text="Search", command=self.on_search)
        self.search_button.grid(row=0, column=4, padx=5)

    def search_articles(self, title_query, text_query):
        results = []
        if title_query:
            results.extend(search_articles_by_title(title_query))
        if text_query:
            results.extend(search_articles_by_text(text_query))
        return results

    def update_table(self, results):
        for row in self.table.get_children():
            self.table.delete(row)
        for result in results:
            title, article_id, categories = result
            self.table.insert("", "end", values=(title, article_id, categories))

    def on_search(self):
        title_query = self.title_search.get()
        text_query = self.text_search.get()
        results = self.search_articles(title_query, text_query)
        self.update_table(results)

if __name__ == "__main__":
    root = tk.Tk()
    app = DataViewer(root)
    root.mainloop()
