# Global TF-IDF

word_List_global = set()
allowed_sections = [0]


DB_files = [] 
assert len(DB_files) > 0 # TODO MARKER
# Glob names of DB files

for DB in DB_files:
    article_sections = [] 
    assert len(article_sections) > 0 # TODO MARKER
    # Get article sections from this DB splinter

    for article_section in article_sections:
        if article_section.section_order in allowed_sections:
            # For now only allow the intro, but eventually to (title, title and intro, title and all sections)

            article_id = article_section.article_id 
            # Pull article id from entry data
            
            article_data = lookup(article_id)
            article_title, article_type = article_data.title, article_data.article_type
            # Use foreign key to access article data, get article type (text, categories, tables, tables and categories)
            
            if article_type == "text": 
                # Only consider text articles

                word_List_global += get_unique_words(article_section)
                # Get all unique words from the article, add them to the set
                
                word_List_global += get_unique_words(article_title)
                # Get all unique words from the article's title, add them to the set

# All words are got by now, use indexes to minimize footprint

word_List_global = list( word_List_global )
word_List_global.sort()
word_List_global = tuple(word_List_global) # Will not change anymore

# Setting the order to alphabetical

TF_Global = {
    # article_id: { word_index: augmented_freq, ... }
}

IDF_Global = {
    # word_index: document_freq, ...
}

# TF and IDF based on: https://en.wikipedia.org/wiki/Tf-idf#Term_frequency

for DB in DB_files:
    article_sections = [] 
    assert len(article_sections) > 0 # TODO MARKER
    # Get article sections from this DB splinter

    for article_section in article_sections:
        if article_section.section_order in allowed_sections:
            # For now only allow the intro, but eventually to (title, title and intro, title and all sections)

            article_id = article_section.article_id 
            # Pull article id from entry data
            
            article_data = lookup(article_id)
            article_title, article_type = article_data.title, article_data.article_type
            # Use foreign key to access article data, get article type (text, categories, tables, tables and categories)
            
            if article_type == "text": 
                # Only consider text articles

                TF_Local = {}
                # Create Local TF object
                
                article_words = article_section.split_into_words() + article_title.split_into_words()

                for word in article_words:
                    word_index = word_List_global.index(word) if word in word_List_global else None
                    # Get the index of this word in the word_List_global, it should always register so throw an error if not

                    if not word_index:
                        raise(ValueError(f"Word: {word} not found in word_List_global"))
                    
                    if TF_Local.get(word_index):
                        TF_Local[word_index] += 1
                    else:
                        TF_Local[word_index] = 1
                # At this point, TF_Local is fully populated
                
                # Update it for augmented frequency, using double normalization 0.5: see wikipedia link
                highest_frequency = max(list(TF_Local.values()))
                _K = 0.5

                _ = TF_Local.copy()
                for iterWordIndex, iterWordFrequency in _.items():
                    TF_Local[iterWordIndex] = _K + (_K * (iterWordFrequency/highest_frequency) ) 
                    # Apply double norm

                # At this point the TF_Local is ready
                # Update TF_Global

                TF_Global[article_id] = TF_Local

                # Now Populate IDF with article frequecies
                for word in article_words:
                    word_index = word_List_global.index(word)
                    if IDF_Global.get(word_index):
                        IDF_Global[word_index] += 1
                    else:
                        IDF_Global[word_index] = 1