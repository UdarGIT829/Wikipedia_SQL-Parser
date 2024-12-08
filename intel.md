# Abstract

Using Wikipedia pages dumped into an SQL database, compare any text with the entirety of Wikipedia with semantic search. 


### Concept System

The concept system will use BERT/etc. Embedding to generate a list of top matching Wikipedia categories (with top matching articles) for ANY input string.

- **Input Data Processing:**
    
    - Accept input data (e.g., a string or query).
    - Use a semantic search model (e.g., BERT, SBERT) to vectorize the input data.
- Initial IDF:

	- Compute IDF across all searchable sections (Title + "Introduction" section)
		- This will globally reduce the effect of words that do not carry meaning through rarity
		- Add to this IDF as the process continues, further reducing the effect of words that have already been considered
- **Semantic Search:**
    
    - Vectorize the searchable sections of all `type=="text"` articles in the database.
	    - Create a new table for article_sections_embedding that links to article_sections and has the embedding version of the text
	    - Potentially expand the searchable section to optionally include other sections
    - Compute similarity scores between the input data vector and each article's vector.
    - Store these scores.
- **Scoring and Grouping:**
    
    - For each article, retrieve its associated categories.
    - Group articles by category and compute a weighted sum of the similarity scores for each category.
- **Category Selection:**
    
    - Select the top categories based on the weighted scores.
- **Conceptual Filter Creation (using IDF):**
    
    - For each selected top category, gather all articles within the category.
    - Compute IDF vectors for these articles.
    - Create a "conceptual filter" for each category by identifying the most relevant terms (e.g., keywords with high IDF scores).
- **Filtering Process:**
    
    - Apply the "conceptual filters" to refine the list of articles.
    - Ensure that the selected articles are highly relevant to the input data without over-including loosely related articles.
- **Iteration and Tree Traversal:**
    
    - Use the refined list of articles to explore related concepts.
    - Traverse the Wikipedia database like a tree, using similarity scores to guide the traversal.
    - Define an end-condition (e.g., reaching a certain depth, cumulative score threshold, or a predefined number of relevant articles).
- **Output:**
    
    - Generate a list of articles and concepts that best match the input data.
    - Provide a summary or visualization of the concept structure.
    - Generate a networkX graph that represents the traversal of the WikipediaDB

