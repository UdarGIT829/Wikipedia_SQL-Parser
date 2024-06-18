# semantic_utils.py

import tiktoken
from sentence_transformers import SentenceTransformer, util

class SemanticSimilarity:
    def __init__(self, base_text):
        """
        Initialize with the base text and encode it.
        """
        self.model = SentenceTransformer('paraphrase-MiniLM-L6-v2')
        self.base_embedding = self.model.encode(base_text, convert_to_tensor=True)

    def calculate_similarity(self, other_text):
        """
        Calculate the semantic similarity between the base text and the other text.
        """
        other_embedding = self.model.encode(other_text, convert_to_tensor=True)
        similarity = util.pytorch_cos_sim(self.base_embedding, other_embedding)
        return similarity.item()

def count_tokens(input_string):
    """
    Get the number of tokens in an input string.
    """
    encoding = tiktoken.encoding_for_model("gpt-3.5-turbo")
    tokens = encoding.encode(input_string)
    return len(tokens)
