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
