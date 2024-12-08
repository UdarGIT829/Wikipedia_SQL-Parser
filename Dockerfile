# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the scripts directory
COPY scripts ./scripts

# Run the script
CMD ["python3", "scripts/wiki_parser.py", "data/enwiki-20240501-pages-articles-multistream.xml", "5"]
