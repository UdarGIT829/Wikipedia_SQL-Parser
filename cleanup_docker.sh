#!/bin/bash

# Run docker-compose and capture its exit code
docker-compose up
exit_code=$?

# Check if the exit code is 0 (success)
if [ $exit_code -eq 0 ]; then
    echo "Docker-compose exited successfully with code 0."

    # Bring down the containers
    docker-compose down

    # Find the image ID of wikipedia_sql-parser_wiki-parser and remove it
    image_id=$(docker images wikipedia_sql-parser_wiki-parser -q)
    
    # Check if the image ID was found before attempting to remove it
    if [ -n "$image_id" ]; then
        docker image rm "$image_id"
        echo "Removed image wikipedia_sql-parser_wiki-parser with ID $image_id."
    else
        echo "Image wikipedia_sql-parser_wiki-parser not found."
    fi
else
    echo "Docker-compose exited with code $exit_code. No further actions taken."
fi
