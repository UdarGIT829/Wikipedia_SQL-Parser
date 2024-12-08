docker compose up && echo "Safe to Exit" && while true; do read -t 30 -k 1 key && [[ $key == q ]] && exit; done
