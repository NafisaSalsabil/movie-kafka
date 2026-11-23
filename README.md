Requirements- Before running the project, make sure you have:
1. Docker Desktop installed and running
2. Docker Compose available
3. Sufficient disk space and memory for the Docker images and dataset.
    Recommended:
    RAM: 16 GB 
    Disk space: 15 GB of free space 


How to Run:
1. Clone or download the project directory into your machine
2. Open terminal in the project directory
3. Ensure Docker Desktop is running by executing "docker info". If Docker information is displayed without errors, proceed to the next step. Otherwise, restart Docker Desktop and try again.
4. Start the project by executing: docker compose up --build 


Deep dive (Part 2):
- Outputs: stream_hourly_counts/  late_ratings/  checkpoints/stream_hourly/
- Restart check:
  1. docker compose stop consumer
  2. docker compose start consumer
  3. Consumer resumes from checkpoints/stream_hourly/
- After a run (and any restart tests), compare to batch:
  docker compose run --rm batch python3 compare_deep_dive.py --max-messages 100

Part 4 (quality):
- Four checks: missing, range, ids, duplicates (each prints failed count)
- Stage in/out lines: [QUALITY] stage=...
- Bad rows quarantined to bad_ratings/ (and bad_ratings_producer.jsonl)
- [ALERT] if fail rate > 1%
- For the final video: add one bad CSV row by hand, re-run, show check + quarantine


How to Stop:
1. Inside the project directory, execute: docker compose down


Note: The first run may take longer because Docker needs to build the project images
and Spark may download the Kafka connector. Subsequent runs can reuse them.



