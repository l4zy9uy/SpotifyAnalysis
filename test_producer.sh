#!/bin/bash

NUM_RUNS=20  # Number of times to run the producer
TOTAL_TIME=0  # Initialize total time

echo "Starting Kafka producer for $NUM_RUNS runs..."

for i in $(seq 1 $NUM_RUNS); do
  echo "Starting run $i..."

  # Measure the time for each run
  START=$(date +%s.%N)
  python3 producer.py  # Replace with the path to your Python script
  END=$(date +%s.%N)

  # Calculate the duration for this run
  DURATION=$(echo "$END - $START" | bc)
  echo "Run $i completed in $DURATION seconds" >> test_res.txt

  # Accumulate total time
  TOTAL_TIME=$(echo "$TOTAL_TIME + $DURATION" | bc)
done

# Calculate the average time
AVERAGE_TIME=$(echo "$TOTAL_TIME / $NUM_RUNS" | bc -l)

echo "All $NUM_RUNS runs completed."
echo "Average time per run: $AVERAGE_TIME seconds"
