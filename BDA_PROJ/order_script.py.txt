import argparse
import logging
from kafka import KafkaConsumer
import subprocess


def write_to_hdfs(data, hdfs_path):
    """
    Write data to the specified HDFS file (part-r-00000).
    """
    try:
        hdfs_file_path = f"{hdfs_path}/part-r-00000"
        process = subprocess.run(
            [
                "docker",
                "exec",
                "hadoop-namenode-bda",
                "bash",
                "-c",
                f"echo '{data}' | hdfs dfs -appendToFile - {hdfs_file_path}",
            ],
            capture_output=True,
            text=True,
        )
        if process.returncode == 0:
            logging.info(f"Successfully wrote data to HDFS: {hdfs_file_path}")
        else:
            logging.error(f"Failed to write data to HDFS. Error: {process.stderr}")
    except Exception as e:
        logging.error(f"Exception during HDFS write: {e}")


def consume_and_store_data(topic, hdfs_path, batch_size=100):
    """
    Consume messages from Kafka and write to HDFS in a single batch.
    """
    logging.info(f"Connecting to Kafka topic: {topic}")
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=["kafka:9092"],
        auto_offset_reset="earliest",
    )

    buffer = []
    try:
        for message in consumer:
            msg_value = message.value.decode("utf-8")
            buffer.append(msg_value)
            logging.info(f"Consumed message: {msg_value}")

            # Exit after batch size is reached
            if len(buffer) >= batch_size:
                logging.info(f"Batch size reached ({batch_size} messages). Writing to HDFS and exiting...")
                write_to_hdfs("\n".join(buffer), hdfs_path)
                break  # Exit after processing the batch

        # Write any remaining messages in the buffer (unlikely due to break above)
        if buffer and len(buffer) < batch_size:
            logging.info(f"Writing remaining {len(buffer)} messages to HDFS...")
            write_to_hdfs("\n".join(buffer), hdfs_path)

        logging.info("Processing complete. Exiting.")

    except Exception as e:
        logging.error(f"Error during Kafka consumption: {e}")
    finally:
        consumer.close()


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(description="Consume Kafka topic and store data to HDFS.")
    parser.add_argument("--topic", required=True, help="Kafka topic name")
    parser.add_argument("--hdfs_path", required=True, help="HDFS directory to save data")
    parser.add_argument("--batch_size", type=int, default=50, help="Number of messages to process per batch")

    args = parser.parse_args()
    logging.info(f"Ensuring HDFS file exists at {args.hdfs_path}/part-r-00000.")
    
    # Ensure HDFS file exists
    subprocess.run(
        [
            "docker",
            "exec",
            "hadoop-namenode-bda",
            "bash",
            "-c",
            f"hdfs dfs -touchz {args.hdfs_path}/part-r-00000",
        ],
        capture_output=True,
        text=True,
    )

    consume_and_store_data(args.topic, args.hdfs_path, args.batch_size)
