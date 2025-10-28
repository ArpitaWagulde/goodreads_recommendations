import logging
import os

# Creates local copy of logs which are not lost when Airflow is rerun
# Captures and formats errors with full stack trace
def get_logger(name):
    log_dir = os.path.join(os.getcwd(), "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"{name}.log")

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers if DAG is re-imported
    if not logger.handlers:
        file_handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Skip console handler inside Airflow to prevent duplication
        if 'AIRFLOW_HOME' not in os.environ:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

    return logger
