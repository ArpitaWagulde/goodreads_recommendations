"""
Logging Configuration Module for Goodreads Data Pipeline

This module provides centralized logging configuration for all pipeline components.
It creates persistent log files that survive Airflow reruns and provides both
file and console logging with consistent formatting.

Key Features:
- Creates persistent log files in the logs/ directory
- Avoids duplicate handlers when DAGs are re-imported
- Provides both file and console logging (console disabled in Airflow)
- Consistent formatting across all pipeline components
- Captures full stack traces for error debugging

Author: Goodreads Recommendation Team
Date: 2025
"""

import logging
import os

def get_logger(name):
    """
    Create and configure a logger for the specified component.
    
    Args:
        name (str): Name of the logger (typically the module or component name)
        
    Returns:
        logging.Logger: Configured logger instance
        
    This function creates a logger with both file and console handlers (when not in Airflow),
    ensuring logs are persistent and properly formatted. It avoids duplicate handlers
    when DAGs are re-imported by checking existing handlers.
    """
    # Create logs directory if it doesn't exist
    log_dir = os.path.join(os.getcwd(), "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"{name}.log")

    # Create logger instance
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers if DAG is re-imported
    if not logger.handlers:
        # Create file handler for persistent logging
        file_handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Add console handler only when not running in Airflow
        # This prevents log duplication in Airflow's logging system
        if 'AIRFLOW_HOME' not in os.environ:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

    return logger
