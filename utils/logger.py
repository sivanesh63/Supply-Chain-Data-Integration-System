import logging
import os
from datetime import datetime
from config import LOG_LEVEL, LOG_FILE

def setup_logger(name="supply_chain_pipeline"):

    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)
    
    # Configure logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, LOG_LEVEL.upper()))
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Create formatters
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # File handler
    file_handler = logging.FileHandler(f"logs/{LOG_FILE}")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def log_pipeline_step(step_name, status="STARTED", details=None):

    logger = setup_logger()
    message = f"Pipeline Step: {step_name} - {status}"
    if details:
        message += f" - Details: {details}"
    
    if status == "STARTED":
        logger.info(message)
    elif status == "COMPLETED":
        logger.info(message)
    elif status == "ERROR":
        logger.error(message)
    elif status == "WARNING":
        logger.warning(message)
    
    return logger

def log_data_quality_check(check_name, result, details=None):

    logger = setup_logger()
    message = f"Data Quality Check: {check_name} - Result: {result}"
    if details:
        message += f" - Details: {details}"
    
    if result == "PASS":
        logger.info(message)
    elif result == "FAIL":
        logger.error(message)
    elif result == "WARNING":
        logger.warning(message)
    
    return logger

def log_alert(alert_type, message, severity="INFO"):

    logger = setup_logger()
    alert_message = f"ALERT [{alert_type}]: {message}"
    
    if severity == "CRITICAL":
        logger.critical(alert_message)
    elif severity == "ERROR":
        logger.error(alert_message)
    elif severity == "WARNING":
        logger.warning(alert_message)
    else:
        logger.info(alert_message)
    
    return logger 