import os
from dotenv import load_dotenv

load_dotenv()

# API Configuration
FAKE_STORE_API_BASE_URL = "https://fakestoreapi.com"
KAGGLE_DATASET_NAME = "rohitsahoo/sales-forecasting"

# Custom API Configuration
CUSTOM_API_BASE_URL = "http://localhost:5000"
CUSTOM_API_ENDPOINTS = {
    "orders": "/api/orders",
    "returns": "/api/returns", 
    "people": "/api/people",
    "analytics": "/api/analytics"
}

# BigQuery Configuration
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT_ID", "your-project-id")
DATASET_ID = "supply_chain_analytics"
LOCATION = "US"

# Data Sources
DATA_DIR = "./data"
EXCEL_FILE_PATH = "test.xlsx"  # Updated to use test.xlsx as primary dataset

# Simulation Settings
INVENTORY_SIMULATION_DAYS = 30
RESTOCKING_FREQUENCY = 7  # days
DEMAND_VARIABILITY = 0.2  # 20% variability in daily demand

# Supply Chain Metrics Configuration
LEAD_TIME_THRESHOLDS = {
    "excellent": 3,
    "good": 7,
    "poor": 14
}

FILL_RATE_THRESHOLDS = {
    "excellent": 0.95,
    "good": 0.85,
    "poor": 0.75
}

# Dashboard Configuration
DASHBOARD_TITLE = "Supply Chain Analytics Dashboard"
DASHBOARD_THEME = "light"

# Logging Configuration
LOG_LEVEL = "INFO"
LOG_FILE = "supply_chain_pipeline.log"

# Alert Thresholds
INVENTORY_ALERT_THRESHOLD = 0.1  # 10% of max stock
MISSING_DATA_ALERT_THRESHOLD = 0.05  # 5% missing data 