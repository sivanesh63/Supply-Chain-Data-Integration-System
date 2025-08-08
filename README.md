# Supply Chain Analytics System

A comprehensive supply chain data integration and analytics system that processes data from `test.xlsx` and provides insights through a custom API and interactive dashboard.

## 🚀 Features

- **Data Integration**: Processes data from `test.xlsx` (train, Return, People sheets)
- **Custom API**: RESTful API serving data from Excel files
- **Interactive Dashboard**: Streamlit-based analytics dashboard
- **Real-time Analytics**: Supply chain metrics and KPIs
- **Data Quality Checks**: Automated validation and quality monitoring
- **Export Capabilities**: Generate reports and export data

## 📊 Data Sources

### Primary Dataset: `test.xlsx`
- **train sheet**: Contains orders data with customer, product, and sales information
- **Return sheet**: Contains returns data for analysis
- **People sheet**: Contains customer information and demographics

### Custom API Endpoints
- `GET /api/orders` - Get orders data with pagination
- `GET /api/returns` - Get returns data with pagination  
- `GET /api/people` - Get customer data with pagination
- `GET /api/analytics` - Get analytics summary
- `GET /api/health` - Health check endpoint

## 🛠️ Installation

### Prerequisites
- Python 3.8+
- pip package manager

### Setup
```bash
# Clone the repository
git clone <repository-url>
cd sivanesh

# Install dependencies
pip install -r requirements.txt
```

### Configuration

1. **Environment Setup** :
   ```bash
   # Create .env file
   echo "GOOGLE_CLOUD_PROJECT_ID=your-project-id" > .env
   ```

2. **Data Source**: Ensure `test.xlsx` is in the project root directory

## 🚀 Running the System

### 1. Start the Custom API Server
```bash
# Start the API server (serves data from test.xlsx)
python start_api.py
```
The API will be available at:
- **API Base URL**: http://localhost:8000
- **API Documentation**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/api/health

### 2. Run the Dashboard
```bash
# Start the Streamlit dashboard
python -m streamlit run dashboard/streamlit_app.py
```
The dashboard will be available at: http://localhost:8501


## 📁 Project Structure

```
sivanesh/
├── main.py                          # Main pipeline orchestrator
├── config.py                        # Configuration settings
├── requirements.txt                 # Python dependencies
├── README.md                       # This file
├── test.xlsx                       # Primary dataset
├── api_server.py                   # Custom API server
├── start_api.py                    # API server launcher
├── data_extraction/                # Data source connectors
│   ├── excel_connector.py          # Excel file processing (test.xlsx)
│   └── api_connector.py            # Custom API integration
├── data_processing/                # Data transformation & metrics
│   └── supply_chain_metrics.py     # KPI calculations
├── data_warehouse/                 # BigQuery operations
│   └── bigquery_connector.py       # Warehouse management
├── dashboard/                      # Streamlit application
│   └── streamlit_app.py           # Interactive dashboard
└── utils/                         # Utility functions
    └── logger.py                  # Logging utilities
```

## 🔧 Configuration

### Environment Variables

Create a `.env` file in the project root:

```env
# BigQuery Configuration (Optional)
GOOGLE_CLOUD_PROJECT_ID=your-project-id

# API Configuration
CUSTOM_API_BASE_URL=http://localhost:8000

# Dashboard Configuration
DASHBOARD_TITLE=Supply Chain Analytics Dashboard
```

### Configuration Options

Key settings in `config.py`:

```python
# Data Sources
EXCEL_FILE_PATH = "test.xlsx"  # Primary dataset

# Custom API Configuration
CUSTOM_API_BASE_URL = "http://localhost:8000"
CUSTOM_API_ENDPOINTS = {
    "orders": "/api/orders",
    "returns": "/api/returns", 
    "people": "/api/people",
    "analytics": "/api/analytics"
}

# Simulation Settings
INVENTORY_SIMULATION_DAYS = 30
RESTOCKING_FREQUENCY = 7  # days
DEMAND_VARIABILITY = 0.2  # 20% variability

# Supply Chain Thresholds
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
```

## 📈 API Endpoints

### Orders Data
```bash
GET /api/orders?limit=10&offset=0
```
Returns orders data with pagination support.

### Returns Data
```bash
GET /api/returns?limit=10&offset=0
```
Returns returns data with pagination support.

### People Data
```bash
GET /api/people?limit=10&offset=0
```
Returns customer data with pagination support.

### Analytics Summary
```bash
GET /api/analytics
```
Returns comprehensive analytics including:
- Summary statistics
- Regional sales breakdown
- Category performance
- Monthly sales trends

## 📊 Dashboard Features

### Key Performance Indicators
- Total Orders and Sales
- Average Order Value
- Lead Time Analysis
- Fill Rate Metrics
- Inventory Turnover

### Interactive Visualizations
- Lead Time Distribution Charts
- Inventory Trend Analysis
- Category Performance Comparison
- Regional Sales Breakdown
- Fill Rate Analysis

### Real-time Alerts
- Low Inventory Alerts
- Data Quality Warnings
- Performance Threshold Alerts


## 📋 Reports

### Available Reports
- Summary Analytics Report
- Inventory Analysis Report
- Customer Performance Report
- Regional Sales Report
- Category Performance Report

### Export Formats
- CSV Export
- Excel Export
- PDF Reports (planned)

## 🚀 Usage Examples

### 1. Quick Start with API
```bash
# Start API server
python start_api.py

# Test API endpoints
curl http://localhost:8000/api/health
curl http://localhost:8000/api/orders?limit=5
```

### 2. Dashboard Analytics
```bash
# Start dashboard
python -m streamlit run dashboard/streamlit_app.py

# Access dashboard at http://localhost:8501
```
