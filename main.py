#!/usr/bin/env python3
"""
Supply Chain Data Integration System
Main pipeline orchestrator
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
import time

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_extraction.excel_connector import ExcelConnector
from data_extraction.api_connector import APIConnector
from data_processing.supply_chain_metrics import SupplyChainMetrics
from data_warehouse.bigquery_connector import BigQueryConnector
from dashboard.streamlit_app import SupplyChainDashboard
from utils.logger import log_pipeline_step, log_alert, setup_logger
from config import *

class SupplyChainPipeline:
    """
    Main pipeline orchestrator for supply chain data integration
    """
    
    def __init__(self):
        self.logger = setup_logger("SupplyChainPipeline")
        self.logger.info("Initializing Supply Chain Data Integration Pipeline")
        
        # Initialize components
        self.excel_connector = ExcelConnector()
        self.api_connector = APIConnector()
        self.metrics_calculator = SupplyChainMetrics()
        self.bigquery_connector = BigQueryConnector()
        self.dashboard = SupplyChainDashboard()
        
        # Data storage
        self.data_dict = {}
        self.metrics_dict = {}
    
    def run_full_pipeline(self):
        """
        Run the complete supply chain data integration pipeline
        """
        try:
            self.logger.info("Starting full pipeline execution")
            
            # Step 1: Data Extraction
            self.extract_data()
            
            # Step 2: Data Processing and Metrics Calculation
            self.process_data()
            
            # Step 3: Data Warehouse Operations
            self.load_to_warehouse()
            
            # Step 4: Create Data Marts
            self.create_data_marts()
            
            # Step 5: Run Dashboard
            self.run_dashboard()
            
            self.logger.info("Pipeline execution completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {str(e)}")
            return False
    
    def extract_data(self):
        """
        Extract data from all sources
        """
        self.logger.info("Starting data extraction phase")
        
        # Extract Excel data
        self.logger.info("Extracting Excel data...")
        excel_data = self.excel_connector.get_all_data()
        if excel_data:
            self.data_dict.update(excel_data)
            self.logger.info("Excel data extracted successfully")
        else:
            self.logger.warning("No Excel data available")
        
        # Extract API data
        self.logger.info("Extracting API data...")
        api_data = self.api_connector.get_all_api_data()
        if api_data:
            self.data_dict.update(api_data)
            self.logger.info("API data extracted successfully")
        else:
            self.logger.warning("No API data available")
        
        # Log extraction summary
        self.log_data_summary()
    
    def process_data(self):
        """
        Process data and calculate metrics
        """
        self.logger.info("Starting data processing phase")
        
        # Calculate supply chain metrics
        if 'orders' in self.data_dict and 'inventory' in self.data_dict:
            self.metrics_dict = self.metrics_calculator.calculate_all_metrics(
                self.data_dict['orders'],
                self.data_dict['inventory'],
                self.data_dict.get('returns')
            )
            self.logger.info("Supply chain metrics calculated successfully")
        else:
            self.logger.warning("Insufficient data for metrics calculation")
        
        # Log metrics summary
        self.log_metrics_summary()
    
    def load_to_warehouse(self):
        """
        Load data to BigQuery warehouse
        """
        self.logger.info("Starting data warehouse operations")
        
        try:
            # Create dataset and tables
            if self.bigquery_connector.client:
                self.bigquery_connector.create_dataset()
                self.bigquery_connector.create_star_schema_tables()
                
                # Load data to BigQuery
                self.bigquery_connector.load_data_to_bigquery(self.data_dict)
                self.logger.info("Data loaded to BigQuery successfully")
            else:
                self.logger.warning("BigQuery client not available - skipping warehouse operations")
                
        except Exception as e:
            self.logger.error(f"Error in warehouse operations: {str(e)}")
    
    def create_data_marts(self):
        """
        Create specialized data marts
        """
        self.logger.info("Creating data marts")
        
        try:
            if self.bigquery_connector.client:
                self.bigquery_connector.create_data_marts()
                self.logger.info("Data marts created successfully")
            else:
                self.logger.warning("BigQuery client not available - skipping data mart creation")
                
        except Exception as e:
            self.logger.error(f"Error creating data marts: {str(e)}")
    
    def run_dashboard(self):
        """
        Run the Streamlit dashboard
        """
        self.logger.info("Starting dashboard")
        
        try:
            # Run dashboard with current data
            self.dashboard.run_dashboard(self.data_dict, self.metrics_dict)
            self.logger.info("Dashboard started successfully")
            
        except Exception as e:
            self.logger.error(f"Error running dashboard: {str(e)}")
    
    def log_data_summary(self):
        """
        Log summary of extracted data
        """
        self.logger.info("=== Data Extraction Summary ===")
        
        for source, data in self.data_dict.items():
            if data is not None and len(data) > 0:
                self.logger.info(f"{source}: {len(data)} records, {data.shape[1]} columns")
            else:
                self.logger.warning(f"{source}: No data available")
        
        self.logger.info("================================")
    
    def log_metrics_summary(self):
        """
        Log summary of calculated metrics
        """
        self.logger.info("=== Metrics Calculation Summary ===")
        
        for metric_type, metrics in self.metrics_dict.items():
            if metrics:
                self.logger.info(f"{metric_type}: {len(metrics)} metrics calculated")
                # Log key metrics
                if metric_type == 'lead_time' and 'mean_lead_time' in metrics:
                    self.logger.info(f"  - Mean Lead Time: {metrics['mean_lead_time']:.1f} days")
                if metric_type == 'fill_rate' and 'mean_fill_rate' in metrics:
                    self.logger.info(f"  - Mean Fill Rate: {metrics['mean_fill_rate']:.1%}")
            else:
                self.logger.warning(f"{metric_type}: No metrics calculated")
        
        self.logger.info("===================================")
    
    def run_demo_mode(self):
        """
        Run the system in demo mode with sample data
        """
        self.logger.info("Running in demo mode with sample data")
        
        # Create sample data
        self.create_sample_data()
        
        # Process sample data
        self.process_data()
        
        # Run dashboard with sample data
        self.run_dashboard()
    
    def create_sample_data(self):
        """
        Create sample data for demonstration
        """
        self.logger.info("Creating sample data for demonstration")
        
        # Sample orders data
        sample_orders = pd.DataFrame({
            'Order ID': [f'ORD_{i:03d}' for i in range(1, 101)],
            'Order Date': pd.date_range('2024-01-01', periods=100, freq='D'),
            'Ship Date': pd.date_range('2024-01-03', periods=100, freq='D'),
            'Customer ID': [f'CUST_{i%20:02d}' for i in range(1, 101)],
            'Product ID': [f'PROD_{i%10:02d}' for i in range(1, 101)],
            'Category': ['Electronics', 'Clothing', 'Home', 'Sports'] * 25,
            'Quantity': np.random.randint(1, 10, 100),
            'Sales': np.random.uniform(100, 1000, 100),
            'Profit': np.random.uniform(10, 200, 100),
            'Discount': np.random.uniform(0, 0.3, 100),
            'Country': ['USA', 'Canada', 'UK', 'Germany', 'France'] * 20,
            'State': ['CA', 'NY', 'TX', 'FL', 'IL'] * 20,
            'Region': ['West', 'East', 'Central', 'South'] * 25
        })
        
        # Calculate lead time
        sample_orders['Lead Time (Days)'] = (sample_orders['Ship Date'] - sample_orders['Order Date']).dt.days
        sample_orders['Order Value'] = sample_orders['Sales'] * sample_orders['Quantity']
        
        # Sample inventory data
        sample_inventory = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=30, freq='D'),
            'product_id': [f'PROD_{i%10:02d}' for i in range(1, 301)],
            'product_name': [f'Product {i%10}' for i in range(1, 301)],
            'category': ['Electronics', 'Clothing', 'Home', 'Sports'] * 75,
            'stock_level': np.random.randint(10, 200, 300),
            'daily_demand': np.random.randint(1, 10, 300),
            'restock_amount': np.random.randint(0, 50, 300),
            'restocked': np.random.choice([True, False], 300, p=[0.3, 0.7]),
            'price': np.random.uniform(20, 500, 300),
            'original_price': np.random.uniform(20, 500, 300),
            'price_change_pct': np.random.uniform(-10, 10, 300),
            'days_of_inventory': np.random.uniform(5, 30, 300),
            'stockout_risk': np.random.choice([True, False], 300, p=[0.1, 0.9]),
            'annualized_turnover': np.random.uniform(2, 12, 300),
            'fill_rate': np.random.uniform(0.7, 1.0, 300)
        })
        
        # Sample returns data
        sample_returns = pd.DataFrame({
            'Return Date': pd.date_range('2024-01-01', periods=20, freq='D'),
            'Order ID': [f'ORD_{i:03d}' for i in np.random.randint(1, 101, 20)],
            'Customer ID': [f'CUST_{i%20:02d}' for i in range(1, 21)],
            'Product ID': [f'PROD_{i%10:02d}' for i in range(1, 21)],
            'Category': ['Electronics', 'Clothing', 'Home', 'Sports'] * 5
        })
        
        # Sample products data
        sample_products = pd.DataFrame({
            'id': [f'PROD_{i:02d}' for i in range(1, 11)],
            'title': [f'Sample Product {i}' for i in range(1, 11)],
            'price': np.random.uniform(20, 500, 10),
            'category': ['Electronics', 'Clothing', 'Home', 'Sports'] * 2 + ['Electronics', 'Clothing'],
            'description': [f'Description for product {i}' for i in range(1, 11)],
            'image': [f'https://example.com/image{i}.jpg' for i in range(1, 11)]
        })
        
        # Store sample data
        self.data_dict = {
            'orders': sample_orders,
            'inventory': sample_inventory,
            'returns': sample_returns,
            'products': sample_products
        }
        
        self.logger.info("Sample data created successfully")
    
    def run_streamlit_dashboard(self):
        """
        Run only the Streamlit dashboard
        """
        self.logger.info("Running Streamlit dashboard")
        
        # Create sample data if no data exists
        if not self.data_dict:
            self.create_sample_data()
            self.process_data()
        
        # Run dashboard
        self.run_dashboard()

def main():
    """
    Main function to run the supply chain pipeline
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Supply Chain Data Integration System')
    parser.add_argument('--mode', choices=['full', 'demo', 'dashboard'], 
                       default='demo', help='Pipeline execution mode')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       default='INFO', help='Logging level')
    
    args = parser.parse_args()
    
    # Set log level
    import logging
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    # Initialize pipeline
    pipeline = SupplyChainPipeline()
    
    # Run based on mode
    if args.mode == 'full':
        success = pipeline.run_full_pipeline()
        if success:
            print("✅ Pipeline completed successfully")
        else:
            print("❌ Pipeline failed")
            sys.exit(1)
    
    elif args.mode == 'demo':
        pipeline.run_demo_mode()
        print("✅ Demo mode completed")
    
    elif args.mode == 'dashboard':
        pipeline.run_streamlit_dashboard()
        print("✅ Dashboard started")

if __name__ == "__main__":
    main() 