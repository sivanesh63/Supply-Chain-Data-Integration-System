import requests
import pandas as pd
import json
from datetime import datetime
from utils.logger import log_pipeline_step, log_data_quality_check
from config import CUSTOM_API_BASE_URL, CUSTOM_API_ENDPOINTS

class CustomAPIConnector:
    """
    Handles data extraction from custom API endpoints
    """
    
    def __init__(self):
        self.logger = log_pipeline_step("CustomAPIConnector", "STARTED")
        self.base_url = CUSTOM_API_BASE_URL
        self.endpoints = CUSTOM_API_ENDPOINTS
        
    def get_orders_data(self):
        """
        Fetch orders data from custom API
        """
        try:
            self.logger.info("Fetching orders data from custom API...")
            
            response = requests.get(f"{self.base_url}{self.endpoints['orders']}")
            response.raise_for_status()
            
            data = response.json()
            orders_df = pd.DataFrame(data)
            
            # Data quality checks
            self._validate_orders_data(orders_df)
            
            # Transform data
            orders_df = self._transform_orders_data(orders_df)
            
            self.logger.info(f"Orders data fetched successfully. Shape: {orders_df.shape}")
            return orders_df
            
        except Exception as e:
            self.logger.error(f"Error fetching orders data: {str(e)}")
            return None
    
    def get_returns_data(self):
        """
        Fetch returns data from custom API
        """
        try:
            self.logger.info("Fetching returns data from custom API...")
            
            response = requests.get(f"{self.base_url}{self.endpoints['returns']}")
            response.raise_for_status()
            
            data = response.json()
            returns_df = pd.DataFrame(data)
            
            # Data quality checks
            self._validate_returns_data(returns_df)
            
            # Transform data
            returns_df = self._transform_returns_data(returns_df)
            
            self.logger.info(f"Returns data fetched successfully. Shape: {returns_df.shape}")
            return returns_df
            
        except Exception as e:
            self.logger.error(f"Error fetching returns data: {str(e)}")
            return None
    
    def get_people_data(self):
        """
        Fetch people data from custom API
        """
        try:
            self.logger.info("Fetching people data from custom API...")
            
            response = requests.get(f"{self.base_url}{self.endpoints['people']}")
            response.raise_for_status()
            
            data = response.json()
            people_df = pd.DataFrame(data)
            
            # Data quality checks
            self._validate_people_data(people_df)
            
            # Transform data
            people_df = self._transform_people_data(people_df)
            
            self.logger.info(f"People data fetched successfully. Shape: {people_df.shape}")
            return people_df
            
        except Exception as e:
            self.logger.error(f"Error fetching people data: {str(e)}")
            return None
    
    def get_analytics_data(self):
        """
        Fetch analytics data from custom API
        """
        try:
            self.logger.info("Fetching analytics data from custom API...")
            
            response = requests.get(f"{self.base_url}{self.endpoints['analytics']}")
            response.raise_for_status()
            
            data = response.json()
            
            self.logger.info("Analytics data fetched successfully")
            return data
            
        except Exception as e:
            self.logger.error(f"Error fetching analytics data: {str(e)}")
            return None
    
    def _validate_orders_data(self, df):
        """
        Validate Orders data quality
        """
        # Check for missing values
        missing_pct = df.isnull().sum().sum() / (df.shape[0] * df.shape[1])
        if missing_pct > 0.05:  # 5% threshold
            log_data_quality_check("Orders Missing Data", "WARNING", f"Missing data: {missing_pct:.2%}")
        else:
            log_data_quality_check("Orders Missing Data", "PASS", f"Missing data: {missing_pct:.2%}")
        
        # Check for required columns
        required_columns = ['Order ID', 'Order Date', 'Customer ID', 'Product ID']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            log_data_quality_check("Orders Required Columns", "FAIL", f"Missing columns: {missing_columns}")
        else:
            log_data_quality_check("Orders Required Columns", "PASS")
    
    def _validate_returns_data(self, df):
        """
        Validate Returns data quality
        """
        # Check for missing values
        missing_pct = df.isnull().sum().sum() / (df.shape[0] * df.shape[1])
        if missing_pct > 0.05:
            log_data_quality_check("Returns Missing Data", "WARNING", f"Missing data: {missing_pct:.2%}")
        else:
            log_data_quality_check("Returns Missing Data", "PASS", f"Missing data: {missing_pct:.2%}")
    
    def _validate_people_data(self, df):
        """
        Validate People data quality
        """
        # Check for missing values
        missing_pct = df.isnull().sum().sum() / (df.shape[0] * df.shape[1])
        if missing_pct > 0.05:
            log_data_quality_check("People Missing Data", "WARNING", f"Missing data: {missing_pct:.2%}")
        else:
            log_data_quality_check("People Missing Data", "PASS", f"Missing data: {missing_pct:.2%}")
    
    def _transform_orders_data(self, df):
        """
        Transform Orders data for analysis
        """
        # Convert date columns
        date_columns = ['Order Date', 'Ship Date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        
        # Calculate lead time
        if 'Order Date' in df.columns and 'Ship Date' in df.columns:
            df['Lead Time (Days)'] = (df['Ship Date'] - df['Order Date']).dt.days
        
        # Add year, month, quarter for time-based analysis
        if 'Order Date' in df.columns:
            df['Order Year'] = df['Order Date'].dt.year
            df['Order Month'] = df['Order Date'].dt.month
            df['Order Quarter'] = df['Order Date'].dt.quarter
        
        # Calculate order value if not present
        if 'Sales' in df.columns and 'Quantity' in df.columns:
            df['Order Value'] = df['Sales'] * df['Quantity']
        elif 'Sales' in df.columns:
            df['Order Value'] = df['Sales']
        
        return df
    
    def _transform_returns_data(self, df):
        """
        Transform Returns data for analysis
        """
        # Convert date columns
        if 'Order Date' in df.columns:
            df['Return Date'] = pd.to_datetime(df['Order Date'])
        
        return df
    
    def _transform_people_data(self, df):
        """
        Transform People data for analysis
        """
        # Convert date columns if they exist
        date_columns = ['First Order Date', 'Last Order Date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        
        return df
    
    def get_all_data(self):
        """
        Fetch all data from custom API endpoints
        """
        data = {}
        
        # Fetch each dataset
        data['orders'] = self.get_orders_data()
        data['returns'] = self.get_returns_data()
        data['people'] = self.get_people_data()
        data['analytics'] = self.get_analytics_data()
        
        return data 