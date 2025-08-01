import pandas as pd
import os
import kaggle
from datetime import datetime
from utils.logger import log_pipeline_step, log_data_quality_check
from config import DATA_DIR, EXCEL_FILE_PATH, KAGGLE_DATASET_NAME

class ExcelConnector:
    """
    Handles extraction and processing of Excel data from Global Superstore dataset
    """
    
    def __init__(self):
        self.logger = log_pipeline_step("ExcelConnector", "STARTED")
        self.data_dir = DATA_DIR
        self.excel_file_path = EXCEL_FILE_PATH
        
    def download_dataset(self):
        """
        Download the Global Superstore dataset from Kaggle
        """
        try:
            self.logger.info("Downloading Global Superstore dataset from Kaggle...")
            
            # Create data directory if it doesn't exist
            os.makedirs(self.data_dir, exist_ok=True)
            
            # Download dataset
            kaggle.api.dataset_download_files(
                KAGGLE_DATASET_NAME, 
                path=self.data_dir, 
                unzip=True
            )
            
            self.logger.info("Dataset downloaded successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error downloading dataset: {str(e)}")
            return False
    
    def load_orders_data(self):
        """
        Load and process Orders sheet from Excel file
        """
        try:
            self.logger.info("Loading Orders data from Excel...")
            
            # Read Orders sheet
            orders_df = pd.read_excel(self.excel_file_path, sheet_name='Orders')
            
            # Data quality checks
            self._validate_orders_data(orders_df)
            
            # Transform data
            orders_df = self._transform_orders_data(orders_df)
            
            self.logger.info(f"Orders data loaded successfully. Shape: {orders_df.shape}")
            return orders_df
            
        except Exception as e:
            self.logger.error(f"Error loading Orders data: {str(e)}")
            return None
    
    def load_returns_data(self):
        """
        Load and process Returns sheet from Excel file
        """
        try:
            self.logger.info("Loading Returns data from Excel...")
            
            # Read Returns sheet
            returns_df = pd.read_excel(self.excel_file_path, sheet_name='Returns')
            
            # Data quality checks
            self._validate_returns_data(returns_df)
            
            # Transform data
            returns_df = self._transform_returns_data(returns_df)
            
            self.logger.info(f"Returns data loaded successfully. Shape: {returns_df.shape}")
            return returns_df
            
        except Exception as e:
            self.logger.error(f"Error loading Returns data: {str(e)}")
            return None
    
    def load_people_data(self):
        """
        Load and process People sheet from Excel file
        """
        try:
            self.logger.info("Loading People data from Excel...")
            
            # Read People sheet
            people_df = pd.read_excel(self.excel_file_path, sheet_name='People')
            
            # Data quality checks
            self._validate_people_data(people_df)
            
            # Transform data
            people_df = self._transform_people_data(people_df)
            
            self.logger.info(f"People data loaded successfully. Shape: {people_df.shape}")
            return people_df
            
        except Exception as e:
            self.logger.error(f"Error loading People data: {str(e)}")
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
        required_columns = ['Order ID', 'Order Date', 'Ship Date', 'Customer ID', 'Product ID']
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
        
        return df
    
    def _transform_returns_data(self, df):
        """
        Transform Returns data for analysis
        """
        # Convert date columns
        if 'Return Date' in df.columns:
            df['Return Date'] = pd.to_datetime(df['Return Date'])
        
        return df
    
    def _transform_people_data(self, df):
        """
        Transform People data for analysis
        """
        # Ensure Person column exists
        if 'Person' not in df.columns:
            df['Person'] = 'Unknown'
        
        return df
    
    def get_all_data(self):
        """
        Load all sheets from the Excel file
        """
        data = {}
        
        # Load each sheet
        data['orders'] = self.load_orders_data()
        data['returns'] = self.load_returns_data()
        data['people'] = self.load_people_data()
        
        return data 