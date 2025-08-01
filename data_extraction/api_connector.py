import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from utils.logger import log_pipeline_step, log_data_quality_check
from config import FAKE_STORE_API_BASE_URL, INVENTORY_SIMULATION_DAYS, RESTOCKING_FREQUENCY, DEMAND_VARIABILITY

class APIConnector:
    """
    Handles API data extraction and inventory simulation
    """
    
    def __init__(self):
        self.logger = log_pipeline_step("APIConnector", "STARTED")
        self.base_url = FAKE_STORE_API_BASE_URL
        
    def get_products(self):
        """
        Fetch products from Fake Store API
        """
        try:
            self.logger.info("Fetching products from Fake Store API...")
            
            response = requests.get(f"{self.base_url}/products")
            response.raise_for_status()
            
            products = response.json()
            
            # Convert to DataFrame
            products_df = pd.DataFrame(products)
            
            # Data quality checks
            self._validate_products_data(products_df)
            
            self.logger.info(f"Products fetched successfully. Count: {len(products_df)}")
            return products_df
            
        except Exception as e:
            self.logger.error(f"Error fetching products: {str(e)}")
            return None
    
    def get_categories(self):
        """
        Fetch product categories from Fake Store API
        """
        try:
            self.logger.info("Fetching categories from Fake Store API...")
            
            response = requests.get(f"{self.base_url}/products/categories")
            response.raise_for_status()
            
            categories = response.json()
            
            # Convert to DataFrame
            categories_df = pd.DataFrame(categories, columns=['category'])
            
            self.logger.info(f"Categories fetched successfully. Count: {len(categories_df)}")
            return categories_df
            
        except Exception as e:
            self.logger.error(f"Error fetching categories: {str(e)}")
            return None
    
    def simulate_inventory(self, products_df, days=INVENTORY_SIMULATION_DAYS):
        """
        Simulate 30-day inventory activity with daily demand, restocking, and price changes
        """
        try:
            self.logger.info(f"Starting inventory simulation for {days} days...")
            
            inventory_data = []
            current_date = datetime.now() - timedelta(days=days)
            
            for day in range(days):
                date = current_date + timedelta(days=day)
                
                for _, product in products_df.iterrows():
                    # Base demand (random between 1-10 units per day)
                    base_demand = random.randint(1, 10)
                    
                    # Add variability to demand
                    demand_variability = random.uniform(1 - DEMAND_VARIABILITY, 1 + DEMAND_VARIABILITY)
                    daily_demand = max(0, int(base_demand * demand_variability))
                    
                    # Get current stock level (initialize if first day)
                    if day == 0:
                        current_stock = random.randint(50, 200)  # Initial stock
                    else:
                        # Get previous day's stock
                        prev_record = next((r for r in inventory_data if r['product_id'] == product['id'] and r['date'] == date - timedelta(days=1)), None)
                        current_stock = prev_record['stock_level'] if prev_record else random.randint(50, 200)
                    
                    # Calculate new stock level
                    new_stock = current_stock - daily_demand
                    
                    # Restocking logic (every 7 days)
                    if day % RESTOCKING_FREQUENCY == 0:
                        restock_amount = random.randint(50, 100)
                        new_stock += restock_amount
                        restocked = True
                    else:
                        restock_amount = 0
                        restocked = False
                    
                    # Price changes (random 5% variation)
                    price_change = random.uniform(0.95, 1.05)
                    new_price = product['price'] * price_change
                    
                    # Create inventory record
                    inventory_record = {
                        'date': date,
                        'product_id': product['id'],
                        'product_name': product['title'],
                        'category': product['category'],
                        'daily_demand': daily_demand,
                        'stock_level': max(0, new_stock),
                        'restock_amount': restock_amount,
                        'restocked': restocked,
                        'price': new_price,
                        'original_price': product['price'],
                        'price_change_pct': ((new_price - product['price']) / product['price']) * 100
                    }
                    
                    inventory_data.append(inventory_record)
            
            # Convert to DataFrame
            inventory_df = pd.DataFrame(inventory_data)
            
            # Calculate additional metrics
            inventory_df = self._calculate_inventory_metrics(inventory_df)
            
            self.logger.info(f"Inventory simulation completed. Records: {len(inventory_df)}")
            return inventory_df
            
        except Exception as e:
            self.logger.error(f"Error in inventory simulation: {str(e)}")
            return None
    
    def _validate_products_data(self, df):
        """
        Validate products data quality
        """
        # Check for missing values
        missing_pct = df.isnull().sum().sum() / (df.shape[0] * df.shape[1])
        if missing_pct > 0.05:
            log_data_quality_check("Products Missing Data", "WARNING", f"Missing data: {missing_pct:.2%}")
        else:
            log_data_quality_check("Products Missing Data", "PASS", f"Missing data: {missing_pct:.2%}")
        
        # Check for required columns
        required_columns = ['id', 'title', 'price', 'category']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            log_data_quality_check("Products Required Columns", "FAIL", f"Missing columns: {missing_columns}")
        else:
            log_data_quality_check("Products Required Columns", "PASS")
        
        # Check for valid prices
        if 'price' in df.columns:
            invalid_prices = df[df['price'] <= 0].shape[0]
            if invalid_prices > 0:
                log_data_quality_check("Products Price Validation", "WARNING", f"Invalid prices: {invalid_prices}")
            else:
                log_data_quality_check("Products Price Validation", "PASS")
    
    def _calculate_inventory_metrics(self, df):
        """
        Calculate additional inventory metrics
        """
        # Calculate days of inventory remaining
        df['days_of_inventory'] = np.where(
            df['daily_demand'] > 0,
            df['stock_level'] / df['daily_demand'],
            float('inf')
        )
        
        # Calculate stockout risk (days of inventory < 3)
        df['stockout_risk'] = df['days_of_inventory'] < 3
        
        # Calculate inventory turnover (annualized)
        df['annualized_turnover'] = np.where(
            df['stock_level'] > 0,
            (df['daily_demand'] * 365) / df['stock_level'],
            0
        )
        
        # Calculate fill rate
        df['fill_rate'] = np.where(
            df['daily_demand'] > 0,
            np.minimum(1.0, df['stock_level'] / df['daily_demand']),
            1.0
        )
        
        return df
    
    def get_all_api_data(self):
        """
        Get all API data including products, categories, and simulated inventory
        """
        data = {}
        
        # Get products
        products_df = self.get_products()
        if products_df is not None:
            data['products'] = products_df
            
            # Simulate inventory based on products
            inventory_df = self.simulate_inventory(products_df)
            if inventory_df is not None:
                data['inventory'] = inventory_df
        
        # Get categories
        categories_df = self.get_categories()
        if categories_df is not None:
            data['categories'] = categories_df
        
        return data 