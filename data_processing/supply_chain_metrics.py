import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from utils.logger import log_pipeline_step, log_data_quality_check
from config import LEAD_TIME_THRESHOLDS, FILL_RATE_THRESHOLDS

class SupplyChainMetrics:
    def __init__(self):
        self.logger = log_pipeline_step("SupplyChainMetrics", "STARTED")
    
    def calculate_lead_time_metrics(self, orders_df):

        try:
            self.logger.info("Calculating lead time metrics...")
            
            if 'Lead Time (Days)' not in orders_df.columns:
                self.logger.error("Lead Time (Days) column not found in orders data")
                return None

            lead_time_stats = {
                'mean_lead_time': orders_df['Lead Time (Days)'].mean(),
                'median_lead_time': orders_df['Lead Time (Days)'].median(),
                'std_lead_time': orders_df['Lead Time (Days)'].std(),
                'min_lead_time': orders_df['Lead Time (Days)'].min(),
                'max_lead_time': orders_df['Lead Time (Days)'].max(),
                'total_orders': len(orders_df)
            }

            orders_df['lead_time_performance'] = pd.cut(
                orders_df['Lead Time (Days)'],
                bins=[0, LEAD_TIME_THRESHOLDS['excellent'], LEAD_TIME_THRESHOLDS['good'], float('inf')],
                labels=['Excellent', 'Good', 'Poor']
            )

            performance_dist = orders_df['lead_time_performance'].value_counts(normalize=True)
            lead_time_stats.update({
                'excellent_pct': performance_dist.get('Excellent', 0),
                'good_pct': performance_dist.get('Good', 0),
                'poor_pct': performance_dist.get('Poor', 0)
            })
            
            self.logger.info("Lead time metrics calculated successfully")
            return lead_time_stats
            
        except Exception as e:
            self.logger.error(f"Error calculating lead time metrics: {str(e)}")
            return None
    
    def calculate_order_cycle_time(self, orders_df):

        try:
            self.logger.info("Calculating order cycle time...")
            
            if 'Ship Date' in orders_df.columns:
                delivery_days = np.random.randint(2, 6, size=len(orders_df))
                orders_df['Delivery Date'] = orders_df['Ship Date'] + pd.to_timedelta(delivery_days, unit='D')
                
                orders_df['Order Cycle Time (Days)'] = (orders_df['Delivery Date'] - orders_df['Order Date']).dt.days
                
                cycle_time_stats = {
                    'mean_cycle_time': orders_df['Order Cycle Time (Days)'].mean(),
                    'median_cycle_time': orders_df['Order Cycle Time (Days)'].median(),
                    'std_cycle_time': orders_df['Order Cycle Time (Days)'].std(),
                    'min_cycle_time': orders_df['Order Cycle Time (Days)'].min(),
                    'max_cycle_time': orders_df['Order Cycle Time (Days)'].max()
                }
                
                self.logger.info("Order cycle time calculated successfully")
                return cycle_time_stats
            
            else:
                self.logger.warning("Ship Date not available, using lead time as proxy")
                return self.calculate_lead_time_metrics(orders_df)
                
        except Exception as e:
            self.logger.error(f"Error calculating order cycle time: {str(e)}")
            return None
    
    def calculate_inventory_turnover(self, inventory_df):

        try:
            self.logger.info("Calculating inventory turnover metrics...")
            
            product_turnover = inventory_df.groupby('product_id').agg({
                'daily_demand': 'mean',
                'stock_level': 'mean',
                'annualized_turnover': 'mean'
            }).reset_index()
            
            product_turnover['days_on_hand'] = np.where(
                product_turnover['daily_demand'] > 0,
                product_turnover['stock_level'] / product_turnover['daily_demand'],
                float('inf')
            )
            
            turnover_stats = {
                'mean_turnover': product_turnover['annualized_turnover'].mean(),
                'median_turnover': product_turnover['annualized_turnover'].median(),
                'mean_days_on_hand': product_turnover['days_on_hand'].mean(),
                'median_days_on_hand': product_turnover['days_on_hand'].median(),
                'total_products': len(product_turnover)
            }
            
            self.logger.info("Inventory turnover metrics calculated successfully")
            return turnover_stats
            
        except Exception as e:
            self.logger.error(f"Error calculating inventory turnover: {str(e)}")
            return None
    
    def calculate_fill_rate_metrics(self, inventory_df):

        try:
            self.logger.info("Calculating fill rate metrics...")
            
            product_fill_rates = inventory_df.groupby('product_id').agg({
                'fill_rate': 'mean',
                'stockout_risk': 'sum'
            }).reset_index()
            
            fill_rate_stats = {
                'mean_fill_rate': product_fill_rates['fill_rate'].mean(),
                'median_fill_rate': product_fill_rates['fill_rate'].median(),
                'excellent_fill_rate_pct': (product_fill_rates['fill_rate'] >= FILL_RATE_THRESHOLDS['excellent']).mean(),
                'good_fill_rate_pct': (product_fill_rates['fill_rate'] >= FILL_RATE_THRESHOLDS['good']).mean(),
                'poor_fill_rate_pct': (product_fill_rates['fill_rate'] < FILL_RATE_THRESHOLDS['poor']).mean(),
                'total_products': len(product_fill_rates),
                'products_at_risk': product_fill_rates['stockout_risk'].sum()
            }
            
            self.logger.info("Fill rate metrics calculated successfully")
            return fill_rate_stats
            
        except Exception as e:
            self.logger.error(f"Error calculating fill rate metrics: {str(e)}")
            return None
    
    def calculate_category_performance(self, orders_df, inventory_df):

        try:
            self.logger.info("Calculating category performance metrics...")
            
            if 'category' in orders_df.columns and 'category' in inventory_df.columns:
                category_lead_time = orders_df.groupby('category')['Lead Time (Days)'].agg([
                    'mean', 'median', 'std', 'count'
                ]).reset_index()
                
                category_inventory = inventory_df.groupby('category').agg({
                    'fill_rate': 'mean',
                    'annualized_turnover': 'mean',
                    'days_of_inventory': 'mean',
                    'stockout_risk': 'sum'
                }).reset_index()
                
                category_performance = pd.merge(
                    category_lead_time, 
                    category_inventory, 
                    on='category', 
                    how='outer'
                )
                
                self.logger.info("Category performance metrics calculated successfully")
                return category_performance
            
            else:
                self.logger.warning("Category information not available in both datasets")
                return None
                
        except Exception as e:
            self.logger.error(f"Error calculating category performance: {str(e)}")
            return None
    
    def calculate_return_metrics(self, orders_df, returns_df):

        try:
            self.logger.info("Calculating return metrics...")
            
            if returns_df is not None and len(returns_df) > 0:
                total_orders = len(orders_df)
                total_returns = len(returns_df)
                return_rate = total_returns / total_orders if total_orders > 0 else 0
                
                if 'category' in returns_df.columns:
                    category_returns = returns_df['category'].value_counts(normalize=True)
                else:
                    category_returns = None
                
                return_stats = {
                    'total_orders': total_orders,
                    'total_returns': total_returns,
                    'return_rate': return_rate,
                    'return_rate_pct': return_rate * 100,
                    'category_returns': category_returns
                }
                
                self.logger.info("Return metrics calculated successfully")
                return return_stats
            
            else:
                self.logger.warning("No returns data available")
                return {
                    'total_orders': len(orders_df),
                    'total_returns': 0,
                    'return_rate': 0,
                    'return_rate_pct': 0
                }
                
        except Exception as e:
            self.logger.error(f"Error calculating return metrics: {str(e)}")
            return None
    
    def calculate_all_metrics(self, orders_df, inventory_df, returns_df=None):

        try:
            self.logger.info("Calculating all supply chain metrics...")
            
            all_metrics = {}
            
            all_metrics['lead_time'] = self.calculate_lead_time_metrics(orders_df)
            all_metrics['order_cycle_time'] = self.calculate_order_cycle_time(orders_df)
            all_metrics['inventory_turnover'] = self.calculate_inventory_turnover(inventory_df)
            all_metrics['fill_rate'] = self.calculate_fill_rate_metrics(inventory_df)
            all_metrics['category_performance'] = self.calculate_category_performance(orders_df, inventory_df)
            all_metrics['return_metrics'] = self.calculate_return_metrics(orders_df, returns_df)
            
            self.logger.info("All supply chain metrics calculated successfully")
            return all_metrics
            
        except Exception as e:
            self.logger.error(f"Error calculating all metrics: {str(e)}")
            return None 