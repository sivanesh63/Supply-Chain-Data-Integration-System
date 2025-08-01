import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import os
from utils.logger import log_pipeline_step, log_data_quality_check
from config import PROJECT_ID, DATASET_ID, LOCATION

class BigQueryConnector:

    def __init__(self):
        self.logger = log_pipeline_step("BigQueryConnector", "STARTED")
        self.project_id = PROJECT_ID
        self.dataset_id = DATASET_ID
        self.location = LOCATION

        try:
            # Try to use service account if available
            if os.path.exists('service-account-key.json'):
                credentials = service_account.Credentials.from_service_account_file(
                    'service-account-key.json'
                )
                self.client = bigquery.Client(
                    credentials=credentials,
                    project=self.project_id
                )
            else:
                # Use default credentials
                self.client = bigquery.Client(project=self.project_id)
            
            self.logger.info("BigQuery client initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Error initializing BigQuery client: {str(e)}")
            self.client = None
    
    def create_dataset(self):

        try:
            self.logger.info(f"Creating dataset: {self.dataset_id}")
            
            dataset_ref = self.client.dataset(self.dataset_id)
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = self.location
            
            dataset = self.client.create_dataset(dataset, exists_ok=True)
            self.logger.info(f"Dataset {self.dataset_id} created successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating dataset: {str(e)}")
            return False
    
    def create_star_schema_tables(self):

        try:
            self.logger.info("Creating star schema tables...")
            
            # Create dimension tables
            self._create_dimensions()
            
            # Create fact tables
            self._create_facts()
            
            self.logger.info("Star schema tables created successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating star schema tables: {str(e)}")
            return False
    
    def _create_dimensions(self):

        # Products dimension
        products_schema = [
            bigquery.SchemaField("product_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("product_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("category", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("price", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("image_url", "STRING"),
            bigquery.SchemaField("created_date", "TIMESTAMP"),
            bigquery.SchemaField("updated_date", "TIMESTAMP")
        ]
        
        self._create_table("dim_products", products_schema, clustering_fields=["category"])
        
        # Customers dimension
        customers_schema = [
            bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("customer_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("segment", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("state", "STRING"),
            bigquery.SchemaField("postal_code", "STRING"),
            bigquery.SchemaField("region", "STRING"),
            bigquery.SchemaField("created_date", "TIMESTAMP"),
            bigquery.SchemaField("updated_date", "TIMESTAMP")
        ]
        
        self._create_table("dim_customers", customers_schema, clustering_fields=["segment", "country"])
        
        # Time dimension
        time_schema = [
            bigquery.SchemaField("date_id", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("year", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("month", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("day", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("quarter", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("day_of_week", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("is_weekend", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("is_holiday", "BOOLEAN", mode="REQUIRED")
        ]
        
        self._create_table("dim_time", time_schema, clustering_fields=["year", "month"])
        
        # Locations dimension
        locations_schema = [
            bigquery.SchemaField("location_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("country", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("state", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("postal_code", "STRING"),
            bigquery.SchemaField("region", "STRING"),
            bigquery.SchemaField("created_date", "TIMESTAMP"),
            bigquery.SchemaField("updated_date", "TIMESTAMP")
        ]
        
        self._create_table("dim_locations", locations_schema, clustering_fields=["country", "state"])
    
    def _create_facts(self):

        # Orders fact table
        orders_fact_schema = [
            bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("order_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("ship_date", "DATE"),
            bigquery.SchemaField("delivery_date", "DATE"),
            bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("product_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("location_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("quantity", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("sales", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("profit", "FLOAT"),
            bigquery.SchemaField("discount", "FLOAT"),
            bigquery.SchemaField("shipping_cost", "FLOAT"),
            bigquery.SchemaField("lead_time_days", "INTEGER"),
            bigquery.SchemaField("order_cycle_time_days", "INTEGER"),
            bigquery.SchemaField("order_value", "FLOAT"),
            bigquery.SchemaField("created_date", "TIMESTAMP"),
            bigquery.SchemaField("updated_date", "TIMESTAMP")
        ]
        
        self._create_table("fact_orders", orders_fact_schema, 
                          partitioning_field="order_date", 
                          clustering_fields=["customer_id", "product_id"])
        
        # Inventory fact table
        inventory_fact_schema = [
            bigquery.SchemaField("inventory_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("product_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("stock_level", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("daily_demand", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("restock_amount", "INTEGER"),
            bigquery.SchemaField("restocked", "BOOLEAN"),
            bigquery.SchemaField("price", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("original_price", "FLOAT"),
            bigquery.SchemaField("price_change_pct", "FLOAT"),
            bigquery.SchemaField("days_of_inventory", "FLOAT"),
            bigquery.SchemaField("stockout_risk", "BOOLEAN"),
            bigquery.SchemaField("annualized_turnover", "FLOAT"),
            bigquery.SchemaField("fill_rate", "FLOAT"),
            bigquery.SchemaField("created_date", "TIMESTAMP"),
            bigquery.SchemaField("updated_date", "TIMESTAMP")
        ]
        
        self._create_table("fact_inventory", inventory_fact_schema, 
                          partitioning_field="date", 
                          clustering_fields=["product_id"])
        
        # Returns fact table
        returns_fact_schema = [
            bigquery.SchemaField("return_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("return_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("product_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("return_reason", "STRING"),
            bigquery.SchemaField("return_amount", "FLOAT"),
            bigquery.SchemaField("created_date", "TIMESTAMP"),
            bigquery.SchemaField("updated_date", "TIMESTAMP")
        ]
        
        self._create_table("fact_returns", returns_fact_schema, 
                          partitioning_field="return_date", 
                          clustering_fields=["customer_id", "product_id"])
    
    def _create_table(self, table_name, schema, partitioning_field=None, clustering_fields=None):

        try:
            table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
            table = bigquery.Table(table_id, schema=schema)
            
            # Set partitioning if specified
            if partitioning_field:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=partitioning_field
                )
            
            # Set clustering if specified
            if clustering_fields:
                table.clustering_fields = clustering_fields
            
            # Create the table
            table = self.client.create_table(table, exists_ok=True)
            self.logger.info(f"Table {table_name} created successfully")
            
        except Exception as e:
            self.logger.error(f"Error creating table {table_name}: {str(e)}")
    
    def load_data_to_bigquery(self, data_dict):

        try:
            self.logger.info("Loading data to BigQuery...")
            
            # Load dimension tables first
            if 'products' in data_dict:
                self._load_products_dimension(data_dict['products'])
            
            if 'orders' in data_dict:
                self._load_orders_fact(data_dict['orders'])
            
            if 'inventory' in data_dict:
                self._load_inventory_fact(data_dict['inventory'])
            
            if 'returns' in data_dict:
                self._load_returns_fact(data_dict['returns'])
            
            self.logger.info("Data loaded to BigQuery successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading data to BigQuery: {str(e)}")
            return False
    
    def _load_products_dimension(self, products_df):

        try:
            # Prepare data for BigQuery
            products_df['created_date'] = pd.Timestamp.now()
            products_df['updated_date'] = pd.Timestamp.now()
            
            # Select and rename columns
            products_bq = products_df[['id', 'title', 'category', 'price', 'description', 'image']].copy()
            products_bq.columns = ['product_id', 'product_name', 'category', 'price', 'description', 'image_url']
            
            # Load to BigQuery
            table_id = f"{self.project_id}.{self.dataset_id}.dim_products"
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
            
            job = self.client.load_table_from_dataframe(
                products_bq, table_id, job_config=job_config
            )
            job.result()
            
            self.logger.info(f"Products dimension loaded: {len(products_bq)} records")
            
        except Exception as e:
            self.logger.error(f"Error loading products dimension: {str(e)}")
    
    def _load_orders_fact(self, orders_df):

        try:
            # Prepare data for BigQuery
            orders_df['created_date'] = pd.Timestamp.now()
            orders_df['updated_date'] = pd.Timestamp.now()
            
            # Create location_id from shipping info
            if 'Country' in orders_df.columns and 'State' in orders_df.columns:
                orders_df['location_id'] = orders_df['Country'] + '_' + orders_df['State']
            else:
                orders_df['location_id'] = 'Unknown'
            
            # Select and rename columns
            orders_bq = orders_df[['Order ID', 'Order Date', 'Ship Date', 'Customer ID', 
                                 'Product ID', 'location_id', 'Quantity', 'Sales', 'Profit', 
                                 'Discount', 'Lead Time (Days)', 'Order Value', 'created_date', 'updated_date']].copy()
            
            orders_bq.columns = ['order_id', 'order_date', 'ship_date', 'customer_id', 
                               'product_id', 'location_id', 'quantity', 'sales', 'profit', 
                               'discount', 'lead_time_days', 'order_value', 'created_date', 'updated_date']
            
            # Load to BigQuery
            table_id = f"{self.project_id}.{self.dataset_id}.fact_orders"
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
            
            job = self.client.load_table_from_dataframe(
                orders_bq, table_id, job_config=job_config
            )
            job.result()
            
            self.logger.info(f"Orders fact loaded: {len(orders_bq)} records")
            
        except Exception as e:
            self.logger.error(f"Error loading orders fact: {str(e)}")
    
    def _load_inventory_fact(self, inventory_df):

        try:
            # Prepare data for BigQuery
            inventory_df['created_date'] = pd.Timestamp.now()
            inventory_df['updated_date'] = pd.Timestamp.now()
            inventory_df['inventory_id'] = inventory_df['product_id'].astype(str) + '_' + inventory_df['date'].dt.strftime('%Y%m%d')
            
            # Select and rename columns
            inventory_bq = inventory_df[['inventory_id', 'date', 'product_id', 'stock_level', 
                                       'daily_demand', 'restock_amount', 'restocked', 'price', 
                                       'original_price', 'price_change_pct', 'days_of_inventory', 
                                       'stockout_risk', 'annualized_turnover', 'fill_rate', 
                                       'created_date', 'updated_date']].copy()
            
            # Load to BigQuery
            table_id = f"{self.project_id}.{self.dataset_id}.fact_inventory"
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
            
            job = self.client.load_table_from_dataframe(
                inventory_bq, table_id, job_config=job_config
            )
            job.result()
            
            self.logger.info(f"Inventory fact loaded: {len(inventory_bq)} records")
            
        except Exception as e:
            self.logger.error(f"Error loading inventory fact: {str(e)}")
    
    def _load_returns_fact(self, returns_df):

        try:
            # Prepare data for BigQuery
            returns_df['created_date'] = pd.Timestamp.now()
            returns_df['updated_date'] = pd.Timestamp.now()
            returns_df['return_id'] = 'RET_' + returns_df.index.astype(str)
            
            # Select and rename columns
            returns_bq = returns_df[['return_id', 'Return Date', 'Order ID', 'Customer ID', 
                                   'Product ID', 'created_date', 'updated_date']].copy()
            
            returns_bq.columns = ['return_id', 'return_date', 'order_id', 'customer_id', 
                                'product_id', 'created_date', 'updated_date']
            
            # Load to BigQuery
            table_id = f"{self.project_id}.{self.dataset_id}.fact_returns"
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
            
            job = self.client.load_table_from_dataframe(
                returns_bq, table_id, job_config=job_config
            )
            job.result()
            
            self.logger.info(f"Returns fact loaded: {len(returns_bq)} records")
            
        except Exception as e:
            self.logger.error(f"Error loading returns fact: {str(e)}")
    
    def create_data_marts(self):

        try:
            self.logger.info("Creating data marts...")
            
            # Vendor Performance Mart
            self._create_vendor_performance_mart()
            
            # Inventory Analysis Mart
            self._create_inventory_analysis_mart()
            
            # Fulfillment Analytics Mart
            self._create_fulfillment_analytics_mart()
            
            # Category Performance Mart
            self._create_category_performance_mart()
            
            self.logger.info("Data marts created successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating data marts: {str(e)}")
            return False
    
    def _create_vendor_performance_mart(self):

        query = """
        CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.mart_vendor_performance` AS
        SELECT 
            p.category,
            COUNT(DISTINCT o.order_id) as total_orders,
            AVG(o.lead_time_days) as avg_lead_time,
            AVG(o.order_value) as avg_order_value,
            SUM(o.sales) as total_sales,
            SUM(o.profit) as total_profit,
            COUNT(DISTINCT r.return_id) as total_returns,
            CASE 
                WHEN COUNT(DISTINCT o.order_id) > 0 
                THEN COUNT(DISTINCT r.return_id) / COUNT(DISTINCT o.order_id) 
                ELSE 0 
            END as return_rate
        FROM `{project_id}.{dataset_id}.fact_orders` o
        LEFT JOIN `{project_id}.{dataset_id}.dim_products` p ON o.product_id = p.product_id
        LEFT JOIN `{project_id}.{dataset_id}.fact_returns` r ON o.order_id = r.order_id
        GROUP BY p.category
        """
        
        self._execute_query(query.format(project_id=self.project_id, dataset_id=self.dataset_id))
    
    def _create_inventory_analysis_mart(self):

        query = """
        CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.mart_inventory_analysis` AS
        SELECT 
            p.category,
            AVG(i.stock_level) as avg_stock_level,
            AVG(i.daily_demand) as avg_daily_demand,
            AVG(i.fill_rate) as avg_fill_rate,
            AVG(i.annualized_turnover) as avg_turnover,
            AVG(i.days_of_inventory) as avg_days_of_inventory,
            SUM(CASE WHEN i.stockout_risk THEN 1 ELSE 0 END) as stockout_risk_count,
            COUNT(*) as total_records
        FROM `{project_id}.{dataset_id}.fact_inventory` i
        LEFT JOIN `{project_id}.{dataset_id}.dim_products` p ON i.product_id = p.product_id
        GROUP BY p.category
        """
        
        self._execute_query(query.format(project_id=self.project_id, dataset_id=self.dataset_id))
    
    def _create_fulfillment_analytics_mart(self):

        query = """
        CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.mart_fulfillment_analytics` AS
        SELECT 
            DATE_TRUNC(o.order_date, MONTH) as month,
            p.category,
            COUNT(DISTINCT o.order_id) as total_orders,
            AVG(o.lead_time_days) as avg_lead_time,
            AVG(o.order_cycle_time_days) as avg_cycle_time,
            SUM(o.sales) as total_sales,
            COUNT(DISTINCT o.customer_id) as unique_customers
        FROM `{project_id}.{dataset_id}.fact_orders` o
        LEFT JOIN `{project_id}.{dataset_id}.dim_products` p ON o.product_id = p.product_id
        GROUP BY DATE_TRUNC(o.order_date, MONTH), p.category
        """
        
        self._execute_query(query.format(project_id=self.project_id, dataset_id=self.dataset_id))
    
    def _create_category_performance_mart(self):

        query = """
        CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.mart_category_performance` AS
        SELECT 
            p.category,
            COUNT(DISTINCT o.order_id) as total_orders,
            SUM(o.sales) as total_sales,
            SUM(o.profit) as total_profit,
            AVG(o.lead_time_days) as avg_lead_time,
            AVG(i.fill_rate) as avg_fill_rate,
            AVG(i.annualized_turnover) as avg_turnover,
            COUNT(DISTINCT r.return_id) as total_returns
        FROM `{project_id}.{dataset_id}.fact_orders` o
        LEFT JOIN `{project_id}.{dataset_id}.dim_products` p ON o.product_id = p.product_id
        LEFT JOIN `{project_id}.{dataset_id}.fact_returns` r ON o.order_id = r.order_id
        LEFT JOIN (
            SELECT product_id, AVG(fill_rate) as fill_rate, AVG(annualized_turnover) as annualized_turnover
            FROM `{project_id}.{dataset_id}.fact_inventory`
            GROUP BY product_id
        ) i ON o.product_id = i.product_id
        GROUP BY p.category
        """
        
        self._execute_query(query.format(project_id=self.project_id, dataset_id=self.dataset_id))
    
    def _execute_query(self, query):

        try:
            job = self.client.query(query)
            job.result()
            self.logger.info("Query executed successfully")
        except Exception as e:
            self.logger.error(f"Error executing query: {str(e)}") 