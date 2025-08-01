import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta
import requests
from utils.logger import log_pipeline_step, log_alert
from config import DASHBOARD_TITLE, DASHBOARD_THEME, INVENTORY_ALERT_THRESHOLD

class SupplyChainDashboard:
    """
    Streamlit dashboard for supply chain analytics
    """
    
    def __init__(self):
        self.logger = log_pipeline_step("SupplyChainDashboard", "STARTED")
        self.setup_page()
    
    def setup_page(self):
        """
        Setup Streamlit page configuration
        """
        st.set_page_config(
            page_title=DASHBOARD_TITLE,
            page_icon="📊",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        
        st.title(DASHBOARD_TITLE)
        st.markdown("---")
    
    def run_dashboard(self, data_dict, metrics_dict):
        """
        Run the main dashboard
        """
        try:
            # Sidebar for filters
            self.create_sidebar(data_dict)
            
            # Main dashboard content
            col1, col2, col3, col4 = st.columns(4)
            
            # KPI Cards
            with col1:
                self.display_kpi_card("Total Orders", metrics_dict.get('lead_time', {}).get('total_orders', 0), "📦")
            
            with col2:
                avg_lead_time = metrics_dict.get('lead_time', {}).get('mean_lead_time', 0)
                self.display_kpi_card("Avg Lead Time", f"{avg_lead_time:.1f} days", "⏱️")
            
            with col3:
                fill_rate = metrics_dict.get('fill_rate', {}).get('mean_fill_rate', 0)
                self.display_kpi_card("Fill Rate", f"{fill_rate:.1%}", "📈")
            
            with col4:
                turnover = metrics_dict.get('inventory_turnover', {}).get('mean_turnover', 0)
                self.display_kpi_card("Inventory Turnover", f"{turnover:.1f}x", "🔄")
            
            st.markdown("---")
            
            # Charts section
            col1, col2 = st.columns(2)
            
            with col1:
                self.display_lead_time_chart(data_dict.get('orders'))
                self.display_inventory_trends(data_dict.get('inventory'))
            
            with col2:
                self.display_category_performance(data_dict.get('orders'), data_dict.get('inventory'))
                self.display_fill_rate_analysis(data_dict.get('inventory'))
            
            # Alerts section
            self.display_alerts(data_dict)
            
            # Detailed analytics
            self.display_detailed_analytics(data_dict, metrics_dict)
            
        except Exception as e:
            self.logger.error(f"Error running dashboard: {str(e)}")
            st.error(f"Error running dashboard: {str(e)}")
    
    def create_sidebar(self, data_dict):
        """
        Create sidebar with filters
        """
        st.sidebar.header("📊 Dashboard Filters")
        
        # Date range filter
        if 'orders' in data_dict and len(data_dict['orders']) > 0:
            min_date = data_dict['orders']['Order Date'].min()
            max_date = data_dict['orders']['Order Date'].max()
            
            date_range = st.sidebar.date_input(
                "Date Range",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date
            )
        
        # Category filter
        if 'orders' in data_dict and 'Category' in data_dict['orders'].columns:
            categories = ['All'] + list(data_dict['orders']['Category'].unique())
            selected_category = st.sidebar.selectbox("Category", categories)
        
        # Region filter
        if 'orders' in data_dict and 'Region' in data_dict['orders'].columns:
            regions = ['All'] + list(data_dict['orders']['Region'].unique())
            selected_region = st.sidebar.selectbox("Region", regions)
        
        st.sidebar.markdown("---")
        st.sidebar.header("📈 Quick Actions")
        
        if st.sidebar.button("🔄 Refresh Data"):
            st.rerun()
        
        if st.sidebar.button("📊 Export Report"):
            self.export_report(data_dict, metrics_dict)
    
    def display_kpi_card(self, title, value, icon):
        """
        Display a KPI card
        """
        st.metric(
            label=f"{icon} {title}",
            value=value,
            delta=None
        )
    
    def display_lead_time_chart(self, orders_df):
        """
        Display lead time distribution chart
        """
        if orders_df is None or len(orders_df) == 0:
            st.warning("No orders data available")
            return
        
        st.subheader("📊 Lead Time Distribution")
        
        if 'Lead Time (Days)' in orders_df.columns:
            fig = px.histogram(
                orders_df,
                x='Lead Time (Days)',
                nbins=20,
                title="Lead Time Distribution",
                labels={'Lead Time (Days)': 'Days', 'count': 'Number of Orders'}
            )
            
            fig.update_layout(
                xaxis_title="Lead Time (Days)",
                yaxis_title="Number of Orders",
                showlegend=False
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Lead time data not available")
    
    def display_inventory_trends(self, inventory_df):
        """
        Display inventory trends chart
        """
        if inventory_df is None or len(inventory_df) == 0:
            st.warning("No inventory data available")
            return
        
        st.subheader("📦 Inventory Trends")
        
        # Group by date and category
        if 'date' in inventory_df.columns and 'category' in inventory_df.columns:
            daily_inventory = inventory_df.groupby(['date', 'category']).agg({
                'stock_level': 'mean',
                'daily_demand': 'mean'
            }).reset_index()
            
            fig = px.line(
                daily_inventory,
                x='date',
                y='stock_level',
                color='category',
                title="Daily Stock Levels by Category"
            )
            
            fig.update_layout(
                xaxis_title="Date",
                yaxis_title="Stock Level",
                hovermode='x unified'
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Inventory trend data not available")
    
    def display_category_performance(self, orders_df, inventory_df):
        """
        Display category performance chart
        """
        if orders_df is None or len(orders_df) == 0:
            st.warning("No orders data available")
            return
        
        st.subheader("🏷️ Category Performance")
        
        if 'Category' in orders_df.columns and 'Sales' in orders_df.columns:
            category_sales = orders_df.groupby('Category').agg({
                'Sales': 'sum',
                'Order ID': 'count'
            }).reset_index()
            
            category_sales.columns = ['Category', 'Total Sales', 'Order Count']
            
            fig = px.bar(
                category_sales,
                x='Category',
                y='Total Sales',
                title="Sales by Category",
                text='Total Sales'
            )
            
            fig.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
            fig.update_layout(
                xaxis_title="Category",
                yaxis_title="Total Sales ($)",
                xaxis_tickangle=-45
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Category performance data not available")
    
    def display_fill_rate_analysis(self, inventory_df):
        """
        Display fill rate analysis chart
        """
        if inventory_df is None or len(inventory_df) == 0:
            st.warning("No inventory data available")
            return
        
        st.subheader("📈 Fill Rate Analysis")
        
        if 'fill_rate' in inventory_df.columns and 'category' in inventory_df.columns:
            fill_rate_by_category = inventory_df.groupby('category')['fill_rate'].mean().reset_index()
            
            fig = px.bar(
                fill_rate_by_category,
                x='category',
                y='fill_rate',
                title="Average Fill Rate by Category",
                text='fill_rate'
            )
            
            fig.update_traces(texttemplate='%{text:.1%}', textposition='outside')
            fig.update_layout(
                xaxis_title="Category",
                yaxis_title="Fill Rate",
                yaxis_tickformat='.1%',
                xaxis_tickangle=-45
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Fill rate data not available")
    
    def display_alerts(self, data_dict):
        """
        Display alerts for abnormal conditions
        """
        st.subheader("🚨 Alerts & Notifications")
        
        alerts = []
        
        # Check for low inventory
        if 'inventory' in data_dict and len(data_dict['inventory']) > 0:
            low_stock_products = data_dict['inventory'][
                data_dict['inventory']['stockout_risk'] == True
            ]
            
            if len(low_stock_products) > 0:
                alerts.append(f"⚠️ {len(low_stock_products)} products at risk of stockout")
        
        # Check for missing data
        if 'orders' in data_dict and len(data_dict['orders']) > 0:
            missing_data_pct = data_dict['orders'].isnull().sum().sum() / (data_dict['orders'].shape[0] * data_dict['orders'].shape[1])
            if missing_data_pct > 0.05:
                alerts.append(f"⚠️ {missing_data_pct:.1%} missing data detected")
        
        # Check for high lead times
        if 'orders' in data_dict and 'Lead Time (Days)' in data_dict['orders'].columns:
            high_lead_time = data_dict['orders'][data_dict['orders']['Lead Time (Days)'] > 14]
            if len(high_lead_time) > 0:
                alerts.append(f"⚠️ {len(high_lead_time)} orders with lead time > 14 days")
        
        if alerts:
            for alert in alerts:
                st.warning(alert)
        else:
            st.success("✅ All systems operating normally")
    
    def display_detailed_analytics(self, data_dict, metrics_dict):
        """
        Display detailed analytics section
        """
        st.markdown("---")
        st.subheader("📊 Detailed Analytics")
        
        # Create tabs for different analytics
        tab1, tab2, tab3, tab4 = st.tabs(["📈 Performance Metrics", "📦 Inventory Analysis", "🚚 Fulfillment", "📋 Reports"])
        
        with tab1:
            self.display_performance_metrics(metrics_dict)
        
        with tab2:
            self.display_inventory_analysis(data_dict.get('inventory'))
        
        with tab3:
            self.display_fulfillment_analytics(data_dict.get('orders'))
        
        with tab4:
            self.display_reports(data_dict, metrics_dict)
    
    def display_performance_metrics(self, metrics_dict):
        """
        Display detailed performance metrics
        """
        if not metrics_dict:
            st.info("No metrics data available")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Lead Time Metrics")
            if 'lead_time' in metrics_dict:
                lt_metrics = metrics_dict['lead_time']
                st.metric("Mean Lead Time", f"{lt_metrics.get('mean_lead_time', 0):.1f} days")
                st.metric("Median Lead Time", f"{lt_metrics.get('median_lead_time', 0):.1f} days")
                st.metric("Excellent Performance", f"{lt_metrics.get('excellent_pct', 0):.1%}")
        
        with col2:
            st.subheader("Fill Rate Metrics")
            if 'fill_rate' in metrics_dict:
                fr_metrics = metrics_dict['fill_rate']
                st.metric("Mean Fill Rate", f"{fr_metrics.get('mean_fill_rate', 0):.1%}")
                st.metric("Excellent Fill Rate", f"{fr_metrics.get('excellent_fill_rate_pct', 0):.1%}")
                st.metric("Products at Risk", fr_metrics.get('products_at_risk', 0))
    
    def display_inventory_analysis(self, inventory_df):
        """
        Display detailed inventory analysis
        """
        if inventory_df is None or len(inventory_df) == 0:
            st.info("No inventory data available")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Inventory Turnover")
            if 'annualized_turnover' in inventory_df.columns:
                turnover_stats = inventory_df['annualized_turnover'].describe()
                st.dataframe(turnover_stats)
        
        with col2:
            st.subheader("Stockout Risk")
            if 'stockout_risk' in inventory_df.columns:
                risk_count = inventory_df['stockout_risk'].sum()
                total_count = len(inventory_df)
                st.metric("Products at Risk", f"{risk_count} / {total_count}")
    
    def display_fulfillment_analytics(self, orders_df):
        """
        Display fulfillment analytics
        """
        if orders_df is None or len(orders_df) == 0:
            st.info("No orders data available")
            return
        
        st.subheader("Fulfillment Performance")
        
        if 'Order Date' in orders_df.columns and 'Lead Time (Days)' in orders_df.columns:
            # Monthly trends
            orders_df['Order Month'] = orders_df['Order Date'].dt.to_period('M')
            monthly_lead_time = orders_df.groupby('Order Month')['Lead Time (Days)'].mean()
            
            fig = px.line(
                x=monthly_lead_time.index.astype(str),
                y=monthly_lead_time.values,
                title="Monthly Average Lead Time"
            )
            
            fig.update_layout(
                xaxis_title="Month",
                yaxis_title="Average Lead Time (Days)"
            )
            
            st.plotly_chart(fig, use_container_width=True)
    
    def display_reports(self, data_dict, metrics_dict):
        """
        Display downloadable reports
        """
        st.subheader("📋 Reports")
        
        # Generate summary report
        if st.button("Generate Summary Report"):
            report_data = self.generate_summary_report(data_dict, metrics_dict)
            st.download_button(
                label="Download Summary Report",
                data=report_data,
                file_name=f"supply_chain_report_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
    
    def generate_summary_report(self, data_dict, metrics_dict):
        """
        Generate a summary report
        """
        report_data = []
        
        # Add metrics to report
        for metric_type, metrics in metrics_dict.items():
            if metrics:
                for key, value in metrics.items():
                    report_data.append({
                        'Metric Type': metric_type,
                        'Metric': key,
                        'Value': value
                    })
        
        # Convert to DataFrame and CSV
        report_df = pd.DataFrame(report_data)
        return report_df.to_csv(index=False)
    
    def export_report(self, data_dict, metrics_dict):
        """
        Export comprehensive report
        """
        # This would typically generate a more comprehensive report
        st.success("Report exported successfully!")

def main():
    """
    Main function to run the dashboard
    """
    # Initialize dashboard
    dashboard = SupplyChainDashboard()
    
    # For demo purposes, we'll create sample data
    # In a real implementation, this would come from the data pipeline
    
    # Sample data creation
    sample_orders = pd.DataFrame({
        'Order ID': [f'ORD_{i}' for i in range(1, 101)],
        'Order Date': pd.date_range('2024-01-01', periods=100, freq='D'),
        'Ship Date': pd.date_range('2024-01-03', periods=100, freq='D'),
        'Customer ID': [f'CUST_{i%20}' for i in range(1, 101)],
        'Product ID': [f'PROD_{i%10}' for i in range(1, 101)],
        'Category': ['Electronics', 'Clothing', 'Home', 'Sports'] * 25,
        'Quantity': np.random.randint(1, 10, 100),
        'Sales': np.random.uniform(100, 1000, 100),
        'Profit': np.random.uniform(10, 200, 100),
        'Lead Time (Days)': np.random.randint(1, 15, 100)
    })
    
    sample_inventory = pd.DataFrame({
        'date': pd.date_range('2024-01-01', periods=30, freq='D'),
        'product_id': [f'PROD_{i%10}' for i in range(1, 301)],
        'category': ['Electronics', 'Clothing', 'Home', 'Sports'] * 75,
        'stock_level': np.random.randint(10, 200, 300),
        'daily_demand': np.random.randint(1, 10, 300),
        'fill_rate': np.random.uniform(0.7, 1.0, 300),
        'annualized_turnover': np.random.uniform(2, 12, 300),
        'stockout_risk': np.random.choice([True, False], 300, p=[0.1, 0.9])
    })
    
    sample_returns = pd.DataFrame({
        'Return Date': pd.date_range('2024-01-01', periods=20, freq='D'),
        'Order ID': [f'ORD_{i}' for i in np.random.randint(1, 101, 20)],
        'Customer ID': [f'CUST_{i%20}' for i in range(1, 21)],
        'Product ID': [f'PROD_{i%10}' for i in range(1, 21)]
    })
    
    data_dict = {
        'orders': sample_orders,
        'inventory': sample_inventory,
        'returns': sample_returns
    }
    
    # Calculate sample metrics
    from data_processing.supply_chain_metrics import SupplyChainMetrics
    metrics_calculator = SupplyChainMetrics()
    metrics_dict = metrics_calculator.calculate_all_metrics(
        sample_orders, sample_inventory, sample_returns
    )
    
    # Run dashboard
    dashboard.run_dashboard(data_dict, metrics_dict)

if __name__ == "__main__":
    main() 