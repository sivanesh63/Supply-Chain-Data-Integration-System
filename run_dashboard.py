#!/usr/bin/env python3
"""
Simple script to run the Streamlit dashboard
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os

def main():
    """
    Run the Streamlit dashboard with sample data
    """
    # Page configuration
    st.set_page_config(
        page_title="Supply Chain Analytics Dashboard",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("📊 Supply Chain Analytics Dashboard")
    st.markdown("---")
    
    # Create sample data
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
        'Lead Time (Days)': np.random.randint(1, 15, 100)
    })
    
    # Create inventory data with consistent lengths
    num_inventory_records = 300
    sample_inventory = pd.DataFrame({
        'date': pd.date_range('2024-01-01', periods=30, freq='D').repeat(num_inventory_records // 30),
        'product_id': [f'PROD_{i%10:02d}' for i in range(1, num_inventory_records + 1)],
        'category': ['Electronics', 'Clothing', 'Home', 'Sports'] * (num_inventory_records // 4),
        'stock_level': np.random.randint(10, 200, num_inventory_records),
        'daily_demand': np.random.randint(1, 10, num_inventory_records),
        'fill_rate': np.random.uniform(0.7, 1.0, num_inventory_records),
        'annualized_turnover': np.random.uniform(2, 12, num_inventory_records),
        'stockout_risk': np.random.choice([True, False], num_inventory_records, p=[0.1, 0.9])
    })
    
    # KPI Cards
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="📦 Total Orders",
            value=len(sample_orders),
            delta=None
        )
    
    with col2:
        avg_lead_time = sample_orders['Lead Time (Days)'].mean()
        st.metric(
            label="⏱️ Avg Lead Time",
            value=f"{avg_lead_time:.1f} days",
            delta=None
        )
    
    with col3:
        fill_rate = sample_inventory['fill_rate'].mean()
        st.metric(
            label="📈 Fill Rate",
            value=f"{fill_rate:.1%}",
            delta=None
        )
    
    with col4:
        turnover = sample_inventory['annualized_turnover'].mean()
        st.metric(
            label="🔄 Inventory Turnover",
            value=f"{turnover:.1f}x",
            delta=None
        )
    
    st.markdown("---")
    
    # Charts section
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Lead Time Distribution")
        fig = px.histogram(
            sample_orders,
            x='Lead Time (Days)',
            nbins=20,
            title="Lead Time Distribution"
        )
        fig.update_layout(
            xaxis_title="Lead Time (Days)",
            yaxis_title="Number of Orders",
            showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("📦 Inventory Trends")
        daily_inventory = sample_inventory.groupby(['date', 'category']).agg({
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
    
    with col2:
        st.subheader("🏷️ Category Performance")
        category_sales = sample_orders.groupby('Category').agg({
            'Sales': 'sum',
            'Order ID': 'count'
        }).reset_index()
        
        fig = px.bar(
            category_sales,
            x='Category',
            y='Sales',
            title="Sales by Category",
            text='Sales'
        )
        fig.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
        fig.update_layout(
            xaxis_title="Category",
            yaxis_title="Total Sales ($)",
            xaxis_tickangle=-45
        )
        st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("📈 Fill Rate Analysis")
        fill_rate_by_category = sample_inventory.groupby('category')['fill_rate'].mean().reset_index()
        
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
    
    # Alerts section
    st.subheader("🚨 Alerts & Notifications")
    
    alerts = []
    
    # Check for low inventory
    low_stock_products = sample_inventory[sample_inventory['stockout_risk'] == True]
    if len(low_stock_products) > 0:
        alerts.append(f"⚠️ {len(low_stock_products)} products at risk of stockout")
    
    # Check for high lead times
    high_lead_time = sample_orders[sample_orders['Lead Time (Days)'] > 14]
    if len(high_lead_time) > 0:
        alerts.append(f"⚠️ {len(high_lead_time)} orders with lead time > 14 days")
    
    if alerts:
        for alert in alerts:
            st.warning(alert)
    else:
        st.success("✅ All systems operating normally")
    
    # Detailed analytics
    st.markdown("---")
    st.subheader("📊 Detailed Analytics")
    
    tab1, tab2, tab3 = st.tabs(["📈 Performance Metrics", "📦 Inventory Analysis", "🚚 Fulfillment"])
    
    with tab1:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Lead Time Metrics")
            st.metric("Mean Lead Time", f"{sample_orders['Lead Time (Days)'].mean():.1f} days")
            st.metric("Median Lead Time", f"{sample_orders['Lead Time (Days)'].median():.1f} days")
            st.metric("Standard Deviation", f"{sample_orders['Lead Time (Days)'].std():.1f} days")
        
        with col2:
            st.subheader("Fill Rate Metrics")
            st.metric("Mean Fill Rate", f"{sample_inventory['fill_rate'].mean():.1%}")
            st.metric("Products at Risk", sample_inventory['stockout_risk'].sum())
            st.metric("Total Products", len(sample_inventory))
    
    with tab2:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Inventory Turnover")
            turnover_stats = sample_inventory['annualized_turnover'].describe()
            st.dataframe(turnover_stats)
        
        with col2:
            st.subheader("Stockout Risk")
            risk_count = sample_inventory['stockout_risk'].sum()
            total_count = len(sample_inventory)
            st.metric("Products at Risk", f"{risk_count} / {total_count}")
    
    with tab3:
        st.subheader("Fulfillment Performance")
        
        # Monthly trends
        sample_orders['Order Month'] = sample_orders['Order Date'].dt.to_period('M')
        monthly_lead_time = sample_orders.groupby('Order Month')['Lead Time (Days)'].mean()
        
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
    
    # Sidebar
    st.sidebar.header("📊 Dashboard Filters")
    
    # Date range filter
    min_date = sample_orders['Order Date'].min()
    max_date = sample_orders['Order Date'].max()
    
    date_range = st.sidebar.date_input(
        "Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )
    
    # Category filter
    categories = ['All'] + list(sample_orders['Category'].unique())
    selected_category = st.sidebar.selectbox("Category", categories)
    
    st.sidebar.markdown("---")
    st.sidebar.header("📈 Quick Actions")
    
    if st.sidebar.button("🔄 Refresh Data"):
        st.rerun()
    
    if st.sidebar.button("📊 Export Report"):
        # Create a comprehensive report
        report_data = {
            'summary': {
                'total_orders': len(sample_orders),
                'avg_lead_time': sample_orders['Lead Time (Days)'].mean(),
                'fill_rate': sample_inventory['fill_rate'].mean(),
                'inventory_turnover': sample_inventory['annualized_turnover'].mean(),
                'products_at_risk': sample_inventory['stockout_risk'].sum()
            },
            'orders_data': sample_orders,
            'inventory_data': sample_inventory
        }
        
        # Create report filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"supply_chain_report_{timestamp}.xlsx"
        
        # Save to Excel file
        with pd.ExcelWriter(report_filename, engine='openpyxl') as writer:
            # Summary sheet
            summary_df = pd.DataFrame([report_data['summary']])
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Orders data
            report_data['orders_data'].to_excel(writer, sheet_name='Orders', index=False)
            
            # Inventory data
            report_data['inventory_data'].to_excel(writer, sheet_name='Inventory', index=False)
        
        st.success(f"📊 Report exported successfully to: {report_filename}")
        st.info(f"📁 File saved in: {os.getcwd()}")

if __name__ == "__main__":
    main() 