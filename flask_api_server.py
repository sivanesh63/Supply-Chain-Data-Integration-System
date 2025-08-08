#!/usr/bin/env python3
"""
Custom Flask API Server for Supply Chain Analytics
Serves data from test.xlsx with proper JSON serialization
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
import numpy as np
from datetime import datetime
import json
from config import EXCEL_FILE_PATH

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

class DataService:
    """Service class for handling data operations"""
    
    def __init__(self):
        self.excel_file = EXCEL_FILE_PATH
        self._load_data()
    
    def _load_data(self):
        """Load all data from Excel file"""
        try:
            # Load orders data (from train sheet)
            self.orders_df = pd.read_excel(self.excel_file, sheet_name='train')
            
            # Load returns data (from Return sheet)
            self.returns_df = pd.read_excel(self.excel_file, sheet_name='Return')
            
            # Load people data (from People sheet)
            self.people_df = pd.read_excel(self.excel_file, sheet_name='People')
            
            # Transform data
            self._transform_data()
            
        except Exception as e:
            print(f"Error loading data: {str(e)}")
            raise
    
    def _transform_data(self):
        """Transform data for API consumption"""
        # Transform orders data
        if 'Order Date' in self.orders_df.columns:
            self.orders_df['Order Date'] = pd.to_datetime(self.orders_df['Order Date'])
        if 'Ship Date' in self.orders_df.columns:
            self.orders_df['Ship Date'] = pd.to_datetime(self.orders_df['Ship Date'])
        
        # Transform returns data
        if 'Order Date' in self.returns_df.columns:
            self.returns_df['Order Date'] = pd.to_datetime(self.returns_df['Order Date'])
        
        # Transform people data
        if 'First Order Date' in self.people_df.columns:
            self.people_df['First Order Date'] = pd.to_datetime(self.people_df['First Order Date'])
        if 'Last Order Date' in self.people_df.columns:
            self.people_df['Last Order Date'] = pd.to_datetime(self.people_df['Last Order Date'])
    
    def _clean_dataframe_for_json(self, df):
        """Clean DataFrame for JSON serialization"""
        # Convert datetime columns to strings
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.strftime('%Y-%m-%d')
        
        # Handle NaN values
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.fillna('')
        
        # Convert to records
        return df.to_dict('records')
    
    def get_orders(self, limit=None, offset=None):
        """Get orders data with optional pagination"""
        df = self.orders_df.copy()
        
        if offset:
            df = df.iloc[offset:]
        if limit:
            df = df.head(limit)
        
        return self._clean_dataframe_for_json(df)
    
    def get_returns(self, limit=None, offset=None):
        """Get returns data with optional pagination"""
        df = self.returns_df.copy()
        
        if offset:
            df = df.iloc[offset:]
        if limit:
            df = df.head(limit)
        
        return self._clean_dataframe_for_json(df)
    
    def get_people(self, limit=None, offset=None):
        """Get people data with optional pagination"""
        df = self.people_df.copy()
        
        if offset:
            df = df.iloc[offset:]
        if limit:
            df = df.head(limit)
        
        return self._clean_dataframe_for_json(df)
    
    def get_analytics(self):
        """Get analytics summary"""
        try:
            # Calculate basic analytics
            total_orders = len(self.orders_df)
            total_returns = len(self.returns_df)
            total_customers = len(self.people_df)
            
            # Sales analytics
            total_sales = self.orders_df['Sales'].sum() if 'Sales' in self.orders_df.columns else 0
            avg_order_value = total_sales / total_orders if total_orders > 0 else 0
            
            # Regional analytics
            if 'Region' in self.orders_df.columns:
                regional_sales = self.orders_df.groupby('Region')['Sales'].sum().to_dict()
            else:
                regional_sales = {}
            
            # Category analytics
            if 'Category' in self.orders_df.columns:
                category_sales = self.orders_df.groupby('Category')['Sales'].sum().to_dict()
            else:
                category_sales = {}
            
            # Time-based analytics
            if 'Order Date' in self.orders_df.columns:
                monthly_sales = self.orders_df.groupby(
                    self.orders_df['Order Date'].dt.to_period('M')
                )['Sales'].sum().to_dict()
            else:
                monthly_sales = {}
            
            return {
                "summary": {
                    "total_orders": int(total_orders),
                    "total_returns": int(total_returns),
                    "total_customers": int(total_customers),
                    "total_sales": float(total_sales),
                    "avg_order_value": float(avg_order_value)
                },
                "regional_sales": {str(k): float(v) for k, v in regional_sales.items()},
                "category_sales": {str(k): float(v) for k, v in category_sales.items()},
                "monthly_sales": {str(k): float(v) for k, v in monthly_sales.items()}
            }
            
        except Exception as e:
            return {"error": str(e)}

# Initialize data service
data_service = DataService()

@app.route('/')
def root():
    """Root endpoint"""
    return jsonify({
        "message": "Supply Chain Analytics API",
        "version": "1.0.0",
        "endpoints": [
            "/api/orders",
            "/api/returns", 
            "/api/people",
            "/api/analytics"
        ]
    })

@app.route('/api/orders')
def get_orders():
    """Get orders data"""
    try:
        limit = request.args.get('limit', type=int)
        offset = request.args.get('offset', type=int)
        
        orders = data_service.get_orders(limit=limit, offset=offset)
        return jsonify({
            "status": "success",
            "data": orders,
            "count": len(orders)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/returns')
def get_returns():
    """Get returns data"""
    try:
        limit = request.args.get('limit', type=int)
        offset = request.args.get('offset', type=int)
        
        returns = data_service.get_returns(limit=limit, offset=offset)
        return jsonify({
            "status": "success",
            "data": returns,
            "count": len(returns)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/people')
def get_people():
    """Get people data"""
    try:
        limit = request.args.get('limit', type=int)
        offset = request.args.get('offset', type=int)
        
        people = data_service.get_people(limit=limit, offset=offset)
        return jsonify({
            "status": "success",
            "data": people,
            "count": len(people)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/analytics')
def get_analytics():
    """Get analytics summary"""
    try:
        analytics = data_service.get_analytics()
        return jsonify({
            "status": "success",
            "data": analytics
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "data_source": EXCEL_FILE_PATH
    })

if __name__ == "__main__":
    print("🚀 Starting Flask API Server...")
    print("API will be available at: http://localhost:5000")
    print("Health Check: http://localhost:5000/api/health")
    print("\nAvailable endpoints:")
    print("- GET /api/orders - Get orders data")
    print("- GET /api/returns - Get returns data") 
    print("- GET /api/people - Get people data")
    print("- GET /api/analytics - Get analytics summary")
    print("\nPress Ctrl+C to stop the server")
    
    app.run(host="0.0.0.0", port=5000, debug=True) 