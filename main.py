# ============================================================================
# LIR REPORT API - Built with FastAPI
# Purpose: Serve LIR Report data from BigQuery Gold Layer
# ============================================================================

import os
import json
from typing import Optional, List, Dict, Any
from datetime import datetime
from functools import lru_cache

# FastAPI imports
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import uvicorn

# Google Cloud imports
from google.cloud import bigquery
from google.cloud import secretmanager

# Data processing
import pandas as pd
from pydantic import BaseModel

# ============================================================================
# CONFIGURATION
# ============================================================================

@lru_cache()
def get_settings():
    """Load configuration from environment variables"""
    return {
        "project_id": os.getenv("GCP_PROJECT_ID", "scmplanningdev"),
        "dataset_id": os.getenv("DATASET_ID", "gold_layer"),
        "table_name": os.getenv("TABLE_NAME", "lir_report_gold"),
        "environment": os.getenv("ENVIRONMENT", "development"),
        "port": int(os.getenv("PORT", 8080))
    }

# ============================================================================
# PYDANTIC MODELS (Data Structure Definitions)
# ============================================================================

class LIRPartResponse(BaseModel):
    """Response model for a single part's LIR data"""
    part_code: str
    part_description: Optional[str]
    supplier_id: Optional[str]
    lead_time_days: Optional[int]
    current_stock: Optional[float]
    safety_stock: Optional[float]
    forecast_demand: Optional[float]
    abc_category: Optional[str]
    data_quality_score: Optional[float]
    
    class Config:
        json_schema_extra = {
            "example": {
                "part_code": "PT001",
                "part_description": "Bearing Assembly",
                "supplier_id": "SUP001",
                "lead_time_days": 14,
                "current_stock": 500.0,
                "safety_stock": 150.0,
                "forecast_demand": 45.0,
                "abc_category": "A",
                "data_quality_score": 95.5
            }
        }

class LIRSummaryResponse(BaseModel):
    """Summary statistics for dashboard"""
    total_parts: int
    total_inventory_value: float
    average_lead_time: float
    parts_with_low_stock: int
    forecast_accuracy: float
    last_updated: datetime

class FilterRequest(BaseModel):
    """Request body for filtering"""
    part_code: Optional[str] = None
    supplier_id: Optional[str] = None
    abc_category: Optional[str] = None
    lead_time_max: Optional[int] = None
    in_stock: Optional[bool] = None
    limit: int = 100

# ============================================================================
# BIGQUERY CLIENT
# ============================================================================

class BigQueryClient:
    """Wrapper for BigQuery operations"""
    
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
        
    def query(self, sql: str) -> pd.DataFrame:
        """Execute query and return DataFrame"""
        try:
            job_config = bigquery.QueryJobConfig()
            query_job = self.client.query(sql, job_config=job_config)
            return query_job.to_dataframe()
        except Exception as e:
            print(f"❌ BigQuery Error: {str(e)}")
            raise
    
    def get_table_info(self, dataset_id: str, table_name: str):
        """Get table schema and row count"""
        try:
            table_id = f"{self.project_id}.{dataset_id}.{table_name}"
            table = self.client.get_table(table_id)
            return {
                "total_rows": table.num_rows,
                "columns": [field.name for field in table.schema],
                "created": table.created.isoformat()
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to get table info: {str(e)}")

# ============================================================================
# INITIALIZE FASTAPI APP
# ============================================================================

app = FastAPI(
    title="LIR Report API",
    description="API for Leading Indicator Report data from BigQuery Gold Layer",
    version="1.0.0"
)

# Add CORS middleware (allows requests from frontend)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize clients
settings = get_settings()
bq_client = BigQueryClient(settings["project_id"])

print(f"✅ API initialized for project: {settings['project_id']}")
print(f"✅ Using dataset: {settings['dataset_id']}")
print(f"✅ Using table: {settings['table_name']}")

# ============================================================================
# HEALTH CHECK ENDPOINT
# ============================================================================

@app.get("/health")
async def health_check():
    """Check if API is running and BigQuery is accessible"""
    try:
        # Test BigQuery connection
        test_query = f"""
        SELECT COUNT(*) as row_count 
        FROM `{settings['project_id']}.{settings['dataset_id']}.{settings['table_name']}`
        LIMIT 1
        """
        result = bq_client.query(test_query)
        
        return {
            "status": "✅ healthy",
            "timestamp": datetime.now().isoformat(),
            "bigquery": "✅ connected",
            "environment": settings["environment"],
            "project": settings["project_id"]
        }
    except Exception as e:
        return {
            "status": "❌ unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

# ============================================================================
# ENDPOINT 1: Get All LIR Report Data with Filters
# ============================================================================

@app.get("/api/lir-report", response_model=List[Dict[str, Any]])
async def get_lir_report(
    part_code: Optional[str] = Query(None, description="Filter by part code"),
    supplier_id: Optional[str] = Query(None, description="Filter by supplier"),
    abc_category: Optional[str] = Query(None, description="Filter by ABC category (A/B/C)"),
    limit: int = Query(100, description="Number of records to return", le=1000)
):
    """
    Get LIR report data with optional filters
    
    Example:
    - GET /api/lir-report
    - GET /api/lir-report?part_code=PT001&limit=50
    - GET /api/lir-report?supplier_id=SUP001&abc_category=A
    """
    try:
        # Build WHERE clause
        where_conditions = []
        
        if part_code:
            where_conditions.append(f"Code LIKE '%{part_code}%'")
        if supplier_id:
            where_conditions.append(f"supplier_id = '{supplier_id}'")
        if abc_category:
            where_conditions.append(f"ABC_Category = '{abc_category}'")
        
        where_clause = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Build query
        query = f"""
        SELECT
            Code as part_code,
            DESCRIPTION as part_description,
            supplier_id,
            lead_time_days,
            current_stock,
            safety_stock,
            forecast_demand,
            ABC_Category as abc_category,
            data_quality_score,
            gold_date,
            created_date
        FROM `{settings['project_id']}.{settings['dataset_id']}.{settings['table_name']}`
        {where_clause}
        ORDER BY Code ASC
        LIMIT {limit}
        """
        
        print(f"📊 Executing query: {query[:100]}...")
        df = bq_client.query(query)
        
        # Convert to list of dictionaries
        result = df.to_dict('records')
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")

# ============================================================================
# ENDPOINT 2: Get Single Part Detail
# ============================================================================

@app.get("/api/lir-report/{part_code}", response_model=Dict[str, Any])
async def get_part_detail(part_code: str):
    """
    Get detailed LIR data for a single part
    
    Example:
    - GET /api/lir-report/PT001
    """
    try:
        query = f"""
        SELECT
            Code as part_code,
            DESCRIPTION as part_description,
            BU,
            supplier_id,
            lead_time_days,
            current_stock,
            safety_stock,
            recommended_qty,
            forecast_demand,
            ABC_Category as abc_category,
            data_quality_score,
            UNIT_COGS,
            UNIT_ASP,
            total_buffer_cogs,
            gold_date,
            created_date,
            modified_date
        FROM `{settings['project_id']}.{settings['dataset_id']}.{settings['table_name']}`
        WHERE Code = '{part_code}'
        LIMIT 1
        """
        
        df = bq_client.query(query)
        
        if df.empty:
            raise HTTPException(status_code=404, detail=f"Part {part_code} not found")
        
        return df.to_dict('records')[0]
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")

# ============================================================================
# ENDPOINT 3: Get Summary Statistics
# ============================================================================

@app.get("/api/lir-report/summary/dashboard", response_model=Dict[str, Any])
async def get_summary():
    """
    Get summary statistics for dashboard
    
    Example:
    - GET /api/lir-report/summary/dashboard
    """
    try:
        query = f"""
        SELECT
            COUNT(*) as total_parts,
            COUNT(DISTINCT supplier_id) as total_suppliers,
            ROUND(SUM(total_buffer_cogs), 2) as total_inventory_value,
            ROUND(AVG(lead_time_days), 2) as average_lead_time,
            ROUND(AVG(data_quality_score), 2) as avg_data_quality,
            COUNTIF(current_stock < safety_stock) as parts_below_safety_stock,
            ROUND(AVG(CASE WHEN ABC_Category = 'A' THEN 1 ELSE 0 END) * 100, 2) as pct_category_a,
            ROUND(AVG(CASE WHEN ABC_Category = 'B' THEN 1 ELSE 0 END) * 100, 2) as pct_category_b,
            ROUND(AVG(CASE WHEN ABC_Category = 'C' THEN 1 ELSE 0 END) * 100, 2) as pct_category_c,
            MAX(gold_date) as last_updated
        FROM `{settings['project_id']}.{settings['dataset_id']}.{settings['table_name']}`
        """
        
        df = bq_client.query(query)
        return df.to_dict('records')[0]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")

# ============================================================================
# ENDPOINT 4: Export to CSV
# ============================================================================

@app.post("/api/lir-report/export")
async def export_report(
    format: str = Query("csv", regex="^(csv|json)$"),
    supplier_id: Optional[str] = Query(None)
):
    """
    Export LIR report to CSV or JSON
    
    Example:
    - POST /api/lir-report/export?format=csv
    - POST /api/lir-report/export?format=json&supplier_id=SUP001
    """
    try:
        where_clause = ""
        if supplier_id:
            where_clause = f"WHERE supplier_id = '{supplier_id}'"
        
        query = f"""
        SELECT *
        FROM `{settings['project_id']}.{settings['dataset_id']}.{settings['table_name']}`
        {where_clause}
        ORDER BY Code ASC
        """
        
        df = bq_client.query(query)
        
        if format == "csv":
            # Return CSV file
            csv_buffer = df.to_csv(index=False)
            return StreamingResponse(
                iter([csv_buffer]),
                media_type="text/csv",
                headers={"Content-Disposition": "attachment; filename=lir_report.csv"}
            )
        else:
            # Return JSON
            return df.to_dict('records')
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")

# ============================================================================
# ENDPOINT 5: Get ABC Analysis
# ============================================================================

@app.get("/api/lir-report/analysis/abc")
async def get_abc_analysis():
    """
    Get ABC category distribution and analysis
    
    Example:
    - GET /api/lir-report/analysis/abc
    """
    try:
        query = f"""
        SELECT
            ABC_Category,
            COUNT(*) as part_count,
            ROUND(AVG(current_stock), 2) as avg_stock,
            ROUND(AVG(lead_time_days), 2) as avg_lead_time,
            ROUND(SUM(total_buffer_cogs), 2) as total_value,
            ROUND(AVG(data_quality_score), 2) as avg_quality
        FROM `{settings['project_id']}.{settings['dataset_id']}.{settings['table_name']}`
        GROUP BY ABC_Category
        ORDER BY ABC_Category
        """
        
        df = bq_client.query(query)
        return df.to_dict('records')
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")

# ============================================================================
# ENDPOINT 6: Get Low Stock Alerts
# ============================================================================

@app.get("/api/lir-report/alerts/low-stock")
async def get_low_stock_alerts(threshold: float = Query(1.0, description="Stock to Safety Stock ratio")):
    """
    Get parts with stock below safety stock level
    
    Example:
    - GET /api/lir-report/alerts/low-stock
    - GET /api/lir-report/alerts/low-stock?threshold=0.8
    """
    try:
        query = f"""
        SELECT
            Code as part_code,
            DESCRIPTION as part_description,
            supplier_id,
            current_stock,
            safety_stock,
            ROUND(current_stock / safety_stock, 2) as stock_to_safety_ratio,
            ABC_Category,
            lead_time_days,
            recommended_qty
        FROM `{settings['project_id']}.{settings['dataset_id']}.{settings['table_name']}`
        WHERE current_stock < (safety_stock * {threshold})
        ORDER BY stock_to_safety_ratio ASC
        LIMIT 100
        """
        
        df = bq_client.query(query)
        return df.to_dict('records')
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")

# ============================================================================
# ENDPOINT 7: Get Supplier Performance
# ============================================================================

@app.get("/api/lir-report/analysis/supplier-performance")
async def get_supplier_performance():
    """
    Get supplier performance metrics
    
    Example:
    - GET /api/lir-report/analysis/supplier-performance
    """
    try:
        query = f"""
        SELECT
            supplier_id,
            COUNT(*) as part_count,
            ROUND(AVG(lead_time_days), 2) as avg_lead_time,
            ROUND(AVG(data_quality_score), 2) as avg_quality,
            ROUND(SUM(current_stock), 2) as total_inventory,
            ROUND(AVG(CASE WHEN current_stock >= safety_stock THEN 1 ELSE 0 END) * 100, 2) as pct_above_safety_stock
        FROM `{settings['project_id']}.{settings['dataset_id']}.{settings['table_name']}`
        WHERE supplier_id IS NOT NULL
        GROUP BY supplier_id
        ORDER BY avg_quality DESC
        """
        
        df = bq_client.query(query)
        return df.to_dict('records')
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")

# ============================================================================
# ROOT ENDPOINT (API Documentation)
# ============================================================================

@app.get("/")
async def root():
    """Root endpoint - API documentation"""
    return {
        "name": "LIR Report API",
        "version": "1.0.0",
        "documentation": "https://your-api-url/docs",
        "endpoints": {
            "health": "GET /health",
            "get_report": "GET /api/lir-report?part_code=...&limit=100",
            "get_part": "GET /api/lir-report/{part_code}",
            "get_summary": "GET /api/lir-report/summary/dashboard",
            "export": "POST /api/lir-report/export?format=csv",
            "abc_analysis": "GET /api/lir-report/analysis/abc",
            "low_stock_alerts": "GET /api/lir-report/alerts/low-stock",
            "supplier_performance": "GET /api/lir-report/analysis/supplier-performance"
        }
    }

# ============================================================================
# ERROR HANDLERS
# ============================================================================

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler"""
    return {
        "error": str(exc),
        "timestamp": datetime.now().isoformat(),
        "path": str(request.url)
    }

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    port = settings["port"]
    print(f"🚀 Starting LIR Report API on port {port}...")
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=settings["environment"] == "development"
    )