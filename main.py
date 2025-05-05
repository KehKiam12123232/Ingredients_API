from fastapi import FastAPI
import psycopg2
from pydantic import BaseModel
import logging
from fastapi.concurrency import run_in_threadpool
from fastapi import HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
import os
from typing import Optional
from psycopg2 import pool
from contextlib import asynccontextmanager

# Connection pool
connection_pool = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    init_connection_pool()
    yield
    # Shutdown
    if connection_pool:
        connection_pool.closeall()
        logger.info("Connection pool closed")

# FastAPI app instance
app = FastAPI(
    title="Ingredients API",
    description="API for managing ingredients",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Set up logging for better debugging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection details (read from environment variables)
DB_NAME = os.getenv("DB_NAME", "neondb")
DB_USER = os.getenv("DB_USER", "neondb_owner")
DB_PASSWORD = os.getenv("DB_PASSWORD", "npg_wTW2nR0sbOKh")
DB_HOST = os.getenv("DB_HOST", "ep-steep-bread-a5km0kjc-pooler.us-east-2.aws.neon.tech")
DB_PORT = os.getenv("DB_PORT", "5432")
SSL_MODE = os.getenv("SSL_MODE", "require")

def init_connection_pool():
    global connection_pool
    try:
        connection_pool = pool.SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            sslmode=SSL_MODE
        )
        logger.info("Connection pool created successfully")
    except Exception as e:
        logger.error(f"Error creating connection pool: {e}")
        raise

def get_db_connection():
    try:
        if connection_pool is None:
            init_connection_pool()
        conn = connection_pool.getconn()
        logger.info("Got connection from pool")
        return conn
    except Exception as e:
        logger.error(f"Error getting connection from pool: {e}")
        return None

def release_connection(conn):
    if conn:
        connection_pool.putconn(conn)
        logger.info("Connection released back to pool")

# Pydantic models
class Ingredient(BaseModel):
    name: str
    price: float
    unit: str

class IngredientUpdate(BaseModel):
    name: Optional[str] = None
    price: Optional[float] = None
    unit: Optional[str] = None

class HealthCheck(BaseModel):
    status: str
    database: str

# Synchronous helper functions for database operations
def _fetch_ingredients_sync():
    conn = get_db_connection()
    if conn is None:
        logger.error("Failed to connect to the database")
        raise ConnectionError("Failed to connect to the database")
    
    try:
        logger.info("Fetching ingredients from the database...")
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM ingredients LIMIT 10;")
        ingredients = cursor.fetchall()
        logger.info(f"Fetched {len(ingredients)} ingredients.")
        
        ingredient_list = [{"name": row[1], "price": row[2], "unit": row[3]} for row in ingredients]
        
        cursor.close()
        return {"ingredients": ingredient_list}
    except Exception as e:
        logger.error(f"Error fetching ingredients: {e}")
        raise e
    finally:
        release_connection(conn)

def _create_ingredient_sync(ingredient: Ingredient):
    conn = get_db_connection()
    if conn is None:
        raise ConnectionError("Failed to connect to the database")
    
    try:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO ingredients (name, price, unit) VALUES (%s, %s, %s) RETURNING id;",
            (ingredient.name, ingredient.price, ingredient.unit)
        )
        ingredient_id = cursor.fetchone()[0]
        conn.commit()
        logger.info(f"Created new ingredient with ID: {ingredient_id}")
        return {"id": ingredient_id, **ingredient.dict()}
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating ingredient: {e}")
        raise e
    finally:
        release_connection(conn)

def _update_ingredient_sync(ingredient_id: int, ingredient_update: IngredientUpdate):
    conn = get_db_connection()
    if conn is None:
        raise ConnectionError("Failed to connect to the database")
    
    try:
        cursor = conn.cursor()
        
        update_fields = []
        update_values = []
        
        if ingredient_update.name is not None:
            update_fields.append("name = %s")
            update_values.append(ingredient_update.name)
        if ingredient_update.price is not None:
            update_fields.append("price = %s")
            update_values.append(ingredient_update.price)
        if ingredient_update.unit is not None:
            update_fields.append("unit = %s")
            update_values.append(ingredient_update.unit)
        
        if not update_fields:
            raise HTTPException(status_code=400, detail="No fields to update")
        
        update_values.append(ingredient_id)
        update_query = f"UPDATE ingredients SET {', '.join(update_fields)} WHERE id = %s RETURNING id, name, price, unit;"
        
        cursor.execute(update_query, update_values)
        updated_ingredient = cursor.fetchone()
        
        if not updated_ingredient:
            raise HTTPException(status_code=404, detail="Ingredient not found")
        
        conn.commit()
        return {
            "id": updated_ingredient[0],
            "name": updated_ingredient[1],
            "price": updated_ingredient[2],
            "unit": updated_ingredient[3]
        }
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        logger.error(f"Error updating ingredient: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        release_connection(conn)

# Health check endpoint
@app.get("/health", response_model=HealthCheck)
async def health_check():
    try:
        conn = get_db_connection()
        if conn:
            release_connection(conn)
            return HealthCheck(status="healthy", database="connected")
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return HealthCheck(status="unhealthy", database="disconnected")

# API Routes
@app.get("/ingredients")
async def get_ingredients():
    try:
        result = await run_in_threadpool(_fetch_ingredients_sync)
        return result
    except ConnectionError as ce:
        raise HTTPException(status_code=503, detail=str(ce))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching ingredients: {str(e)}")

@app.post("/ingredients")
async def create_ingredient(ingredient: Ingredient):
    try:
        result = await run_in_threadpool(_create_ingredient_sync, ingredient)
        return result
    except ConnectionError as ce:
        raise HTTPException(status_code=503, detail=str(ce))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating ingredient: {str(e)}")

@app.put("/ingredients/{ingredient_id}")
async def update_ingredient(ingredient_id: int, ingredient_update: IngredientUpdate):
    try:
        result = await run_in_threadpool(_update_ingredient_sync, ingredient_id, ingredient_update)
        return result
    except HTTPException:
        raise
    except ConnectionError as ce:
        raise HTTPException(status_code=503, detail=str(ce))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating ingredient: {str(e)}")
