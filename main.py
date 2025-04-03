from fastapi import FastAPI
import psycopg2
from pydantic import BaseModel
import logging
from fastapi.concurrency import run_in_threadpool
from fastapi import HTTPException
import os # Import the os module

# FastAPI app instance
app = FastAPI()

# Set up logging for better debugging
logging.basicConfig(level=logging.DEBUG)

# Database connection details (read from environment variables)
DB_NAME = os.getenv("DB_NAME", "neondb") # Default for local testing
DB_USER = os.getenv("DB_USER", "neondb_owner")
DB_PASSWORD = os.getenv("DB_PASSWORD", "npg_wTW2nR0sbOKh") # Replace with a safe default or handle error if not set
DB_HOST = os.getenv("DB_HOST", "ep-steep-bread-a5km0kjc-pooler.us-east-2.aws.neon.tech")
DB_PORT = os.getenv("DB_PORT", "5432")
SSL_MODE = os.getenv("SSL_MODE", "require") # Add SSL_MODE variable

# Database connection function using psycopg2
def get_db_connection():
    try:
        logging.info("Connecting to the database...")
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            sslmode=SSL_MODE  # Use the variable here
        )
        logging.info("Database connection successful.")
        return conn
    except Exception as e:
        logging.error(f"Database connection error: {e}")
        return None

# Pydantic model to parse incoming data
class Ingredient(BaseModel):
    name: str
    price: float
    unit: str

# Synchronous helper function for database operations
def _fetch_ingredients_sync():
    conn = get_db_connection()
    if conn is None:
        logging.error("Failed to connect to the database")
        # Raise an exception or return an indicator of failure
        raise ConnectionError("Failed to connect to the database")
    
    try:
        logging.info("Fetching ingredients from the database...")
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM ingredients LIMIT 10;")
        ingredients = cursor.fetchall()
        logging.info(f"Fetched {len(ingredients)} ingredients.")
        
        # Convert the database rows into a list of dictionaries
        ingredient_list = [{"name": row[1], "price": row[2], "unit": row[3]} for row in ingredients]
        
        cursor.close()
        conn.close()
        return {"ingredients": ingredient_list}
    except Exception as e:
        logging.error(f"Error fetching ingredients: {e}")
        # Re-raise the exception to be caught by the async wrapper
        raise e
    finally:
        # Ensure connection is closed even if errors occur before cursor.close()
        if conn:
            conn.close()


# Route to get all ingredients from the database (now async)
@app.get("/ingredients")
async def get_ingredients():
    try:
        # Run the synchronous database operations in a thread pool
        result = await run_in_threadpool(_fetch_ingredients_sync)
        return result
    except ConnectionError as ce:
        # Handle connection errors specifically if needed
        # Return a 503 Service Unavailable status code
        raise HTTPException(status_code=503, detail=str(ce))
    except Exception as e:
        # Catch other exceptions from the sync function
        # Return a 500 Internal Server Error for other DB issues
        raise HTTPException(status_code=500, detail=f"Error fetching ingredients: {str(e)}")
