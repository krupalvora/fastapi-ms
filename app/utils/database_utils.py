from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv
import datetime,time

# Load environment variables
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_async_engine(DATABASE_URL, echo=True, future=True)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def execute_query(query: str):
    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(text(query))
            rows = result.fetchall()
            columns = result.keys()
            await session.commit()
            return rows, columns
        
    except Exception as e:
        print(f"Error executing query: {e}")
        return None, None
    finally:
        await session.close()

async def fetch_interval_data(query: str, doctype: str, where_cond: str, start_date: datetime, end_date: datetime):
    try:
        # Build the date filter condition for the current interval
        date_filter = f" AND `tab{doctype}`.creation BETWEEN '{start_date.strftime('%Y-%m-%d %H:%M:%S')}' AND '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'"
        # Combine with the base query and additional where conditions
        query = query + " " + date_filter
        print(f"Fetching data for interval {start_date} to {end_date}...")
        
        # Execute the query
        rows, columns = await execute_query(query)
        print(f"Fetched {len(rows)} rows for interval {start_date} to {end_date}")
        return rows, columns
    except Exception as e:
        print(f"Error fetching data for interval {start_date} to {end_date}: {e}")
        return [], []

#Function to get min and max creation date from given query
async def get_min_max_date(doctype: str, where_condition: str):
    date_query = "SELECT MIN(creation) as min_date, MAX(creation) as max_date FROM `tab"+doctype+"` "+where_condition
    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(text(date_query))
            rows = result.fetchall()
            await session.commit()
            return rows
    except Exception as e:
        print(f"Error executing query: {e}")
        return None
    finally:
        await session.close()

from dateutil.relativedelta import relativedelta

def generate_monthly_intervals(min_date: datetime, max_date: datetime):
    intervals = []
    current = min_date.replace(day=1)
    while current < max_date:
        next_month = current + relativedelta(months=6)
        # Ensure we don't go beyond max_date
        intervals.append((current, min(next_month - relativedelta(seconds=1), max_date)))
        current = next_month
    return intervals