from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

ERP_DATABASE_URL = os.getenv("ERP_DATABASE_URL")
engine = create_async_engine(ERP_DATABASE_URL, echo=True, future=True)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

#change status of recrd in Export Report table based on report name, 
async def change_status(report_name,status,public_url=None):
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(text(f"UPDATE `tabExport Report` SET status = '{status}',file_url = '{public_url}' WHERE name = '{report_name}'"))
            await session.commit()
    except Exception as e:
        print(f"Error : change_status-Error executing query: {e}")
    finally:
        await session.close()