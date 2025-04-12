from fastapi import FastAPI, BackgroundTasks, HTTPException, Depends
from pydantic import BaseModel
import os
import csv
from utils.database_utils import execute_query, get_min_max_date,generate_monthly_intervals,fetch_interval_data
from utils.email_utils import send_email 
from utils.audit import change_status
from utils.filename import get_filename
from utils.monitor import monitor_usage
from dotenv import load_dotenv
import boto3
import io
import tempfile
import time, datetime
import psutil
import asyncio
# from magnum import Magnum


# Load environment variables
load_dotenv()

s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET"),
    region_name=os.getenv("REGION"),
)

app = FastAPI()
# handler = Magnum(app)

class ReportRequest(BaseModel):
    query: str
    email: str
    report_name:str
    doctype:str
    where_cond:str

# Function to execute the query and store the CSV
@monitor_usage
async def process_query_and_send_email(query: str, email: str, report_name: str, doctype: str, where_cond: str):
    try:
        print("------------ Processing query ------------")
        # Get the min and max date from the query
        min_max = await get_min_max_date(doctype, where_cond)
        print(f"Min max date: {min_max}")
        if min_max and len(min_max) > 0:
            min_date, max_date = min_max[0]
            print(f"Min date: {min_date}, Max date: {max_date}")
        else:
            print("No date range found.")

        # Generate monthly intervals (or use another interval if preferred)
        # Intervals are used to split the data fetching into smaller chunks
        # This is useful for large datasets and to avoid timeouts
        # We have kept intervals to 1 month, and we are using creation date because frappe creates index on creation date.
        intervals = generate_monthly_intervals(min_date, max_date)
        print(f"Generated {len(intervals)} intervals.")


        # Create tasks for each interval that fetch data concurrently
        tasks = [
            fetch_interval_data(query, doctype, where_cond, start, end)
            for start, end in intervals
        ]
        results = await asyncio.gather(*tasks)

        # Combine results from all intervals
        all_rows = []
        header = None
        for rows, columns in results:
            if header is None and columns:
                header = columns
            all_rows.extend(rows)
        
        print(f"Total rows fetched: {len(all_rows)}")

        # Write all data to a single CSV file
        filename = f"{get_filename(report_name)}"
        s3_path = f"reports/{filename}"
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
            writer = csv.writer(temp_file)
            # Write header once
            if header:
                writer.writerow(header)
            # Write all rows
            writer.writerows(all_rows)
            temp_file_path = temp_file.name

        process = psutil.Process() 
        print(f"Rows: {len(rows)}, Columns: {len(columns)} , Memory: {process.memory_info().rss / (1024 * 1024)}")

        print("------------ CSV written to temporary file ------------")

        # Upload to S3 in a single efficient request
        s3_client.upload_file(temp_file_path, os.getenv("BUCKET"), s3_path)
        os.remove(temp_file_path)  # Cleanup the temporary file

        print("------------ Uploaded to S3 ------------")

        # Generate public URL
        public_url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": os.getenv("BUCKET"), "Key": s3_path},
            ExpiresIn=86400,
        )

        # Send email in background
        await send_email(email, public_url)

        print(f"Report generated: {public_url}")
        await change_status(report_name, status="Completed", public_url=public_url)

    except Exception as e:
        await change_status(report_name, status="Failed")
        print(f"-> Error processing request: {e}")

@app.post("/generate_report/")
async def generate_report(request: ReportRequest, background_tasks: BackgroundTasks):
    try:
        import time
        time.sleep(3)
        # Use background tasks to run the report generation
        background_tasks.add_task(process_query_and_send_email, request.query, request.email ,request.report_name ,request.doctype, request.where_cond )
        return {"message": "Report generation started", "email": request.email}
    except Exception as e:
        print(f"->Error processing request: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/chk/")
def health_check():
    return {"message": "FastAPI server is running"}
