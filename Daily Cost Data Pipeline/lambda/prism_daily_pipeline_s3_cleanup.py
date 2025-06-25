import boto3
import datetime
from dateutil.relativedelta import relativedelta

def delete_objects_between_dates(bucket_name, folder, start_date, end_date):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')

    current_date = start_date
    while current_date <= end_date:
        yearmonth = current_date.strftime('%Y%m')
        usagedate = current_date.strftime('%Y-%m-%d')
        
        prefix = f"{folder}yearmonth={yearmonth}/lineitem_usagedate={usagedate}/"
        print(f"[INFO] Checking prefix: {prefix}")
        
        deleted_any = False
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                print(f"[DELETE] Deleting object: {key}")
                s3.delete_object(Bucket=bucket_name, Key=key)
                deleted_any = True
        
        if not deleted_any:
            print(f"[SKIP] No objects found under: {prefix}")
        
        current_date += datetime.timedelta(days=1)

def lambda_handler(event, context):
    print(f"[DEBUG] Event received: {event}")

    # Getting the bucket and folder from event
    bucket_name = event['bucket_name']
    folder = event['folder']  # e.g., 'tb_daily_cost_summary/'

    # Extracting date range, default can be taken as '0'
    start_date_str = event.get('start_date', '0')
    end_date_str = event.get('end_date', '0')

    # Determine actual date range
    if start_date_str in ['0', ''] or end_date_str in ['0', '']:
        today = datetime.date.today()
        start_date = today - datetime.timedelta(days=15)
        end_date = today
        print(f"[INFO] Using default date range (last 15 days): {start_date} to {end_date}")
    else:
        start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').date()
        print(f"[INFO] Using provided date range: {start_date} to {end_date}")

    if end_date < start_date:
        raise ValueError("[ERROR] end_date must be greater than or equal to start_date")

    # Call deletioning function logic
    print(f"[ACTION] Starting object deletion from S3 for the given date range...")
    delete_objects_between_dates(bucket_name, folder, start_date, end_date)

    return {
        'statusCode': 200,
        'message': f"Deleted data between {start_date} and {end_date} from bucket {bucket_name}"
    }
