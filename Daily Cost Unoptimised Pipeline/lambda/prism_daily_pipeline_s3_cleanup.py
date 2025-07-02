
import boto3
import datetime
from botocore.exceptions import ClientError

def generate_yearmonth_range(start, end):
    start_year, start_month = int(start[:4]), int(start[4:])
    end_year, end_month = int(end[:4]), int(end[4:])
    while (start_year < end_year) or (start_year == end_year and start_month <= end_month):
        yield f"{start_year}{str(start_month).zfill(2)}"
        start_month += 1
        if start_month > 12:
            start_month = 1
            start_year += 1

def delete_objects_by_yearmonth(bucket_name, folder, start_ym, end_ym):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')

    for yearmonth in generate_yearmonth_range(start_ym, end_ym):
        prefix = f"{folder}yearmonth={yearmonth}/"
        print(f"[INFO] Checking prefix: {prefix}")
        deleted_any = False
        try:
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    print(f"[DELETE] Deleting object: {key}")
                    s3.delete_object(Bucket=bucket_name, Key=key)
                    deleted_any = True
            if not deleted_any:
                print(f"[SKIP] No objects found under: {prefix}")
        except ClientError as e:
            print(f"[ERROR] Failed to delete objects for prefix {prefix}: {e}")

def lambda_handler(event, context):
    try:
        bucket_name = event['bucket_name']
        folder = event['folder']
        start_ym = event.get('start_ym', '0')
        end_ym = event.get('end_ym', '0')

        if start_ym == '0' or end_ym == '0':
            today = datetime.date.today()
            current_month = today.strftime("%Y%m")
            if today.day <= 15:
                if today.month == 1:
                    previous_month = (today.replace(month=12, year=today.year-1)).strftime("%Y%m")
                else:
                    previous_month = (today.replace(month=today.month-1)).strftime("%Y%m")
                start_ym, end_ym = previous_month, current_month
            else:
                start_ym = end_ym = current_month

        print(f"[INFO] Deleting objects from {start_ym} to {end_ym} in {bucket_name}/{folder}")
        delete_objects_by_yearmonth(bucket_name, folder, start_ym, end_ym)

        return {
            'statusCode': 200,
            'message': f"Deleted S3 objects from {start_ym} to {end_ym}."
        }

    except KeyError as e:
        return {'statusCode': 400, 'message': f"Missing required parameter: {str(e)}"}
    except Exception as e:
        return {'statusCode': 500, 'message': f"Unhandled error: {str(e)}"}
