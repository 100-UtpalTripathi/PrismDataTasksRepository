import json
import datetime
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
import psycopg2
from math import ceil

def split_list(inputs, max_size=100):
    return [inputs[i:i + max_size] for i in range(0, len(inputs), max_size)]

def get_secret(secret_name):
    region_name = "us-east-1"
    config = Config(connect_timeout=1, read_timeout=60)
    client = boto3.client('secretsmanager', region_name=region_name, config=config)

    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except ClientError as e:
        raise e

def get_db_connection(secret):
    try:
        return psycopg2.connect(
            host=secret['endpoint'],
            database=secret['dbname'],
            user=secret['username'],
            password=secret['password'],
            port=secret['port']
        )
    except Exception as e:
        print(f"Error connecting to database: {str(e)}")
        raise e

def get_model_ids(dbconn):
    try:
        with dbconn.cursor() as cur:
            cur.execute("""SELECT model_id FROM "MpaAccount" WHERE prism = true""")
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        raise e

def get_default_yearmonths():
    today = datetime.date.today()
    current = int(today.strftime("%Y%m"))
    if today.day <= 15:
        previous = (today.replace(day=1) - datetime.timedelta(days=1)).strftime("%Y%m")
        return int(previous), current
    return current, current

def lambda_handler(event, context):
    secret_name = "prism-backend-service-dev-secret"
    env = event.get("env", "dev")
    start_ym = int(event.get("startYearMonth", 0))
    end_ym = int(event.get("endYearMonth", 0))

    if start_ym == 0 or end_ym == 0:
        start_ym, end_ym = get_default_yearmonths()

    print(f"[INFO] YearMonth range: {start_ym} -> {end_ym}")

    # Fetch DB creds and model IDs
    secret = get_secret(secret_name)
    conn = get_db_connection(secret)
    model_ids = get_model_ids(conn)
    chunks = split_list(model_ids, 100)

    print(f"[INFO] Total model IDs: {len(model_ids)}, split into {len(chunks)} chunks")

    year_month_array = []

    year = start_ym // 100
    month = start_ym % 100

    while (year * 100 + month) <= end_ym:
        ym = f"{year}{month:02d}"
        print(f"[INFO] Processing yearmonth: {ym}")

        for chunk in chunks:
            chunk_str = ",".join(chunk)
            query = f"""EXECUTE prism_insert_tb_monthly_ec2_cost_summary USING '{ym}', '{chunk_str}'"""

            year_month_array.append({"query": query})
            


        # Advance to next month
        month += 1
        if month > 12:
            month = 1
            year += 1

    queries_submitted = json.dumps({'yearMonthArray': year_month_array})
    print(f"[INFO] Total Athena queries prepared: {len(year_month_array)}")
    print(f"[INFO] Queries: {queries_submitted}")

    return {
        "statusCode": 200,
        "body": queries_submitted
    }
