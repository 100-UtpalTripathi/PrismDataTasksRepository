import json
import boto3
import datetime
from botocore.exceptions import ClientError
from botocore.config import Config
import psycopg2
from math import ceil

def get_secret(secret_name: str):
    region_name = "us-east-1"
    config = Config(connect_timeout=1, read_timeout=60)
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name, config=config)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except ClientError as e:
        print(f"[ERROR] Failed to get secret: {e}")
        raise e

def get_db_connection(secret: dict):
    try:
        return psycopg2.connect(
            host=secret['endpoint'],
            database=secret['dbname'],
            user=secret['username'],
            password=secret['password'],
            port=secret['port']
        )
    except Exception as e:
        print(f"[ERROR] DB connection failed: {e}")
        raise e

def get_model_ids(db_conn):
    try:
        with db_conn.cursor() as cur:
            cur.execute("SELECT model_id FROM \"MpaAccount\" WHERE prism = true")
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        print(f"[ERROR] Failed fetching model_ids: {e}")
        raise e

def chunk_list(lst, chunk_size=100):
    return [lst[i:i+chunk_size] for i in range(0, len(lst), chunk_size)]

def lambda_handler(event, context):
    athena_client = boto3.client('athena')
    prepared_stmt_map = {
        "dev": "prism_insert_tb_daily_cost_summary_chunked",
        "prod": "prism_insert_tb_daily_cost_summary_chunked_prod"
    }

    start_date_str = event.get('start_date', '0')
    end_date_str = event.get('end_date', '0')
    env = event.get('env', 'dev')

    if start_date_str == '0' or end_date_str == '0':
        today = datetime.date.today()
        start_date = today - datetime.timedelta(days=15)
        end_date = today
    else:
        start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').date()

    if end_date < start_date:
        return {'statusCode': 400, 'message': "end_date must be after start_date"}

    secret_name = "prism-backend-service-dev-secret"
    try:
        db_secret = get_secret(secret_name)
        db_conn = get_db_connection(db_secret)
        model_ids = get_model_ids(db_conn)
    except Exception as e:
        return {'statusCode': 500, 'message': str(e)}

    chunks = chunk_list(model_ids)
    query_results = []

    while start_date <= end_date:
        yearmonth = start_date.strftime('%Y%m')
        usagedate = start_date.strftime('%Y-%m-%d')
        for chunk in chunks:
            try:
                modelid_arr = ",".join(chunk)
                query = f"EXECUTE {prepared_stmt_map[env]} USING '{yearmonth}', '{usagedate}', '{modelid_arr}'"
                response = athena_client.start_query_execution(
                    QueryString=query,
                    QueryExecutionContext={"Database": "prism_curated_dev"},
                    ResultConfiguration={"OutputLocation": "s3://athena-604727574140-prism-frontend-wg-query-results/"},
                    WorkGroup="prism-frontend-wg"
                )
                query_results.append({
                    'query': query,
                    'execution_id': response['QueryExecutionId']
                })
            except Exception as e:
                print(f"[ERROR] Query execution failed: {e}")
        start_date += datetime.timedelta(days=1)

    return {
        'statusCode': 200,
        'executed_queries': query_results
    }






