import json
import datetime
import boto3

# --- constants ---
ROLE_B_ARN = "arn:aws:iam::604727574140:role/CrossAccountDDBRead_PrismResellTracker"  # the role CrossAccount allowed!
TABLE_ARN  = "arn:aws:dynamodb:us-east-1:246778806733:table/prism-ops-dynamodb-resell-tracker"
REGION     = "us-east-1"


def _ddb_client_as_role_b():
    """
    Assume Role-B (the role listed as Principal on Nick's table),
    then build a DynamoDB client with the temporary credentials.
    """
    sts = boto3.client("sts")
    creds = sts.assume_role(RoleArn=ROLE_B_ARN, RoleSessionName="ddb-xacct")["Credentials"]

    return boto3.client(
        "dynamodb",
        region_name=REGION,
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
    )


def get_model_ids_from_ddb():
    """
    Return a list of all ModelID (strings) for items where PRISM = true.
    Uses ProjectionExpression to only fetch ModelID, and paginates through the table.
    """
    ddb = _ddb_client_as_role_b()

    model_ids = []
    params = {
        "TableName": TABLE_ARN,
        "ProjectionExpression": "#mid",                         # only return ModelID
        "ExpressionAttributeNames": {"#mid": "ModelID", "#p": "PRISM"},
        "FilterExpression": "#p = :true",                       # keep items where PRISM == true
        "ExpressionAttributeValues": {":true": {"BOOL": True}},
    }

    resp = ddb.scan(**params)
    for it in resp.get("Items", []):
        # low-level client returns DynamoDB-typed JSON: {"ModelID": {"S": "<uuid>"}}
        if "ModelID" in it and "S" in it["ModelID"]:
            model_ids.append(it["ModelID"]["S"])

    # paginate if there are more pages
    while "LastEvaluatedKey" in resp:
        resp = ddb.scan(ExclusiveStartKey=resp["LastEvaluatedKey"], **params)
        for it in resp.get("Items", []):
            if "ModelID" in it and "S" in it["ModelID"]:
                model_ids.append(it["ModelID"]["S"])

    return model_ids

def split_list(inputs, max_size=100):
    return [inputs[i:i + max_size] for i in range(0, len(inputs), max_size)]

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

    # fetch model IDs
    model_ids = get_model_ids_from_ddb()
    chunks = split_list(model_ids, 100)

    print(f"[INFO] Total model IDs: {len(model_ids)}, split into {len(chunks)} chunks")

    queries = []

    year = start_ym // 100
    month = start_ym % 100

    while (year * 100 + month) <= end_ym:
        ym = f"{year}{month:02d}"
        print(f"[INFO] Processing yearmonth: {ym}")

        for chunk in chunks:
            chunk_str = ",".join(chunk)
            query = f"""EXECUTE prism_insert_tb_daily_cost_summary_chunked USING '{ym}', '{chunk_str}'"""


            queries.append({"query": query})
        # Advance to next month
        month += 1
        if month > 12:
            month = 1
            year += 1

    queries_submitted = json.dumps({'queries': queries})
    print(f"[INFO] Total Athena queries prepared: {len(queries)}")
    print(f"[INFO] Queries: {queries_submitted}")

    return {
        "statusCode": 200,
        "body": queries_submitted
    }
