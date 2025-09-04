import boto3

ROLE_B_ARN = "arn:aws:iam::604727574140:role/CrossAccountDDBRead_PrismResellTracker"
TABLE_ARN  = "arn:aws:dynamodb:us-east-1:246778806733:table/prism-ops-dynamodb-resell-tracker-data"
REGION     = "us-east-1"

def _ddb_as_role_b():
    sts = boto3.client("sts")
    creds = sts.assume_role(RoleArn=ROLE_B_ARN, RoleSessionName="ddb-xacct")["Credentials"]
    return boto3.client(
        "dynamodb",
        region_name=REGION,
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
    )

def lambda_handler(event, context):
    ddb = _ddb_as_role_b()

    model_ids = []
    params = {
        "TableName": TABLE_ARN,
        "ProjectionExpression": "model_id",
        "FilterExpression": "is_prism = :t",
        "ExpressionAttributeValues": {":t": {"BOOL": True}},
    }

    resp = ddb.scan(**params)
    while True:
        for it in resp.get("Items", []):
            mid = it.get("model_id", {}).get("S")
            if mid:
                model_ids.append(mid)

        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break
        resp = ddb.scan(ExclusiveStartKey=lek, **params)

    return {"count": len(model_ids), "model_ids": model_ids}