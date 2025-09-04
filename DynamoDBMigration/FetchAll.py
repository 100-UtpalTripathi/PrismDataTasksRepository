import boto3

ROLE_B_ARN = "arn:aws:iam::604727574140:role/CrossAccountDDBRead_PrismResellTracker"
TABLE_ARN  = "arn:aws:dynamodb:us-east-1:246778806733:table/prism-ops-dynamodb-resell-tracker-data"
REGION     = "us-east-1"

def _ddb_client_as_role_b():
    sts = boto3.client("sts")
    creds = sts.assume_role(
        RoleArn=ROLE_B_ARN,
        RoleSessionName="ddb-xacct"
    )["Credentials"]

    return boto3.client(
        "dynamodb",
        region_name=REGION,
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
    )

def lambda_handler(event, context):
    ddb = _ddb_client_as_role_b()

    resp = ddb.scan(
        TableName=TABLE_ARN,   # ARN is fine in Lambda
        Limit=1
    )

    return {
        "scan_count": resp.get("Count", 0),
        "scan_items": resp.get("Items", []),
    }