import boto3

ROLE_B_ARN = "arn:aws:iam::604727574140:role/CrossAccountDDBRead_PrismResellTracker"  
TABLE_ARN  = "arn:aws:dynamodb:us-east-1:246778806733:table/prism-ops-dynamodb-resell-tracker"
REGION     = "us-east-1"


def _ddb_client_as_role_b():
    
    sts = boto3.client("sts")
    assumed = sts.assume_role(
        RoleArn=ROLE_B_ARN,
        RoleSessionName="ddb-xacct"  # any string; appears in CloudTrail
    )["Credentials"]

    # Creating a DDB client using *Role-B's* temporary creds
    return boto3.client(
        "dynamodb",
        region_name=REGION,
        aws_access_key_id=assumed["AccessKeyId"],
        aws_secret_access_key=assumed["SecretAccessKey"],
        aws_session_token=assumed["SessionToken"],
    )


def lambda_handler(event, context):
    # Get a DDB client that represents Role-B (the role listed as Principal on the table)
    ddb = _ddb_client_as_role_b()

    scan_resp = ddb.scan(
        TableName=TABLE_ARN,
        Limit=1  # keep it small; scans can be expensive
    )

    # --- Option 2 (example): fetch a single item by PK 'modelid' ---
    # Uncomment and provide a real modelid if you want to test GetItem instead.
    # get_resp = ddb.get_item(
    #     TableName=TABLE_ARN,
    #     Key={"modelid": {"S": "cbf303f5-82c4-4c0f-a879-ad0ba50b4366"}}
    # )

    return {
        "scan_count": scan_resp.get("Count", 0),
        "scan_items": scan_resp.get("Items", []),
        # "get_item": get_resp.get("Item", {})  # if you used GetItem above
    }
