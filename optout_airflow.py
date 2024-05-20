import os
import re
import sys
import _io
import csv
import time
import json
import errno
import boto3
import argparse
from pathlib import Path
from pprint import pprint
from builtins import range
from pymssql import connect
from decimal import Decimal
from subprocess import Popen
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
from boto.exception import BotoServerError, NoAuthHandlerFound
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator

# ========
# global variable
# ========

AWS_CONFIG = Variable.get("AWS_DEFAULT", deserialize_json=True)

MSSQL_DEFAULT = BaseHook.get_connection("mssql_default")

AWS_DYNAMODB = Variable.get("AWS_DYNAMODB", deserialize_json=True)

AWS_DYNAMODB_CLIENT = boto3.client(
    "dynamodb",
    region_name=AWS_DYNAMODB["region"],
    aws_access_key_id=AWS_CONFIG["access_key"],
    aws_secret_access_key=AWS_CONFIG["secret_key"],
)

AWS_DYNAMODB_TABLE_NAME = "DynamoDBTable"
PK_OPTOUT = "Company"
SK_OPTOUT = "OptOut"

START_COMMAND = """    
    date
    cd $HOME
"""

# ========
# schedule interval
# ========

dag = DAG(
    dag_id="optout",
    schedule_interval=timedelta(minutes=15),
    start_date=days_ago(2),
)

# ========
# function
# ========


def execute_put_item(input):
    try:
        response = AWS_DYNAMODB_CLIENT.put_item(**input)
        print("Successfully put item.")
        return response
    except ClientError as error:
        return error.response["Error"]["Message"]
    except BaseException:
        return "Error BaseException"


def create_optout_input(CompanyID, Email):
    date_now = datetime.today().strftime("%Y-%m-%d")

    items = {
        "EventID": {"S": "{}-{}".format(PK_OPTOUT, CompanyID)},
        "SK": {"S": "{}-{}".format(SK_OPTOUT, Email)},
        "CompanyID": {"S": "{}".format(CompanyID)},
        "CreatedAt": {"S": "{}".format(date_now)},
    }

    return {"TableName": AWS_DYNAMODB_TABLE_NAME, "Item": items}


def get_error_dynamodb(resp):
    if "Items" not in resp:
        return "Data not found"
    else:
        if 0 == len(resp["Items"]):
            return "Data not found"

    if "Count" not in resp:
        return "Data not found"
    else:
        if 0 == resp["Count"]:
            return "Data not found"

    return None


def get_optout_email(CompanyID, Email):
    dynamodb = AWS_DYNAMODB_CLIENT.query(
        TableName=AWS_DYNAMODB_TABLE_NAME,
        KeyConditionExpression="#85690 = :85690 And begins_with(#85691, :85691)",
        ExpressionAttributeNames={"#85690": "EventID", "#85691": "SK"},
        ExpressionAttributeValues={
            ":85690": {"S": "{}-{}".format(PK_OPTOUT, CompanyID)},
            ":85691": {"S": "{}-{}".format(SK_OPTOUT, Email)},
        },
    )

    return dynamodb


def get_data_all(sql_query):
    try:
        MSSQL_CONN = connect(
            MSSQL_DEFAULT.host,
            MSSQL_DEFAULT.login,
            MSSQL_DEFAULT.password,
            MSSQL_DEFAULT.schema,
        )
        MSSQL_CURSOR = MSSQL_CONN.cursor()
        MSSQL_CURSOR.execute(sql_query)
        row = MSSQL_CURSOR.fetchall()
        MSSQL_CONN.close()
        if row:
            return row
        return None
    except Exception as e:
        print("error query sql:" + str(e))
        print(sql_query)
        return None


def get_email(data):
    sk_s = data["SK"]["S"]
    email = sk_s[11:]
    return email


def get_email_all():
    SQL = "SELECT company_id, email, deleted, modified  FROM LAUNCHDB.dbo.optout  "
    SQL += "GROUP BY company_id, email, deleted, modified "
    SQL += "HAVING deleted = 0 AND DATEDIFF(minute, modified, GETDATE()) < 60 "
    return get_data_all(SQL)


def get_optout_sync(**kwargs):

    try:
        records = get_email_all()
        if None == records:
            print("records: None")
            return

        for row in records:
            CompanyID = row[0]
            Email = str(row[1]).lower()

            email_queue_raw = get_optout_email(CompanyID, Email)

            error_dynamodb = get_error_dynamodb(email_queue_raw)
            if None != error_dynamodb:
                create_optout = create_optout_input(CompanyID, Email)
                print("Create Dyn: {}".format(create_optout))
                execute_put_item(create_optout)
                continue

        return 0
    except ClientError as e:
        resp = e.response["Error"]
        print("[{}] {}".format(resp["Code"], resp["Message"]))
        return 0


# ======
# TASK
# ======


start_task = BashOperator(
    task_id="start",
    bash_command=START_COMMAND,
    depends_on_past=False,
    dag=dag,
)

get_optout_sync_task = PythonOperator(
    task_id="get_optout_sync",
    provide_context=True,
    python_callable=get_optout_sync,
    dag=dag,
)

start_task >> get_optout_sync_task

if __name__ == "__main__":
    dag.cli()
