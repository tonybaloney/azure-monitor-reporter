import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone

import azure.functions as func
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.monitor.query import LogsQueryClient, LogsQueryStatus
from azure.storage.blob import BlobClient
from pandas.api.types import is_datetime64_any_dtype as is_datetime

matplotlib.use('agg')
from io import BytesIO

app = func.FunctionApp()

# Learn more at aka.ms/pythonprogrammingmodel

# Get started by running the following code to create a function using a HTTP trigger.

@app.function_name(name="QueryKusto")
@app.route(route="graph", methods=[func.HttpMethod.POST], auth_level=func.AuthLevel.ANONYMOUS)
def query_kusto(req: func.HttpRequest) -> func.HttpResponse:
    request_id = str(uuid.uuid4())
    logging.info('Assigning request %s', request_id)

    query = req.get_body().decode('utf-8')
    tenant_id = req.headers.get('x-ms-client-tenant-id')
    client_id = req.headers.get('x-ms-client-id')
    client_secret = req.headers.get('x-ms-client-secret')


    workspace = req.params.get('workspace')
    x_col = req.params.get('x')
    y_col = req.params.get('y')
    start_time=datetime.now(tz=timezone.utc) - timedelta(hours=6)
    end_time=datetime.now(tz=timezone.utc)
    credential = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
    client = LogsQueryClient(credential)
    response = client.query_workspace(
        workspace_id=workspace,
        query=query,
        timespan=(start_time, end_time),
        )
    if response.status == LogsQueryStatus.PARTIAL:
        error = response.partial_error
        data = response.partial_data
        logging.info(error)
    elif response.status == LogsQueryStatus.SUCCESS:
        data = response.tables
        logging.info("Got result")
    for table in data:
        df = pd.DataFrame(data=table.rows, columns=table.columns)
        logging.info("Reported types are %s", table.columns_types)
        logging.info("Data types are %s", df.dtypes)
        # sort values if x_col is a datetime
        if is_datetime(df[x_col].dtype):
            df.sort_values(by=[x_col], inplace=True)
        plt.plot(df[x_col], df[y_col])

    s = BytesIO()
    plt.savefig(s, format='png')

    blob = BlobClient.from_connection_string(conn_str=os.environ["AZURE_STORAGE_CONNECTION_STRING"], container_name=os.environ["AZURE_STORAGE_CONTAINER_NAME"], blob_name=f"{request_id}.png")

    blob.upload_blob(s.getvalue())
    s.close()

    return func.HttpResponse(json.dumps({"url": blob.url}), mimetype="application/json")