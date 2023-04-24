from datetime import datetime
from typing import List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    ResourceDefinition,
    String,
    graph,
    op,
)
from workspaces.config import REDIS, S3, S3_FILE
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    config_schema={"s3_key": String},
    out={"stocks": Out(dagster_type=List[Stock])}
)
def get_s3_data(context: OpExecutionContext):
    file_key = context.op_config["s3_key"]
    stocks = context.resources.s3.get_data(file_key)
    return [Stock.from_list(record) for record in stocks]

@op(description="Return the stock with the highest value.")
def process_data(context: OpExecutionContext, stocks):
    highest = max(stocks, key=lambda item: item.high)
    return Aggregation(date=highest.date, high=highest.high)


@op(
    ins={"data": In(dagster_type=Aggregation)}
)
def put_redis_data(context: OpExecutionContext, data):
    redis = context.resources.redis
    redis.put_data(
        data.date.strftime("%m-%d-%Y"), 
        str(data.high)
    )


@op(
    ins={"data": In(dagster_type=Aggregation)}
)
def put_s3_data(context: OpExecutionContext, data):
    s3 = context.resources.s3

    filename = f'{data.date.strftime("%m-%d-%Y")}.csv'
    s3.put_data(
        key_name = filename,
        data = data
    )


@graph
def machine_learning_graph():
    pass


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

docker = {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

machine_learning_job_local = machine_learning_graph.to_job(
    name="machine_learning_job_local",
)

machine_learning_job_docker = machine_learning_graph.to_job(
    name="machine_learning_job_docker",
)
