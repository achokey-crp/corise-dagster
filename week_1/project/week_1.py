import csv
from datetime import datetime
from typing import Iterator, List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    String,
    job,
    op,
    usable_as_dagster_type,
)
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: List[str]):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


def csv_helper(file_name: str) -> Iterator[Stock]:
    with open(file_name) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield Stock.from_list(row)


@op(config_schema={"s3_key": String})
def get_s3_data_op(context):
    data_file = context.op_config["s3_key"]
    data = list(csv_helper(data_file))
    return data

@op
def process_data_op(context, stocks):

    highest = max(stocks, key=lambda item: item.high)
    return Aggregation(date=highest.date, high=highest.high)


@op(ins={"data": In(dagster_type=Aggregation)})
def put_redis_data_op(context, data):
    pass


@op(ins={"data": In(dagster_type=Aggregation)})
def put_s3_data_op(context, data):
    pass


@job
def machine_learning_job():
    agg_data = process_data_op(get_s3_data_op())
    put_redis_data_op(agg_data)
    put_s3_data_op(agg_data)
