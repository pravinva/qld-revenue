import pandas as pd

from qldrevenue.dbsql import dataframe_from_statement_response


class Col:
    def __init__(self, name: str):
        self.name = name


class Schema:
    def __init__(self, columns):
        self.columns = columns


class Manifest:
    def __init__(self, schema):
        self.schema = schema


class Result:
    def __init__(self, data_array):
        self.data_array = data_array


class RespNew:
    def __init__(self):
        self.manifest = Manifest(Schema([Col("a"), Col("b")] ))
        self.result = Result([[1, 2], [3, 4]])


class RespOld:
    def __init__(self):
        # manifest attached to result in older shape
        self.result = Result([["x"]])
        self.result.manifest = Manifest(Schema([Col("c")]))


def test_dataframe_from_statement_response_new_shape() -> None:
    df = dataframe_from_statement_response(RespNew())
    assert list(df.columns) == ["a", "b"]
    assert df.to_dict("records") == [{"a": 1, "b": 2}, {"a": 3, "b": 4}]


def test_dataframe_from_statement_response_old_shape() -> None:
    df = dataframe_from_statement_response(RespOld())
    assert list(df.columns) == ["c"]
    assert df.to_dict("records") == [{"c": "x"}]
