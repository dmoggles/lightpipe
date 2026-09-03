from __future__ import annotations

from lightpipe import CachePolicy, pipeline, stage

type Row = dict[str, int | str]


@stage
def scrape(target: str) -> list[Row]:
    return [{"target": target, "value": value} for value in range(3)]


@stage(cache=CachePolicy.seconds(3600))
def predict(row: Row) -> Row:
    value = row["value"]
    assert isinstance(value, int)
    return {**row, "prediction": value % 2}


@stage
def save(row: Row) -> None:
    print(f"persist {row}")


@pipeline
def scrape_and_predict(target: str):
    predictions = predict.map(scrape(target))
    return save.map(predictions)
