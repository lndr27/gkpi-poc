import numpy as np
import pandas as pd
from cognite.client import CogniteClient
from cognite.client.data_classes import TimeSeriesWrite


def create_random_timeseries(
    client: CogniteClient, ts_external_ids: list[str], random_seed: int
):
    """
    create_random_timeseries(
        client,
        [TIMESERIES_PARAM_A, TIMESERIES_PARAM_B, TIMESERIES_PARAM_C]
        RANDOM_SEED
    )
    """
    existing_ts = client.time_series.retrieve_multiple(
        external_ids=ts_external_ids, ignore_unknown_ids=True
    )
    ts_to_create: list[str] = [
        ts
        for ts in ts_external_ids
        if ts not in [existing.external_id for existing in existing_ts]
    ]
    client.time_series.create(
        time_series=[
            TimeSeriesWrite(
                external_id=ts,
                name=ts,
            )
            for ts in ts_to_create
        ]
    )
    idx = pd.date_range(start="2024-01-01", end="2024-11-12", freq="1d")
    generator = np.random.default_rng(random_seed)
    noise = [generator.integers(1, 100, len(idx)) for _ in ts_external_ids]
    df = pd.DataFrame({id: noise[i] for i, id in enumerate(ts_external_ids)}, index=idx)
    client.time_series.data.insert_dataframe(df)
