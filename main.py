from datetime import datetime
from itertools import chain

import pandas as pd
from cognite.client.data_classes import DatapointsQuery
from zoneinfo import ZoneInfo

from helpers.create_random_timeseries import create_kpis_timeseries
from helpers.utils import frequency_to_granularity
from infra.cognite_client_factory import create_cognite_client
from models.enums import KpiFrequency
from models.kpi import Kpi, KpiParameter

client = create_cognite_client()

RANDOM_SEED = 123456

# This should be retrieved from the data model
KPIS = [
    Kpi(
        external_id="Batatinha",
        formula="({A|average}*{B|sum})/{C|sum}",
        daily_timeseries="TEST:GKPI:SITE_A:UNIT_A:SEGMENT_A:KPI:Batatinha:DAILY",
        monthly_timeseries="TEST:GKPI:SITE_A:UNIT_A:SEGMENT_A:KPI:Batatinha:MONTHLY",
        quarterly_timeseries="TEST:GKPI:SITE_A:UNIT_A:SEGMENT_A:KPI:Batatinha:QUARTERLY",
        yearly_timeseries="TEST:GKPI:SITE_A:UNIT_A:SEGMENT_A:KPI:Batatinha:YEARLY",
        parameters=[
            KpiParameter(
                external_id="A",
                timeseries="TEST:GKPI:SITE_A:UNIT_A:SEGMENT_A:PARAM:A",
            ),
            KpiParameter(
                external_id="B",
                timeseries="TEST:GKPI:SITE_A:UNIT_A:SEGMENT_A:PARAM:B",
            ),
            KpiParameter(
                external_id="C",
                timeseries="TEST:GKPI:SITE_A:UNIT_A:SEGMENT_A:PARAM:C",
            ),
        ],
    ),
    Kpi(
        external_id="Frita",
        formula="({A|sum}+{E|sum})*{D|sum}",
        daily_timeseries="TEST:GKPI:SITE_A:UNIT_A:SEGMENT_A:KPI:Frita:DAILY",
        monthly_timeseries="TEST:GKPI:SITE_A:UNIT_A:SEGMENT_A:KPI:Frita:MONTHLY",
        quarterly_timeseries="TEST:GKPI:SITE_A:UNIT_A:SEGMENT_A:KPI:Frita:QUARTERLY",
        yearly_timeseries="TEST:GKPI:SITE_A:UNIT_A:SEGMENT_A:KPI:Frita:YEARLY",
        parameters=[
            KpiParameter(
                external_id="A",
                timeseries="TEST:GKPI:SITE_A:UNIT_A:SEGMENT_A:PARAM:A",
            ),
            KpiParameter(
                external_id="D",
                timeseries="TEST:GKPI:SITE_A:UNIT_A:SEGMENT_A:PARAM:D",
            ),
            KpiParameter(
                external_id="E",
                timeseries="TEST:GKPI:SITE_A:UNIT_A:SEGMENT_A:PARAM:E",
            ),
        ],
    ),
]
# ==================================================


def main():
    create_kpis_timeseries(client, KPIS)
    start = datetime(2024, 1, 1, tzinfo=ZoneInfo("CST6CDT"))  # CLK Timezone
    end = datetime(2024, 11, 13, tzinfo=ZoneInfo("CST6CDT"))  # CLK Timezone

    kpi_frequency = KpiFrequency.Daily
    calculate_kpi(
        KPIS,
        start,
        end,
        kpi_frequency,
    )

    kpi_frequency = KpiFrequency.Monthly
    calculate_kpi(
        KPIS,
        start,
        end,
        kpi_frequency,
    )

    kpi_frequency = KpiFrequency.Quarterly
    calculate_kpi(
        KPIS,
        start,
        end,
        kpi_frequency,
    )

    kpi_frequency = KpiFrequency.Yearly
    calculate_kpi(
        KPIS,
        start,
        end,
        kpi_frequency,
    )

    kpi_frequency = KpiFrequency.Custom
    start = datetime(2024, 1, 1, tzinfo=ZoneInfo("CST6CDT"))  # CLK Timezone
    end = datetime(2024, 1, 15, tzinfo=ZoneInfo("CST6CDT"))  # CLK Timezone
    calculate_kpi(
        KPIS,
        start,
        end,
        kpi_frequency,
    )


def calculate_kpi(
    kpis: list[Kpi], start: datetime, end: datetime, kpi_frequency: KpiFrequency
):
    assert start.tzinfo, "Date must have a timezone"
    unique_timeseries_external_ids = list(
        set(chain(*[k.parameters_timeseries for k in kpis]))
    )

    df = client.time_series.data.retrieve_dataframe(
        external_id=unique_timeseries_external_ids,
        start=start,
        end=end,
        timezone=start.tzinfo,  # type: ignore
        granularity=frequency_to_granularity(kpi_frequency, start, end),
        aggregates=["sum", "average"],
    )

    # It's even possible to have a more fined grained query and get only the
    # necessary data even for multiple granularities at once
    #
    # include_granularity_name=True,
    # external_id=list(chain(*(
    #     [
    #         DatapointsQuery(
    #             external_id=id,
    #             granularity="1mo",
    #             aggregates=["sum","average","min","max"],
    #         ),
    #         DatapointsQuery(
    #             external_id=id,
    #             granularity="1w",
    #             aggregates=["sum","average","min","max"],
    #         )
    #     ]
    #     for id in unique_timeseries_external_ids
    # ))),

    # Calculate the KPIs values for all the dates at once using pandas
    kpis_column_names: list[str] = []
    for kpi in KPIS:
        column_name = kpi.get_timeseries_by_frequency(kpi_frequency) or kpi.external_id
        kpis_column_names.append(column_name)
        df[column_name] = eval(kpi.parsed_expression)

    # Export parameters and calculated kpis to excel file
    df["timestamp"] = df.index.tz_localize(None)  # type: ignore
    df.to_excel(
        f"outputs/KPI_{kpi_frequency}_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.xlsx",
        index=False,
    )

    # Save Kpis to timeseries, skipping custom frequency
    if kpi_frequency != KpiFrequency.Custom:
        new_df = df.loc[:, kpis_column_names]
        client.time_series.data.insert_dataframe(new_df)


if __name__ == "__main__":
    main()

    # Create random timeseries for parameters
    # from helpers.create_random_timeseries import create_random_timeseries
    # unique_timeseries_external_ids = list(
    #     set(chain(*[k.parameters_timeseries for k in KPIS]))
    # )
    # create_random_timeseries(client, unique_timeseries_external_ids, RANDOM_SEED)
