from datetime import datetime
from itertools import chain

from cognite.client.data_classes import DatapointsQuery
from zoneinfo import ZoneInfo

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

    kpi_frequency = KpiFrequency.Yearly
    calculate_kpi(
        KPIS,
        start,
        end,
        kpi_frequency,
    )

    start = datetime(2024, 1, 1, tzinfo=ZoneInfo("CST6CDT"))  # CLK Timezone
    end = datetime(2024, 1, 15, tzinfo=ZoneInfo("CST6CDT"))  # CLK Timezone
    kpi_frequency = KpiFrequency.Custom
    calculate_kpi(
        KPIS,
        start,
        end,
        kpi_frequency,
    )


def calculate_kpi(
    kpis: list[Kpi], start: datetime, end: datetime, kpi_frequency: KpiFrequency
):
    unique_timeseries_external_ids = list(
        set(chain(*[k.parameters_timeseries for k in kpis]))
    )

    df = client.time_series.data.retrieve_dataframe(
        external_id=unique_timeseries_external_ids,
        start=start,
        end=end,
        timezone=start.tzinfo,
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

    # Calculate the KPIs values foro all the dates at once using pandas
    for kpi in KPIS:
        df[kpi.external_id] = eval(kpi.parsed_expression)

    df["timestamp"] = df.index.tz_localize(None)
    df.to_excel(
        f"KPI_{kpi_frequency}_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.xlsx",
        index=False,
    )
    print(df.head())


if __name__ == "__main__":
    main()

    # from helpers.create_random_timeseries import create_random_timeseries
    # unique_timeseries_external_ids = list(
    #     set(chain(*[k.parameters_timeseries for k in KPIS]))
    # )
    # create_random_timeseries(client, unique_timeseries_external_ids, RANDOM_SEED)
