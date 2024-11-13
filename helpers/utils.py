from datetime import datetime

from models.enums import KpiFrequency


def frequency_to_granularity(
    frequency: KpiFrequency,
    start: datetime | None = None,
    end: datetime | None = None,
):
    match frequency:
        case KpiFrequency.Daily:
            return "1d"
        case KpiFrequency.Monthly:
            return "1mo"
        case KpiFrequency.Yearly:
            return "1y"
        case KpiFrequency.Custom:
            assert end > start, "End must be after start"
            delta = end - start
            return f"{delta.days}d"
        case _:
            return "1d"
