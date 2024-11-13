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
        case KpiFrequency.Quarterly:
            return "1q"
        case KpiFrequency.Yearly:
            return "1y"
        case KpiFrequency.Custom:
            assert start and end, "start and end must be provided for custom frequency"
            assert end > start, "End must be after start"
            delta = end - start
            return f"{delta.days}d"
