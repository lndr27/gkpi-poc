from enum import StrEnum


class KpiFrequency(StrEnum):
    Daily = "Daily"
    Monthly = "Monthly"
    Quarterly = "Quarterly"
    Yearly = "Yearly"
    Custom = "Custom"
