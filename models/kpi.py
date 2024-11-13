import re

from pydantic import BaseModel

from models.enums import KpiFrequency


class KpiParameter(BaseModel):
    external_id: str
    timeseries: str


class Kpi(BaseModel):
    external_id: str
    formula: str
    parameters: list[KpiParameter]
    daily_timeseries: str
    monthly_timeseries: str
    quarterly_timeseries: str
    yearly_timeseries: str

    @property
    def parameters_timeseries(self):
        return [p.timeseries for p in self.parameters]

    @property
    def parameter_to_timeseries_map(self):
        return {p.external_id: p.timeseries for p in self.parameters}

    @property
    def parsed_expression(self):
        pattern = r"\{([^|]+)\|([^}]+)\}"
        map_param_to_timeseries = self.parameter_to_timeseries_map

        def replace_match(match):
            param, aggregate = match.groups()
            if param in map_param_to_timeseries:
                return str(f"df['{map_param_to_timeseries[param]}|{aggregate}']")
            else:
                return match.group(0)

        result = re.sub(pattern, replace_match, self.formula)
        return result

    def get_timeseries_by_frequency(self, kpi_frequency: KpiFrequency):
        match kpi_frequency:
            case KpiFrequency.Daily:
                return self.daily_timeseries
            case KpiFrequency.Monthly:
                return self.monthly_timeseries
            case KpiFrequency.Quarterly:
                return self.quarterly_timeseries
            case KpiFrequency.Yearly:
                return self.yearly_timeseries
