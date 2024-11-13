import re

from pydantic import BaseModel


class KpiParameter(BaseModel):
    external_id: str
    timeseries: str


class Kpi(BaseModel):
    external_id: str
    formula: str
    parameters: list[KpiParameter]

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
