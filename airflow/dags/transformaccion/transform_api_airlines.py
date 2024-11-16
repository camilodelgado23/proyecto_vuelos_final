def filter_ByUnitedState(api_airplanes):
    api_airplanes = api_airplanes[api_airplanes['country_name'].str.contains('United States', case=False, na=False)]
    return api_airplanes

def columns_to_remove(api_airplanes):
    columns_to_remove=["callsign","type" , "fleet_size" , "iata_prefix_accounting", "date_founded", "iata_code" , "hub_code", "fleet_average_age"]
    api_airplanes = api_airplanes.drop(columns=columns_to_remove)
    return api_airplanes

def inputation_nulls(api_airplanes):
    api_airplanes['icao_code'] = api_airplanes['icao_code'].fillna('000')
    api_airplanes.loc[(api_airplanes['country_name'] == 'United States') & (api_airplanes['country_iso2'].isnull()), 'country_iso2'] = 'US'
    api_airplanes.loc[(api_airplanes['country_name'] == 'United States Minor Outlying Islands') & (api_airplanes['country_iso2'].isnull()), 'country_iso2'] = 'UM'
    api_airplanes.loc[(api_airplanes['airline_name'] == 'Hemmeter Aviation') & (api_airplanes['country_iso2'] == 'UK'), 'country_iso2'] = 'US'
    return api_airplanes

