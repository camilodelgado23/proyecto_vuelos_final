import pandas as pd

def normalize_airline_name(name):
    name = name.lower()
    name = name.replace('inc', '').replace('co.', '').replace('airways', 'airway').replace('lines', 'line').replace('airlines', 'airline').replace('llc', '').replace('.', '').replace(',', '').strip()
    return name


def standardize_name(name):
    name = name.lower().strip()
    
    replacements = {
        'spirit air line': 'spirit',
        'air spirit': 'spirit',
        'united air line': 'united airline',
        'united airline': 'united airline',
        'midwest airline': 'midwest airline',
        'trans midwest airline': 'midwest airline',
        'jetblue': 'jetblue airway',
        'comair': 'comair',
        'delta connection comair': 'comair',
        'envoy air': 'envoy air'
    }
    
    if name in replacements:
        name = replacements[name]
    
    return name

def standarizate_airline_name(flights, api_airplanes):
    flights['carrier_name'] = flights['carrier_name'].apply(normalize_airline_name)
    api_airplanes['airline_name'] = api_airplanes['airline_name'].apply(normalize_airline_name)
    flights['carrier_name'] = flights['carrier_name'].apply(standardize_name)
    api_airplanes['airline_name'] = api_airplanes['airline_name'].apply(standardize_name)
    return flights , api_airplanes
    

def merge_api_and_postgres_dataset(flights , api_airplanes):
    merged_df = pd.merge(flights, api_airplanes, left_on='carrier_name', right_on='airline_name', how='inner')
    merged_df = merged_df[~merged_df['icao_code'].str.startswith('0')]

    return merged_df

def delete_merged_unnecesary_columns(merged_df):
    api_airplanes = merged_df.drop(columns="carrier_name")
    return merged_df
    