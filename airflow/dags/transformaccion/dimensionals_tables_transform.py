def create_flight_fact(merge_flights_and_airlines):
    flight_fact = merge_flights_and_airlines[[
        'dep_del15', 'distance_group', 'segment_number', 'concurrent_flights',
        'number_of_seats', 'airport_flights_month', 'airline_flights_month',
        'airline_airport_flights_month', 'avg_monthly_pass_airport', 
        'avg_monthly_pass_airline', 'plane_age', 'month', 'day_of_week',
        'dep_time_blk', 'airline_id', 'departing_airport', 'prcp', 'snow', 
        'tmax', 'awnd'
    ]].copy()
    
    # Primero crear dim_weather y obtener sus keys
    dim_weather = create_dim_weather(merge_flights_and_airlines)
    weather_keys = dim_weather['weather_key'].tolist()
    
    # Crear un mapeo de las condiciones climáticas a sus keys
    weather_conditions = merge_flights_and_airlines[['prcp', 'snow', 'tmax', 'awnd']].drop_duplicates()
    weather_conditions['dim_weather_key'] = range(1, len(weather_conditions) + 1)
    
    # Hacer merge con flight_fact para asignar los weather_keys correctos
    flight_fact = flight_fact.merge(
        weather_conditions,
        on=['prcp', 'snow', 'tmax', 'awnd'],
        how='left'
    )
    
    # Crear el resto de las keys
    flight_fact['flight_id'] = range(1, len(flight_fact) + 1)
    flight_fact['dim_date_key'] = flight_fact['month'].astype(str) + '-' + flight_fact['day_of_week'].astype(str) + '-' + flight_fact['dep_time_blk']
    flight_fact['dim_airline_key'] = flight_fact['airline_id']
    
    # Eliminar columnas que ya no necesitamos
    flight_fact = flight_fact.drop(['prcp', 'snow', 'tmax', 'awnd', 'month', 'day_of_week', 'dep_time_blk', 'airline_id'], axis=1)
    
    return flight_fact

def create_dim_date(merge_flights_and_airlines):
    dim_date = merge_flights_and_airlines[['month', 'day_of_week', 'dep_time_blk']].drop_duplicates().copy()
    dim_date['date_key'] = dim_date['month'].astype(str) + '-' + dim_date['day_of_week'].astype(str) + '-' + dim_date['dep_time_blk']
    dim_date = dim_date[['date_key', 'month', 'day_of_week', 'dep_time_blk']]
    print(dim_date.head(5))
    return dim_date

def create_dim_airline(merge_flights_and_airlines):
    dim_airline = merge_flights_and_airlines[[
        'airline_id', 'airline_name', 'icao_code', 'country_iso2', 'status'
    ]].drop_duplicates().copy()
    dim_airline = dim_airline.rename(columns={'airline_id': 'airline_key'})
    print(dim_airline.head(5))
    return dim_airline

def create_dim_weather(merge_flights_and_airlines):
    dim_weather = merge_flights_and_airlines[['prcp', 'snow', 'tmax', 'awnd']].drop_duplicates().copy()
    dim_weather['weather_key'] = range(1, len(dim_weather) + 1)
    dim_weather = dim_weather[['weather_key', 'prcp', 'snow', 'tmax', 'awnd']]
    print(dim_weather.head(5))
    return dim_weather

def create_dim_airport(merge_flights_and_airlines):
    dim_airport = merge_flights_and_airlines[['departing_airport','latitude','longitude']].drop_duplicates().copy()
    dim_airport['airport_key'] = range(1, len(dim_airport) + 1)
    dim_airport = dim_airport[['airport_key', 'departing_airport','latitude','longitude']]
    print(dim_airport.head(5))
    return dim_airport



def create_dim_airport_foreign_key_on_fact_table(dim_airport, flight_fact):
    # Realizar el merge de flight_fact con dim_airport
    flight_fact = flight_fact.merge(dim_airport, left_on='departing_airport', right_on='departing_airport', how='left')
    
    # Eliminar la columna original y renombrar airport_key a dim_airport_key
    flight_fact = flight_fact.drop(columns=['departing_airport']).rename(columns={'airport_key': 'dim_airport_key'})
    
    # Asegurarse de que la columna dim_airport_key esté en flight_fact
    flight_fact = flight_fact[[
        'flight_id', 'dep_del15', 'distance_group', 'segment_number', 
        'concurrent_flights', 'number_of_seats', 'airport_flights_month', 
        'airline_flights_month', 'airline_airport_flights_month', 
        'avg_monthly_pass_airport', 'avg_monthly_pass_airline', 'plane_age', 
        'dim_date_key', 'dim_airline_key', 'dim_airport_key', 'dim_weather_key'
    ]]
    return flight_fact

# def create_dim_airport_foreign_key_on_fact_table(dim_airport, flight_fact):
#     flight_fact = flight_fact.merge(dim_airport, on='departing_airport', how='left')
#     flight_fact = flight_fact.drop(columns=['departing_airport'])
#     flight_fact = flight_fact[[
#         'flight_id', 'dep_del15', 'distance_group', 'segment_number', 
#         'concurrent_flights', 'number_of_seats', 'airport_flights_month', 
#         'airline_flights_month', 'airline_airport_flights_month', 
#         'avg_monthly_pass_airport', 'avg_monthly_pass_airline', 'plane_age', 
#         'dim_date_key', 'dim_airline_key', 'dim_airport_key', 'dim_weather_key'
#     ]]
#     return flight_fact