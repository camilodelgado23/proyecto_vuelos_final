import pandas as pd

# def drop_duplicates(flights):
#     flights = flights.drop_duplicates()
#     return flights

def drop_unnecesary_columns(flights):
    columns_to_remove = ['flt_attendants_per_pass','ground_serv_per_pass','carrier_historical', 'dep_airport_hist', 'day_historical', 'dep_block_hist','previous_airport', 'snwd','ground_serv_per_pass']
    columns_to_remove = [col for col in columns_to_remove if col in flights.columns]
    flights = flights.drop(columns=columns_to_remove)
    return flights

def standarizate_values(flights):
    flights['prcp'] = flights['prcp'] / 10
    flights['tmax'] = flights['tmax'] / 10
    flights['awnd'] = flights['awnd'] / 10
    flights['snow'] = flights['snow'] / 10
    flights['carrier_name'] = flights['carrier_name'].replace("American Eagle Airlines Inc.", "Envoy Air")
    
    return flights

def transform_numerical_to_categorical_data(flights):
    meses = {
        1: 'January', 2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June',
        7: 'July', 8: 'August', 9: 'September', 10: 'October', 11: 'November', 12: 'December'
    }

    dias_semana = {
        1: 'Monday', 2: 'Tuesday', 3: 'Wednesday', 4: 'Thursday', 5: 'Friday',
        6: 'Saturday', 7: 'Sunday'
    }

    flights['month'] = flights['month'].map(meses)
    flights['day_of_week'] = flights['day_of_week'].map(dias_semana)

    #flights = flights.drop(columns=['latitude', 'longitude', 'previous_airport', 'snwd'])
    return flights
