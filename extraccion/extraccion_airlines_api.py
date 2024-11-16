import requests
import pandas as pd

def fetch_airplane_data(limit=100000):
    url = "https://api.aviationstack.com/v1/airlines"  
    params = {
        'access_key': 'c59b77d090294ff5a942b30c8009de40',
        'limit': limit  
    }
    response = requests.get(url, params=params)
    response.raise_for_status()  
    return response.json().get('data', [])

def process_airplane_data(airplanes):
    df_api = pd.DataFrame(airplanes)
    
    # columns_to_extract = [
    #     'registration_number',
    #     'production_line',
    #     'iata_type',
    #     'model_name',
    #     'model_code',
    #     'icao_code_hex',
    #     'iata_code_short',
    #     'construction_number',
    #     'test_registration_number',
    #     'rollout_date',
    #     'first_flight_date',
    #     'delivery_date',
    #     'registration_date',
    #     'line_number',
    #     'plane_series',
    #     'airline_iata_code',
    #     'airline_icao_code',
    #     'plane_owner',
    #     'engines_count',
    #     'engines_type',
    #     'plane_age',
    #     'plane_status',
    #     'plane_class'
    # ]

    return df_api
    #[columns_to_extract]

def save_to_csv(df, filename='api_airlines.csv'):
    df.to_csv(filename, index=False)
    print(f"Archivo guardado como: {filename}")

def main():
    try:
        airplanes_data = fetch_airplane_data()
        processed_airplanes = process_airplane_data(airplanes_data)
        save_to_csv(processed_airplanes)
    except requests.RequestException as e:
        print(f"Error en la solicitud de la API: {e}")

if __name__ == "__main__":
    main()

