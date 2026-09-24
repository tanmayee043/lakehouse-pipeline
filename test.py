import requests

resp = requests.get(
    "https://api.open-meteo.com/v1/forecast",
    params={"latitude": 51.5074, "longitude": -0.1278, "hourly": "temperature_2m"},
)
resp.raise_for_status()
print(resp.json()["hourly"]["temperature_2m"][:5])