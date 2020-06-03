import json
import csv
import requests
from datetime import datetime
req = requests.get('https://api.covidindiatracker.com/state_data.json')
url_data = req.text
data = json.loads(url_data)
covid_data = []
date = datetime.today().strftime('%Y-%m-%d')
for state in data:
    covid_data.append([date, state.get('state'), state.get('aChanges')])
with open("covid_data_{}.csv".format(date), "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerows(covid_data)