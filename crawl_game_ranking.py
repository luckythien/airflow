import imp
from bs4 import BeautifulSoup
import requests
import pandas as pd
import datetime

url = 'https://steamdb.info/stats/gameratings/'
r = requests.get(url)
pageText = r.text
print(pageText)
