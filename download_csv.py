import os
from os.path import exists
import requests
import urllib.request
from multiprocessing import Pool

from bs4 import BeautifulSoup


def import_by_years_range(year_range):
    year_range_as_string = ' '.join(map(str, year_range))
    print(f'Worker starting to work on year range : {year_range_as_string}')
    for year in year_range:
        url_dir = f"https://www.ncei.noaa.gov/data/global-hourly/access/{year}/"
        r = requests.get(url_dir)
        data = r.text
        soup = BeautifulSoup(data, features="html5lib")
        for link in soup.find_all('a'):
            os.makedirs(f"./data/{year}", exist_ok=True)
            if link.get('href').endswith(".csv"):
                if exists(f"./data/{year}/{link.string}"):
                    print(f"ALREADY EXISTS {year}/{link.string}")
                else:
                    (r, n) = urllib.request.urlretrieve(url_dir + link.string, filename=f"./data/{year}/{link.string}")
                    print(f"DOWNLOADED: {year}/{link.string}")


def import_by_year(year):
    print(f'Worker starting to work on year range : {year}')
    url_dir = f"https://www.ncei.noaa.gov/data/global-hourly/access/{year}/"
    r = requests.get(url_dir)
    data = r.text
    soup = BeautifulSoup(data, features="html5lib")
    for link in soup.find_all('a'):
        os.makedirs(f"./data/{year}", exist_ok=True)
        if link.get('href').endswith(".csv"):
            if exists(f"./data/{year}/{link.string}"):
                print(f"ALREADY EXISTS {year}/{link.string}")
            else:
                (r, n) = urllib.request.urlretrieve(url_dir + link.string, filename=f"./data/{year}/{link.string}")
                print(f"DOWNLOADED: {year}/{link.string}")


rangings = [[i, f] for (i, f) in zip(range(2020, 2022, 2), range(2019, 2021, 2))]


if __name__ == '__main__':
    pool = Pool(processes=10)
    # pool.map(import_by_years_range, rangings)

    pool.map(import_by_year(2022))

