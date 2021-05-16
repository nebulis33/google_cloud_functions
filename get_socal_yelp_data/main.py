import requests
import csv
import os
from datetime import datetime
from google.cloud import storage

socal_cities = ['Los Angeles, CA', 'Long Beach, CA', 'Irvine, CA', 'Santa Ana, CA', 'Anaheim, CA', 'Santa Monica, CA',
        'Burbank, CA', 'Malibu, CA', 'Thousand Oaks, CA', 'Santa Clarita, CA', 'Ontaria, CA', 'West Covina, CA',
        'Riverside, CA', 'San Bernardino, CA', 'Pasadena, CA', 'Huntington Beach, CA']
header = {'Authorization':os.environ['API_KEY']}
date = datetime.now().strftime("%m_%d_%Y")
file_name = "socal_boba_" + date + ".csv"

def write_places(record, file):
    id = record['id']
    alias = record['alias']
    name = record['name']
    categories = ''
    for category in record['categories']:
        categories = categories + (category['alias'] + ' ')
    closed = record['is_closed']
    review_count = record['review_count']
    rating = record['rating']
    address = record['location']['address1']
    city = record['location']['city']
    zip_code = record['location']['zip_code']
    with open(file, 'a', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([id, alias, name, categories, closed, review_count, rating, address, city, zip_code])

def search(url, header_vals, locations, file):
    field_names = ['id', 'alias', 'name', 'categories', 'is_closed', 'review_count', 'rating', 'address', 'city', 'zip_code']
    with open(file, 'w', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(field_names)
    for city in locations:
        offset = 0
        while offset < 1000:
            param_vals = {'categories':'bubbletea', 'location':city, 'limit':50, 'offset': offset}
            r = requests.get(url, params=param_vals, headers=header_vals)
            results = r.json()
            if not results['businesses']:
                break
            else:
                for place in results['businesses']:
                    write_places(place, file)
                offset += 50

def query_yelp(data, context):
  search(os.environ['URL'], header, socal_cities, "/tmp/socal_boba.csv")
  storage_client = storage.Client()
  storage_client.get_bucket('socal_boba_bucket_dirty').blob(file_name).upload_from_filename('/tmp/socal_boba.csv', content_type='text/csv')