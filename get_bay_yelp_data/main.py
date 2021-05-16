import requests
import csv
import os
from datetime import datetime
from google.cloud import storage

bay_cities = ['Santa Rosa, CA', 'Novato, CA', 'San Rafael, CA', 'San Francisco, CA', 'San Mateo, CA',
        'Palo Alto, CA', 'San Jose, CA', 'Fremont, CA', 'Hayward, CA', 'Oakland, CA',
        'Richmond, CA', 'Concord, CA', 'Vallejo, CA', 'Napa, CA', 'Pleasanton, CA',
        'Walnut Creek, CA', 'Santa Rosa, CA', 'Fairfield, CA', 'Vacaville, CA', 'Gilroy, CA', 'Antioch, CA']
header = {'Authorization':os.environ['API_KEY']}
date = datetime.now().strftime("%m_%d_%Y")
file_name = "bay_area_boba_" + date + ".csv"

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
  search(os.environ['URL'], header, bay_cities, "/tmp/bay_area_boba.csv")
  storage_client = storage.Client()
  storage_client.get_bucket('bay_boba_bucket_dirty').blob(file_name).upload_from_filename('/tmp/bay_area_boba.csv', content_type='text/csv')