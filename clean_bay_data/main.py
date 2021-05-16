import pandas as pd
import fsspec
import gcsfs
from google.cloud import storage


def clean_csv(data, context):
    dirty_file_name = data['name']
    cleaned_file_name = "cleaned_" + dirty_file_name
    dirty_file_path = 'gs://bay_boba_bucket_dirty/{}'.format(dirty_file_name)

    prohibited = ["Sacramento", "West Sacramento", "Elk Grove", "Woodland", "Davis"]

    storage_client = storage.Client()

    df = pd.read_csv(dirty_file_path)

    # remove duplicates
    df.drop_duplicates(subset=None, inplace=True)

    # remove records from cities not in the area
    np = df[~df.city.isin(prohibited)]

    # remove records that are panda express locations or food trucks
    nf = np[np.categories.str.contains("foodtrucks")==False]
    new_df = nf[nf.alias.str.contains("panda-express")==False]

    # upload to the clean bucket
    new_df.to_csv('/tmp/temp_data.csv', index=False, encoding='utf-8')
    storage_client.get_bucket('bay_boba_bucket_clean').blob(cleaned_file_name).upload_from_filename('/tmp/temp_data.csv', content_type='text/csv')