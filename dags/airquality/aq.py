from urllib import request
import json
import polars as pl
from urllib import error

def download_air_quality_index(output_path,**context):
    output_path=output_path
    execution_date = context['execution_date'].strftime('%Y-%m-%d-%H')
    try:
        path="https://api.data.gov.in/resource/3b01bcb8-0b14-4abf-b6f2-c1fd384ba69?api-key=579b46db66ec23bdd000001c8cd78fa5a9a4e6c77f9b888a65d8fc&format=json&limit=1000"
        response=request.urlopen(path)
        if response.getcode()==200:
            content = response.read()
            decoded_string = content.decode('utf-8')
            data = json.loads(decoded_string)
            df=pl.DataFrame(data['records'])
        else:
            print(f"Request failed with status code: {response.getcode()}")
    except error.URLError as e:
        print(f"Error occurred: {e}")
    print(f"count of data {df.shape}")
    print(output_path+execution_date)
    df.write_parquet(output_path+execution_date)
