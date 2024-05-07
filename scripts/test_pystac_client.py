from pystac_client import Client
import xarray as xr
import s3fs

# catalog = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1/")
# search_results = catalog.search(
#     collections=["era5-pds"], query={"era5:kind": {"eq": "an"}}
# )

# items = search_results.items()
# print(items)
# for item in items:
#     print(item.to_dict())



# Versione conforme allo standard STAC
client = Client.open('http://127.0.0.1:8083/')  # Replace with the URL of your catalog
client.add_conforms_to("ITEM_SEARCH")
client.add_conforms_to("QUERY")

search_result = client.search(
    collections=["seasonal_forecasts"],
    datetime=["2024-02-01","2024-02-29"]
    # query={"issue_date": {"eq": "202402"},"model": {"eq": 464}}
    )

print("SEARCH RESULT")
print(search_result)
fs_s3 = s3fs.S3FileSystem(anon=True) 

# agg_dataset = None
data_arrays = []
for item in search_result.items():
    print(item)
    asset_href = item.get_assets()['data'].href
    s3_file_obj = fs_s3.open(asset_href, mode='rb')
    
    dataset = xr.open_dataset(s3_file_obj,engine='h5netcdf')

    # add variable model from item properties to dataset
    dataset.coords['model'] = item.properties['model']

    data_arrays.append(dataset)

if data_arrays:
    agg_dataset = xr.concat(data_arrays, dim='model')
    agg_dataset.to_dataframe().to_csv('data.csv')
    print("AGGREGATED DATASET")
    print(agg_dataset)
else:
    print("No data found")


