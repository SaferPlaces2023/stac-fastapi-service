from pystac_client import Client

client = Client.open('http://127.0.0.1:8083/')  # Replace with the URL of your catalog
client.add_conforms_to("ITEM_SEARCH")

search_result = client.search()

print("SEARCH RESULT")
print(search_result)
for item in search_result.items():
    print("ITEM")
    print(item.to_dict())