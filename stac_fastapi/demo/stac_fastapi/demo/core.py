"""Item crud client."""
import json
from typing import Union, Optional, List, Type
from urllib.parse import urljoin
from bson.json_util import dumps
import attr
from datetime import datetime, timedelta
from stac_fastapi.demo import serializers
from pydantic import ValidationError
from stac_fastapi.demo.types.error_checks import ErrorChecks
from stac_fastapi.types.search import BaseSearchPostRequest

from stac_fastapi.demo.config import MongoSettings
from fastapi import HTTPException
from stac_fastapi.types.core import BaseCoreClient
from stac_fastapi.types.stac import Collection, Collections, Item, ItemCollection, LandingPage
from fastapi import Request
from stac_pydantic.links import Relations
from stac_pydantic.shared import MimeTypes

from stac_pydantic.shared import BBox
import xarray as xr
import s3fs

NumType = Union[float, int]



def generate_month_list(start_date, end_date):
    # Parse start and end dates
    # format like 2021-01-19T00:00:00Z
    date_format = "%Y-%m-%dT%H:%M:%SZ"
    start = datetime.strptime(start_date, date_format)
    end = datetime.strptime(end_date, date_format)

    # Initialize list to store formatted dates
    month_list = []

    # Iterate through each month between start and end dates
    while start <= end:
        # Format current date as YYYYMM and append to list
        month_list.append(start.strftime("%Y%m"))
        # Move to the next month
        start += timedelta(days=32)
        start = start.replace(day=1)

    return month_list


@attr.s
class CoreCrudClient(BaseCoreClient):
    """Client for core endpoints defined by stac."""
    settings = MongoSettings()
    client = settings.create_client
    item_table = client.stac.stac_item
    collection_table = client.stac.stac_collection
    # error_check = ErrorChecks(client=client)
    # item_serializer: Type[serializers.Serializer] = attr.ib(
    #     default=serializers.ItemSerializer
    # )
    # collection_serializer: Type[serializers.Serializer] = attr.ib(
    #     default=serializers.CollectionSerializer
    # )

    def landing_page(self, **kwargs) -> LandingPage:
        request: Request = kwargs["request"]
        base_url = str(request.base_url)
        extension_schemas = [
            schema.schema_href for schema in self.extensions if schema.schema_href
        ]
        landing_page = self._landing_page(
            base_url=base_url,
            conformance_classes=self.conformance_classes(),
            extension_schemas=extension_schemas,
        )
        # Add Collections links
        collections = self.all_collections(request=kwargs["request"])
        for collection in collections:
            landing_page["links"].append(
                {
                    "rel": Relations.child.value,
                    "type": MimeTypes.json.value,
                    "title": collection.get("title") or collection.get("id"),
                    "href": urljoin(base_url, f"collections/{collection['id']}"),
                }
            )
        
        # Add OpenAPI URL
        landing_page["links"].append(
            {
                "rel": "service-desc",
                "type": "application/vnd.oai.openapi+json;version=3.0",
                "title": "OpenAPI service description",
                "href": urljoin(base_url, request.app.openapi_url.lstrip("/")),
            }
        )
        
        # Add human readable service-doc
        landing_page["links"].append(
            {
                "rel": "service-doc",
                "type": "text/html",
                "title": "OpenAPI service documentation",
                "href": urljoin(base_url, request.app.docs_url.lstrip("/")),
            }
        )
        return landing_page

    def all_collections(self, **kwargs) -> Collections:
        """Read all collections from the database."""
        collections = self.collection_table.find()
        collections = [json.loads(dumps(collection)) for collection in collections]
        return collections

    def get_collection(self, collection_id: str, **kwargs) -> Collection:
        """Get collection by id."""
        collection = self.collection_table.find_one({"id": collection_id})
        return json.loads(dumps(collection))

    def item_collection(
        self, collection_id: str, limit: int = 10, token: str = None, **kwargs
    ) -> ItemCollection:
        """Read an item collection from the database."""
        collection_children = (
            self.item_table.find({"collection": collection_id})
        )
        items = [json.loads(dumps(item)) for item in collection_children]

        return {
            "type": "FeatureCollection",
            "features": items
        }

    def get_item(self, item_id: str, collection_id: str, **kwargs) -> Item:
        """Get item by item id, collection id."""
        item = self.item_table.find_one({"id": item_id, "collection": collection_id})
        return json.loads(dumps(item))

    def get_search(
        self,
        collections: Optional[List[str]] = None,
        ids: Optional[List[str]] = None,
        bbox: Optional[List[NumType]] = None,
        datetime: Optional[Union[str, datetime]] = None,
        limit: Optional[int] = 10,
        query: Optional[str] = None,
        token: Optional[str] = None,
        fields: Optional[List[str]] = None,
        sortby: Optional[str] = None,
        **kwargs,
    ) -> ItemCollection:
        """GET search catalog."""
        base_args = {
            "collections": collections,
            "ids": ids,
            "bbox": bbox,
            "limit": limit,
            "token": token,
            "datetime": datetime,
            "query": json.loads(query) if query else query,
        }
        pass

    def post_search(
        self, search_request: BaseSearchPostRequest, **kwargs
    ) -> ItemCollection:
        """POST search catalog."""
        base_url = str(kwargs["request"].base_url)
        queries = {}

        queries.update({"collection": {"$in": search_request.collections}})

        if search_request.intersects:
            intersect_filter = {
                "geometry": {
                    "$geoIntersects": {
                        "$geometry": {
                            "type": search_request.intersects.type,
                            "coordinates": search_request.intersects.coordinates,
                        }
                    }
                }
            }
            queries.update(**intersect_filter)


        print("DATETIME")
        print(search_request.datetime)
        str_start_date = search_request.datetime.split("/")[0]
        str_end_date = search_request.datetime.split("/")[1]
        print(str_start_date)
        print(str_end_date)

        parsed_dates = generate_month_list(str_start_date, str_end_date)
        print(parsed_dates)
        print("+++++++++++++++++++++++++++++++++++++")
        # # parse date format like 2021-01-19T00:00:00Z
        # date_format = "%Y-%m-%dT%H:%M:%SZ"
        # start_date = datetime.strptime(str_start_date,date_format)
        # end_date = datetime.strptime(str_end_date,date_format)
        print("---")
        if search_request.datetime:
            datetime_filter = {
                "properties.issue_date": { "$in": parsed_dates }
            }
            print("QUERY")
            print(datetime_filter)
            queries.update(**datetime_filter)

        if search_request.query:
            if type(search_request.query) == str:
                search_request.query = json.loads(search_request.query)
            for (field_name, expr) in search_request.query.items():
                field = "properties." + field_name
                for (op, value) in expr.items():
                    key_filter = {field: {f"${op}": value}}
                    queries.update(**key_filter)

        if search_request.bbox:
            bbox_filter = {
                "bbox": {
                    "$geoWithin": {
                        "$geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [search_request.bbox[0], search_request.bbox[1]],
                                    [search_request.bbox[2], search_request.bbox[1]],
                                    [search_request.bbox[2], search_request.bbox[3]],
                                    [search_request.bbox[0], search_request.bbox[3]],
                                    [search_request.bbox[0], search_request.bbox[1]]
                                ]
                            ]
                        }
                    }
                }
            }
            queries.update(**bbox_filter)

        results = (self.item_table.find(queries).limit(search_request.limit))

        items = [json.loads(dumps(item)) for item in results]

        return ItemCollection(
            type="FeatureCollection",
            features=items
        )

    
    def test_endpoint() -> str:
        return "Hello World"


    # def post_search(
    #     self, search_request: BaseSearchPostRequest, **kwargs
    # ) -> ItemCollection:
    #     """POST search catalog."""
    #     base_url = str(kwargs["request"].base_url)
    #     queries = {}

    #     queries.update({"collection": {"$in": search_request.collections}})

    #     if search_request.intersects:
    #         intersect_filter = {
    #             "geometry": {
    #                 "$geoIntersects": {
    #                     "$geometry": {
    #                         "type": search_request.intersects.type,
    #                         "coordinates": search_request.intersects.coordinates,
    #                     }
    #                 }
    #             }
    #         }
    #         queries.update(**intersect_filter)

    #     if search_request.query:
    #         if type(search_request.query) == str:
    #             search_request.query = json.loads(search_request.query)
    #         for (field_name, expr) in search_request.query.items():
    #             field = "properties." + field_name
    #             for (op, value) in expr.items():
    #                 key_filter = {field: {f"${op}": value}}
    #                 queries.update(**key_filter)

    #     if search_request.bbox:
    #         bbox_filter = {
    #             "bbox": {
    #                 "$geoWithin": {
    #                     "$geometry": {
    #                         "type": "Polygon",
    #                         "coordinates": [
    #                             [
    #                                 [search_request.bbox[0], search_request.bbox[1]],
    #                                 [search_request.bbox[2], search_request.bbox[1]],
    #                                 [search_request.bbox[2], search_request.bbox[3]],
    #                                 [search_request.bbox[0], search_request.bbox[3]],
    #                                 [search_request.bbox[0], search_request.bbox[1]]
    #                             ]
    #                         ]
    #                     }
    #                 }
    #             }
    #         }
    #         queries.update(**bbox_filter)

    #     results = (self.item_table.find(queries).limit(search_request.limit))

    #     items = [json.loads(dumps(item)) for item in results]
    #     print("ITEMS")
    #     print(items)
    #     print("...")
    #     fs_s3 = s3fs.S3FileSystem(anon=True) 

    #     # read nc files and return the data in assets field
    #     for item in items:
    #         print(item['assets'])
    #         print("--")
    #         if item["assets"]:
    #             file_nc = item["assets"]['data']['href']
    #             s3_file_obj = fs_s3.open(file_nc, mode='rb')

    #             data = xr.open_dataset(s3_file_obj,engine='h5netcdf')
    #             print(data)

    #             # cast time to string
    #             data['time'] = data['time'].astype(str)

    #             item['assets']['data']['values'] = data.to_dict()
        
    #     print("ITEMS")
    #     print(items)

    #     return ItemCollection(
    #         type="FeatureCollection",
    #         features=items
    #     )
