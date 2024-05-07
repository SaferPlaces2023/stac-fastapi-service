# Basic flask app to serve the EED climate data
# """
from flask import Flask, request, jsonify
from pystac_client import Client
import xarray as xr
import s3fs
import json


def xarray_to_prs_coverage_json(dataset):
    # Extract necessary information from xarray dataset
    latitudes = dataset.geo_y.values.tolist()
    longitudes = dataset.geo_x.values.tolist()
    altitudes = dataset.geo_z.values.tolist()
    times = dataset.time.values.tolist()
    data_vars = list(dataset.data_vars)

    # Construct PRS.Coverage+JSON structure
    coverage_json = {
        "type": "Coverage",
        "domain": {
            "type": "Domain",
            "domainType": "MultiExtentCoverage",
            "axes": {
                "x": {"values": longitudes},    #"bounds": [min(longitudes), max(longitudes)], "num": len(longitudes)},
                "y": {"values": latitudes},     #"bounds": [min(latitudes), max(latitudes)], "num": len(latitudes)},
                "z": {"values": altitudes},     #"bounds": [min(altitudes), max(altitudes)], "num": len(altitudes)},
                "t": {"values": times},         #"bounds": [min(times), max(times)], "num": len(times)}
            }
        },
        "parameters": {}
    }

    print("DATA VARS")
    print(data_vars)
    # Add data variables to PRS.Coverage+JSON structure
    for var in data_vars:
        coverage_json["parameters"][var] = {
            "type": "Parameter",
            "description": var,
            "unit": "Unknown",
            "values": dataset[var].values.tolist()
        }

    # return json.dumps(coverage_json)
    return coverage_json





app = Flask(__name__)

@app.route('/climate_data', methods=['GET'])
def get_climate_data():
    # Get the query parameters
    query_params = request.args
    print(query_params)
    # Get the data from the database
    # data = get_data_from_database(query_params)
    
    data = {
        "temperature": 25,
        "humidity": 50,
        "wind_speed": 10,
        "precipitation": 0.5
    }
    return jsonify(data)


# OGC API complaint cube endpoint
@app.route('/collections/<collection_id>/cube', methods=['GET'])
def get_cube(collection_id):
    '''Return the data in the OGC API complaint cube format
    parameter-name:     Parameter (COUT)
    bbox:               Bounding Box comma separated values (minx,miny,maxx,maxy)
    z:                  Vertical Extent (1000/300)
    datetime:           Time Extent (2021-01-19T06:00:00/2021-01-20T06:00:00)
    f:                  Output format (json, csv, netcdf, etc.)
    '''
    parameter_name = None
    bbox = None
    z = None
    datetime = None
    f = None

    # Get the query parameters
    if 'parameter-name' in request.args:
        parameter_name = request.args['parameter-name']
    if 'bbox' in request.args:
        bbox = request.args['bbox']
    if 'z' in request.args:
        z = request.args['z']
    if 'datetime' in request.args:
        datetime = request.args['datetime']
    if 'f' in request.args:
        f = request.args['f']
    
    print("Query parameters")
    print("parameter-name",parameter_name)
    print("bbox",bbox)
    print("z",z)
    print("datetime",datetime)
    print("f",f)
    # Versione conforme allo standard STAC
    client = Client.open('http://127.0.0.1:8083/')  # Replace with the URL of your catalog
    client.add_conforms_to("ITEM_SEARCH")
    client.add_conforms_to("QUERY")

    search_result = client.search(collections=[collection_id],bbox=bbox,datetime=datetime)  # query={"issue_date": {"eq": "202402"},"model": {"eq": 464}})

    print("SEARCH RESULT")
    print(search_result)
    fs_s3 = s3fs.S3FileSystem(anon=True) 

    # agg_dataset = None
    data_arrays = []
    for item in search_result.items():
        print(item)
        item_props = item.to_dict().get('properties')
        print("-------------------")
        print(item_props)
        print("-------------------")
        asset_href = item.get_assets()['data'].href
        s3_file_obj = fs_s3.open(asset_href, mode='rb')
        
        dataset = xr.open_dataset(s3_file_obj,engine='h5netcdf')

        # rename COUT variable to COUT_<model>
        dataset = dataset.rename_vars({ 'COUT': f'COUT_{item_props["model"]}' })

        # # add variable model from item properties to dataset
        # dataset.coords['model'] = item.properties['model']

        data_arrays.append(dataset)

    # TODO: Remove model dimension and create N variables named COUT_<model_1>, COUT_<model_2>, etc.
    # and return the data requested in the parameter-name query parameter

    # TODO: https://developer.ogc.org/api/edr/index.html#tag/Collection-data-queries/operation/GetDataForCube
    if data_arrays:
        agg_dataset = xr.merge(data_arrays, join='outer')
        print("+++++++++++++++++++++++++++++++++++++++++++++++++++++")
        print(agg_dataset)
        print(agg_dataset.dims)
        # agg_dataset.to_dataframe().to_csv('data.csv')
        # print("AGGREGATED DATASET")
        coverage = xarray_to_prs_coverage_json(agg_dataset)
        print(coverage['domain'])
        
    else:
        print("No data found")


    
    # Return the data in the OGC API complaint cube format
    data = {
        "type": "Coverage",
        "domainType": "string",
        "coverages": [
            {
            "type": "Coverage",
            "domain": {
                "type": "Domain",
                "domainType": "string",
                "axes": {
                "x": {
                    "dataType": "string",
                    "values": [
                    0
                    ],
                    "coordinates": [
                    "string",
                    "string"
                    ],
                    "bounds": [
                    0
                    ]
                },
                "y": {
                    "dataType": "string",
                    "values": [
                    0
                    ],
                    "coordinates": [
                    "string",
                    "string"
                    ],
                    "bounds": [
                    0
                    ]
                },
                "z": {
                    "dataType": "string",
                    "values": [
                    0
                    ],
                    "coordinates": [
                    "string",
                    "string"
                    ],
                    "bounds": [
                    0
                    ]
                },
                "t": {
                    "dataType": "string",
                    "values": [
                    "string"
                    ],
                    "coordinates": [
                    "string",
                    "string"
                    ],
                    "bounds": [
                    "string"
                    ]
                }
                },
                "referencing": [
                {
                    "coordinates": [
                    "string"
                    ],
                    "system": {
                    "type": "string",
                    "calendar": "string",
                    "timeScale": "string"
                    }
                }
                ]
            },
            "ranges": {
                "property1": {
                "type": "NdArray",
                "dataType": "float",
                "shape": [
                    0
                ],
                "axisNames": [
                    "string"
                ],
                "values": [
                    0
                ]
                },
                "property2": {
                "type": "NdArray",
                "dataType": "float",
                "shape": [
                    0
                ],
                "axisNames": [
                    "string"
                ],
                "values": [
                    0
                ]
                }
            }
            }
        ],
        "parameters": {
            "property1": {
            "id": "string",
            "type": "Parameter",
            "description": {
                "property1": "string",
                "property2": "string"
            },
            "observedProperty": {
                "id": "string",
                "label": {
                "property1": "string",
                "property2": "string"
                },
                "description": {
                "property1": "string",
                "property2": "string"
                },
                "categories": [
                {
                    "id": "string",
                    "label": {
                    "property1": "string",
                    "property2": "string"
                    },
                    "description": {
                    "property1": "string",
                    "property2": "string"
                    }
                }
                ]
            },
            "unit": {
                "id": "string",
                "label": {
                "property1": "string",
                "property2": "string"
                },
                "symbol": "string"
            },
            "categoryEncoding": {
                "property1": 0,
                "property2": 0
            }
            },
            "property2": {
            "id": "string",
            "type": "Parameter",
            "description": {
                "property1": "string",
                "property2": "string"
            },
            "observedProperty": {
                "id": "string",
                "label": {
                "property1": "string",
                "property2": "string"
                },
                "description": {
                "property1": "string",
                "property2": "string"
                },
                "categories": [
                {
                    "id": "string",
                    "label": {
                    "property1": "string",
                    "property2": "string"
                    },
                    "description": {
                    "property1": "string",
                    "property2": "string"
                    }
                }
                ]
            },
            "unit": {
                "id": "string",
                "label": {
                "property1": "string",
                "property2": "string"
                },
                "symbol": "string"
            },
            "categoryEncoding": {
                "property1": 0,
                "property2": 0
            }
            }
        },
        "ranges": {
            "property1": {
            "type": "NdArray",
            "dataType": "float",
            "shape": [
                0
            ],
            "axisNames": [
                "string"
            ],
            "values": [
                0
            ]
            },
            "property2": {
            "type": "NdArray",
            "dataType": "float",
            "shape": [
                0
            ],
            "axisNames": [
                "string"
            ],
            "values": [
                0
            ]
            }
        },
        "referencing": [
            {
            "coordinates": [
                "string"
            ],
            "system": {
                "type": "string",
                "calendar": "string",
                "timeScale": "string"
            }
            }
        ]
    }

    return jsonify(coverage)

if __name__ == '__main__':
    app.run(port=8080, debug=True)


# Example response OGC API complaint cube endpoint
# {
#   "type": "FeatureCollection",
#   "features": [
#     {
#       "type": "Feature",
#       "geometry": {
#         "type": "Point",
#         "coordinates": [
#           0,
#           0
#         ]
#       },
#       "properties": [
#         {
#           "datetime": "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z",
#           "label": "Monitoring site name",
#           "parameter-name": [
#             "velocity",
#             "temperature"
#           ],
#           "edrqueryendpoint": "https://example.org/api/collections/collection/locations/location_123"
#         }
#       ],
#       "id": "string",
#       "links": [
#         {
#           "href": "https://example.com/collections/monitoringsites/locations/1234",
#           "rel": "alternate",
#           "type": "application/geo+json",
#           "hreflang": "en",
#           "title": "Monitoring site name",
#           "length": 0,
#           "templated": true
#         }
#       ]
#     }
#   ],
#   "parameters": [
#     {
#       "id": "string",
#       "type": "Parameter",
#       "description": {
#         "additionalProp1": "string",
#         "additionalProp2": "string",
#         "additionalProp3": "string"
#       },
#       "observedProperty": {
#         "id": "string",
#         "label": {
#           "additionalProp1": "string",
#           "additionalProp2": "string",
#           "additionalProp3": "string"
#         },
#         "description": {
#           "additionalProp1": "string",
#           "additionalProp2": "string",
#           "additionalProp3": "string"
#         },
#         "categories": [
#           {
#             "id": "string",
#             "label": {
#               "additionalProp1": "string",
#               "additionalProp2": "string",
#               "additionalProp3": "string"
#             },
#             "description": {
#               "additionalProp1": "string",
#               "additionalProp2": "string",
#               "additionalProp3": "string"
#             }
#           }
#         ]
#       },
#       "unit": {
#         "id": "string",
#         "label": {
#           "additionalProp1": "string",
#           "additionalProp2": "string",
#           "additionalProp3": "string"
#         },
#         "symbol": "string"
#       },
#       "categoryEncoding": {
#         "additionalProp1": 0,
#         "additionalProp2": 0,
#         "additionalProp3": 0
#       }
#     }
#   ],
#   "links": [
#     {
#       "href": "https://example.com/collections/monitoringsites/locations/1234",
#       "rel": "alternate",
#       "type": "application/geo+json",
#       "hreflang": "en",
#       "title": "Monitoring site name",
#       "length": 0,
#       "templated": true
#     }
#   ],
#   "timeStamp": "2017-08-17T08:05:32Z",
#   "numberMatched": 127,
#   "numberReturned": 10
# }