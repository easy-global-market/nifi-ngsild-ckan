# NGSI-LD to CKAN

[![License: Apache-2.0](https://img.shields.io/github/license/stellio-hub/stellio-context-broker.svg)](https://spdx.org/licenses/Apache-2.0.html)
![Build](https://github.com/easy-global-market/nifi-ngsild-ckan/actions/workflows/maven.yml/badge.svg)

## Table of Contents
- [Overview](#overview)
- [Functionality](#functionality)
   - [CKAN Data Structures And Mapping](#ckan-data-structures-and-mapping)
   - [Input Data](#input-data)
   - [Output](#output)
- [Requirements](#requirements)
- [Configuration](#configuration)
   - [Processor Properties](#processor-properties)
- [Naming conventions](#Naming-conventions)
- [Metadata](#metadata)
- [Template](#template)
- [Limitations](#Current-limitations)
- [Roadmap & Issues](#roadmap--issues)
- [Acknowledgments](#acknowledgments)

## Overview

`NgsiLdToCkan` is a NiFi processor that persists NGSI-LD entities within a [CKAN](https://ckan.org/) server.
Entities are received through NGSI-LD notifications sent by a NGSI-LD Context Broker.

## Functionality

### CKAN Data Structures And Mapping

CKAN is a powerful platform for managing and publishing collections of data. These collections are owned by organizations.
Each organization contains multiple datasets (called packages in CKAN) and every dataset consists of several resources. 
A resource is the structure that holds the data itself. In addition, a dataset also contains metadata (information about the data).

CKAN offers a generic mapping between its model and [DCAT](https://www.w3.org/TR/vocab-dcat-3/) classes like `dcat:Dataset` and `dcat:Distribution`. 

In this mapping, a `Dataset` entity in the Context Broker is equivalent to a dataset in CKAN, while a `Distribution` entity 
is equivalent to a resource. However, a `Catalog` entity is not directly equivalent to an organization because the 
concept of an organization in CKAN does not directly align with the concept of a `dcat:Catalog`. Thus, when creating an 
organization in CKAN, the processor uses the `publisherUrl` attribute from the flowfile.

For more in-depth information about `dcat:Dataset` and `dcat:Distribution` see [Smart data models - DCAT data model](https://github.com/smart-data-models/dataModel.DCAT-AP).

When creating datasets and resources in CKAN, the processor leverages the attributes in the received NGSI-LD entities to
enrich the created datasets and resources with the metadata that it can extract from the NGSI-LD entities. One goal of this
behavior is also to expose datasets and resources that comply with the [FAIR principles](https://www.go-fair.org/fair-principles/).

### Input Data

The input data must be a valid NGSI-LD notification containing the entities to be persisted in the CKAN server. Each 
entity must have a `servesDataset` attribute that references a `Dataset` entity. Leveraging the Linked Entity Retrieval
features, the `Dataset` entity must be part of each entity received in the notification.

The attributes processed by the processor are described in the [Metadata](#Metadata) section.

Example of a NGSI-LD notification received by the processor:

```json
{
  "id" : "urn:ngsi-ld:Notification:1",
  "type" : "Notification",
  "subscriptionId" : "urn:ngsi-ld:Subscription:Waterverse:CKAN",
  "notifiedAt" : "2026-03-19T09:37:26.859657Z",
  "data" : [ {
    "id" : "urn:ngsi-ld:HydrometricStation:X031001001",
    "type" : "HydrometricStation",
    "flow" : {
      "type" : "Property",
      "value" : 26100.0,
      "unitCode" : "G51",
      "observedAt" : "2026-03-19T09:00:00Z"
    },
    "waterLevel" : {
      "type" : "Property",
      "value" : 230.0,
      "unitCode" : "MMT",
      "observedAt" : "2026-01-16T12:30:00Z"
    },
    "temporal" : {
      "type" : "JsonProperty",
      "json" : {
        "endDate" : "2025-08-07T06:00:00Z",
        "startDate" : "2023-08-03T08:45:00Z"
      }
    },
    "accessURL" : {
      "type" : "Property",
      "value" : "https://sedimark.org"
    },
    "byteSize" : {
      "type" : "Property",
      "value" : 100
    },
    "description" : {
      "type" : "Property",
      "value" : "Hydrometric station for water level and flows"
    },
    "mediaType" : {
      "type" : "Property",
      "value" : "application/ld+json"
    },
    "location" : {
      "type" : "GeoProperty",
      "value" : {
        "type" : "Point",
        "coordinates" : [ 6.49800996, 44.55535641 ]
      }
    },
    "downloadURL" : {
      "type" : "Property",
      "value" : "https://download.org"
    },
    "servesDataset" : {
      "type" : "Relationship",
      "object" : "urn:ngsi-ld:Dataset:HydrometricStations:2",
      "entity" : {
        "id" : "urn:ngsi-ld:Dataset:HydrometricStations:2",
        "type" : "Dataset",
        "title" : {
          "type" : "Property",
          "value" : " HydrometricStations Dataset"
        },
        "theme" : {
          "type" : "Relationship",
          "datasetId" : "urn:ngsi-ld:Dataset:Theme:EauNutriments",
          "object" : "urn:ngsi-ld:Theme:EauNutriments"
        },
        "spatial" : {
          "type" : "GeoProperty",
          "value" : {
            "type" : "Polygon",
            "coordinates" : [ [ [ 5.9452872, 44.380557 ], [ 6.1117237, 44.4761088 ], [ 6.49800996, 44.55535641 ], [ 6.7008533, 44.5317341 ], [ 6.6516895, 44.384303 ], [ 5.9452872, 44.380557 ] ] ]
          }
        },
        "temporal" : {
          "type" : "JsonProperty",
          "json" : {
            "status" : "inactive"
          }
        },
        "keyword" : {
          "type" : "Property",
          "value" : "[Environnement,EauNutriments,Data]"
        },
        "publisher" : {
          "type" : "Property",
          "value" : "EGM Organization"
        },
        "description" : {
          "type" : "Property",
          "value" : "Dataset for HydrometricStations, measuring water levels and flows."
        },
        "accessRights" : {
          "type" : "Property",
          "value" : "Attribution Rights"
        },
        "landingPage" : {
          "type" : "Property",
          "value" : "https://hydrometric-stations.org"
        },
        "contactPoint" : {
          "type" : "Property",
          "value" : "EGM"
        }
      }
    }
  } ]
}
```

### Output

The `NgsiLdToCkan` processor publishes all entities of the same type in the same dataset.
Each entity is a resource with a default `application/ld+json` format.

## Requirements

A `Subscription` must be created to trigger the notifications sent when entities are created or updated.

Example of a NGSI-LD subscription:

```json
{
    "id": "urn:ngsi-ld:Subscription:CKAN",
    "type": "Subscription",
    "entities": [
        {
            "type": "Distribution"
        }
    ],
    "notificationTrigger": [
        "entityCreated",
        "entityUpdated"
    ],
    "notification": {
        "format": "normalized",
        "endpoint": {
            "uri": "http://localhost:8080/ckanListener",
            "accept": "application/json"
        },
        "join": "inline",
        "joinLevel": 1
    }
}
```

## Configuration

### Processor Properties

<img src="docs/images/Ckan config.png" width=600 alt="Sample configuration of the NgsiLdToCkan processor">

* `CKAN Viewer` property specifies the visualization of the resource data on the CKAN resource page.
* `CKAN API Key` property is a token generated from the user account on the CKAN site.
* `Create DataStore` property creates the resource in the datastore when set to true.

## Naming conventions

Names for an organization, a dataset, or a resource must only contain alphanumeric characters, `-`, or `_`. 

The length must be between 2 and 100 characters.

## Metadata

Metadata treated by the processor is used to create metadata in DCAT when creating organizations, datasets and resources.

This table lists `Dataset` metadata the processor extracts from the input data and its equivalent in DCAT vocabulary.

| Metadata in CKAN server for datasets | Equivalent in DCAT vocabulary | Attribute name in `Dataset` entity |         
|--------------------------------------|-------------------------------|------------------------------------|
| access_rights                        | dcterms:accessRights          | datasetRights                      |
| contact_email                        | -                             | contactEmail                       |
| contact_name                         | -                             | contactName                        |
| contact_uri                          | dcat:contactPoint             | contactPoint                       |
| keywords                             | dcat:keywords                 | keywords                           |
| landingPage                          | dcat:landingPage              | landingPage                        |
| notes                                | dcterms:description           | description                        |
| spatial                              | dcterms:spatial               | spatial                            |
| spatial_uri                          | -                             | spatialUri                         |
| tags                                 | dcat:keyword                  | keyword                            |
| temporal_start                       | dcat:startDate                | temporalStart                      |
| temporal_end                         | dcat:endDate                  | temporalEnd                        |
| title                                | dcterms:title                 | title (required)                   |
| theme                                | dcat:theme                    | theme                              |
| url                                  | dcat:landingPage              | landingPage                        |
| version                              | -                             | version                            |
| visibility                           | -                             | visibility                         |

This table lists `Distribution` metadata the processor extracts from the input data and its equivalent in DCAT vocabulary.

| CKAN Metadata for resources | Equivalent in DCAT vocabulary | Attribute name in `Distribution` entity |
|-----------------------------|-------------------------------|-----------------------------------------|
| access_url                  | dcat:accessURL                | accessURL                               |
| availability                | -                             | availability                            |
| description                 | dcterms:description           | description                             |
| download_url                | dcat:downloadURL              | downloadURL                             |
| license                     | dcterms:license               | license                                 |
| license_type                | -                             | licenseType                             |
| mimetype                    | dcat:mediaType                | mediaType                               |
| rights                      | dcterms:rights                | rights                                  |
| size                        | dcat:byteSize                 | byteSize                                |
| title                       | dcat:byteSize                 | title (required)                        |

Additionally, the `publisherURL` is extracted from the flowfile attributes and used as the name of the organization
to which belongs the dataset.

## Template

A basic NiFi template with the `NgsiLdToCkan` processor can be found [here](CKAN_User_Guide_Template.xml).

## Current limitations

* The processor only supports attributes of type `Property`, `Relationship` and `GeoProperty`. 
* An already existing resource can't be updated with new attributes.
* The processor is missing unit tests.

## Roadmap & Issues

To check out planned features, report bugs or suggest new features, see [open issues](https://github.com/easy-global-market/nifi-ngsild-ckan/issues).

## Acknowledgments

This project has been funded by the [WATERVERSE project](https://waterverse.eu/) of the European Union’s Horizon Europe programme under Grant Agreement no 101070262.

WATERVERSE is a project that promotes the use of FAIR (Findable, Accessible, Interoperable, and Reusable) 
data principles to improve water sector data management and sharing.  

As part of this effort, `NgsiLdToCkan` enables publishing NGSI-LD context information to CKAN, making it more open, 
interoperable, and reusable.
