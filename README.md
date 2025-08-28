# NgsiLdToCkan

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
- [Metadata](#metadata)
- [Template](#template)
- [Limitations](#limitations)
- [Roadmap & Issues](#roadmap--issues)

## Overview

`NgsiLdToCkan` is a NiFi processor that persists NGSI-LD entities within a [CKAN](https://ckan.org/) server.
Entities are received through NGSI-LD notifications sent by a NGSI-LD Context Broker.

It is part of the [WATERVERSE Project](https://watervese.eu/).

## Functionality

### CKAN Data Structures And Mapping

CKAN is a powerful platform for managing and publishing collections of data. These collections are owned by organizations.
Each organization contains multiple datasets (called packages in CKAN) and every dataset consists of several resources. 
A resource is the structure that holds the data itself. In addition, a dataset also contains metadata (information about the data).

CKAN offers a generic mapping between its model and [DCAT](https://www.w3.org/TR/vocab-dcat-3/) classes like `dcat:Dataset` and `dcat:Distribution`. 

In this mapping, a `Dataset` entity in the Context Broker is equivalent to a dataset in CKAN, while a `Distribution` entity 
is equivalent to a resource. However, a `Catalog` entity is not directly equivalent to an organization
because the concept of an organization in CKAN does not directly align with the concept of a `dcat:Catalog`. 

A more straight-forward mapping from CKAN metadata to DCAT metadata is also done.

For more in-depth information about `dcat:Dataset` and `dcat:Distribution` see [Smart data models - DCAT data model](https://github.com/smart-data-models/dataModel.DCAT-AP).

When creating datasets and resources in CKAN, the processor leverages the attributes in the received NGSI-LD entities to
enrich the created datasets and resources with the metadata that it can extract from the NGSI-LD entities. One goal of this
behavior is also to expose datasets and resources that comply with the [FAIR principles](https://www.go-fair.org/fair-principles/).

### Input Data

The input data must be a valid NGSI-LD notification containing the entities to be persisted in the CKAN server.
Each entity must at least have a `title` attribute that will be used as the name of the resource. 

A dataset must include a `publisher` attribute, which is a relationship to an `Organization` entity responsible for making the
datasets and the resources available. `NgsiLdToCkan` processor uses the `title` of the `Organization` entity as the name of the
`Organization` to create in the CKAN server.

A `datasetTitle` must exist as a flowfile attribute to be used as the name of the dataset belonging to the resource.
(future versions of the processor will automatically retrieve dataset information by using the relationship between a
`Distribution` entity and a `Dataset` entity).

In addition, other metadata can also be added as flowfile attributes to be extracted by the processor.
The `keywords` attribute however must always be present. It is a comma seperated list of terms that will be added as tags 
to the dataset (e.g., ["keyword1", "keyword2", "keyword3"]).

**Example of a NGSI-LD notification received by the processor**

```json
{
    "id": "urn:ngsi-ld:Notification:1",
    "type": "Notification",
    "subscriptionId": "urn:ngsi-ld:Subscription:CKAN",
    "notifiedAt": "2025-02-10T13:29:53.903986Z",
    "data": [
        {
            "id": "urn:ngsi-ld:Distribution:1",
            "type": "Distribution",
            "title": {
                "type": "Property",
                "value": "Entity example"
            },
            "name": {
                "type": "Property",
                "value": "Entity name",
                "observedAt": "2025-02-10T10:18:20Z"
            }
        }
    ]
}
```

### Output

The `NgsiLdToCkan` processor publishes all entities of the same type in the same dataset.
Each entity is a resource with a default `application/ld+json` format.

In CKAN, names for an organization, a dataset, or a resource must only contain alphanumeric characters, `-` , or `_`.
The length must be between 2 and 100 characters. Thus, a transformation is automatically performed by the processor. 

## Requirements

A `Subscription` must be created to trigger the notifications sent when entities are created or updated.


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
        }
    }
}
```

## Configuration

### Processor Properties

<img src="docs/images/Ckan config.png" width=600/>

* `CKAN Viewer` property specifies the visualization of the resource data on the CKAN resource page.

* `CKAN API Key` property is a token generated from the user account on the CKAN site. 

* `Create DataStore` property creates the resource in the datastore when set to true.

## Metadata

Metadata treated by the processor and added in the CKAN server can be mapped to DCAT metadata. 


This table lists `Dataset` metadata the processor extracts as flowfile attributes and its equivalent in DCAT vocabulary.


| Metadata in CKAN server for datasets | Equivalent in DCAT vocabulary | flowfile attribute name in `NgsiLdToCkan` processor |         
|--------------------------------------|-------------------------------|-----------------------------------------------------|
| access_rights                        | dcterms:accessRights          | datasetRights                                       |
| contact_email                        | -                             | contactEmail                                        |
| contact_name                         | -                             | contactName                                         |
| contact_uri                          | dcat:contactPoint             | contactPoint                                        |
| notes                                | dcterms:description           | packageDescription                                  |
| publisher                            | dcterms:publisher             | publisherURL                                        |
| spatial                              | dcterms:spatial               | spatialCoverage                                     |
| spatial_uri                          | -                             | spatialUri                                          |
| tags                                 | dcat:keyword                  | keyword                                             |
| temporal_start                       | dcat:startDate                | temporalStart                                       |
| temporal_end                         | dcat:endDate                  | temporalEnd                                         |
| title                                | dcterms:title                 | datasetTitle                                        |
| theme                                | dcat:theme                    | themes                                              |
| url                                  | dcat:landingPage              | landingPage                                         |
| version                              | -                             | version                                             |
| visibility                           | -                             | visibility                                          |



This table lists `Distribution` metadata the processor extracts from the input data and its equivalent in DCAT vocabulary.



| CKAN Metadata for resources | Equivalent in DCAT vocabulary | attribute name in `Distribution` entity |
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


## Template

A basic NiFi template with the `NgsiLdToCkan` processor can be found [here](CKAN_User_Guide_Template.xml).

## Current limitations

* The processor does not support multi-attributes instances.
* The processor only supports attributes of type `Property`, `Relationship` and `GeoProperty`. 
* An already existing resource can't be updated with new attributes.
* There's no description for datasets.
* Not all DCAT metadata available in a dataset is utilized (for example `publisher` metadata which can be used when creating an organization)
* Metadata and dataset title must be added as flowfile attributes instead of being extracted inside the processor (this can be avoided with the use of NGSI-LD Linked Entity Retrieval).
* The processor does not implement unit testing.

## Roadmap & Issues

To check out planned features, report bugs or suggest new features see [open issues](https://github.com/easy-global-market/nifi-ngsild-ckan/issues).
