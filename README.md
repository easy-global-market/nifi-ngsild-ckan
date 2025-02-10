# NgsiLdToCkan

[![FIWARE](https://nexus.lab.fiware.org/repository/raw/public/badges/chapters/core.svg)](https://www.fiware.org/developers/catalogue/)
[![NGSI-LD badge](https://img.shields.io/badge/NGSI-LD-red.svg)](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.06.01_60/gs_CIM009v010601p.pdf)
[![License: Apache-2.0](https://img.shields.io/github/license/stellio-hub/stellio-context-broker.svg)](https://spdx.org/licenses/Apache-2.0.html)
![Build](https://github.com/easy-global-market/nifi-ngsild-ckan/actions/workflows/maven.yml/badge.svg)

## Table of Contents
- [Overview](#overview)
- [Functionality](#functionality)
   - [CKAN Data Structures And Mapping](#ckan-data-structures-and-mapping)
   - [Input Data](#input-data)
   - [Output](#output)
- [Configuration](#configuration)
   - [Processor Properties](#processor-properties)
- [Metadata](#metadata)
   - [Metadata List](#metadata-list)

## Overview
`NgsiLdToCkan` is a Nifi processor developed by FIWARE to persist NGSI-LD context data events within a [CKAN](https://ckan.org/) server. Context data is received as a notification sent by a Context Broker or any system that supports NGSI-LD.


## Functionality

The processor receives NGSI-LD events (notifications) and converts them internally into an `NGSIEvent` objects, which contain the entities to be published on the CKAN server.

### CKAN Data Structures And Mapping

CKAN is a powerful platform for managing and publishing collections of data. These collections are owned by organizations. Each organization contains multiple datasets (also called packages) and every dataset consists of several resources. 
A resource is the structure that holds the data itself. In addition, a dataset can also contain metadata (information about the context data).

When context data is consumed by the `NgsiLdToCkan` processor, each entity will be persisted in the CKAN server as a resource belonging to an organization and a dataset. 

### Input Data

The input data must be a valid NGSI-LD context data with a `data` element containing the entities to be persisted in the CKAN server. Each entity must have a `title` attribute that will
be used as a resource name. A `datasetTitle` must exist as a flowfile attribute to be used as a dataset name.

In addition, metadata can also be added as flowfile attributes to be extracted by the processor ([metadata list](#metadata-list)). The `keywords` attribute however must always be a present. 
It is a comma seperated list of terms that will be added as tags to the dataset: ["keyword1", "keyword2", "keyword3"] .

**Naming conventions**

Names for an organization, a dataset, or a resource must only contain alphanumeric characters, `-` , or `_`. The length must be between 2 and 100 characters.

**Example of an NGSI-LD event**
```
{
    "id": "urn:ngsi-ld:Notification:1",
    "type": "Notification",
    "subscriptionId": "urn:ngsi-ld:Subscription:CKAN",
    "notifiedAt": "2025-02-10T13:29:53.903986Z",
    "data": [
        {
            "id": "urn:ngsi-ld:Entity:1",
            "type": "Entity",
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

The `NgsiLdToCkan` processor publishes all entities received in the notified context data in the same dataset. Each entity will be added as a resource 
with a default `application/ld+json` format.

## Configuration

### Processor Properties

<img src="docs/images/Ckan config.png" width=600/>

`CKAN Viewer` property specifies the visualization of the resource data on the CKAN resource page.

`CKAN API Key` property is a token generated from the user account on the CKAN site. 

`Create DataStore` property creates the resource when set to true.

## Metadata

### Metadata List

| Metadata for datasets |         
|-----------------------|
| packageDescription    |
| version               |
| landingPage           |
| visibility            |
| organizationType      |
| contactPoint          |
| contactName           |
| contactEmail          |
| spatialUri            |
| spatialCoverage       |
| temporalStart         |
| temporalEnd           |
| themes                |
| datasetRights         |
| keywords              |


| Metadata for resources |
|------------------------|
| accessURL              |
| availability           |
| resourceDescription    |
| mimeType               |
| license                |
| downloadURL            |
| byteSize               |
| resourceRights         |
| licenseType            |
