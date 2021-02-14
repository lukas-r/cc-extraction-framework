# cc-extraction-framework

Java framework for extracting and processing isA-pairs from CommonCrawl data. Contains classes for distributed extraction, entity disambiguation of tuple entities, building local taxonomies from isA-pairs and merging these into bigger global taxonomies. Functionality for exporting tuples and taxonomy graphs also included.

## Dependencies

This framework is compatible with extractor classes from the [Web Data Commons Extraction Framework](http://webdatacommons.org/framework/). It therefore includes dependencies which can be resolved by importing the WCD Extraction Framework.

To store data in SQLite Databases, this project relies on sqlite-jdbc, which needs to be imported in order to use it.