Integrating Fili with Apache Drill
==================================

Fili supports Druid and will support more Fact Stores in the future. The assumption of this path is that a single
instance of Fili can support **only 1** Fact Store.

A different path, therefore, would be to have the single Fili instance talk to multiple data sources, for example,
Druid, Hive, and HBase. The advantage of this are

- providing the downstream projects with much more flexibility on the choices of data sources
- empowering Fili to be a centralized controller on query planning and data provisioning 

[Apache Drill](https://drill.apache.org/) stands out as an excellent tool to enable this.

Storage Plugin
==============

A storage plugin is a software module for connecting Drill to data sources. The first milestone could be writing a
storage plugin for Druid, integrate Drill into Fili, and have Fili talk to Druid through the Drill plugin. Once that
is finished, it will be very fast to incorporate other data sources, such as Hive, HDFS, HBase, etc.

There are 2 major components for writing a storage plugin:

1. exposing information to the query planner and schema management system
2. implementing the translation from the datasource API to the Drill record representation

One successful story of integration is [Kudu integration](https://github.com/apache/drill/tree/master/contrib/storage-kudu/src/main/java/org/apache/drill/exec/store/kudu).
