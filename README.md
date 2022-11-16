Fili 
====

*Read this in other languages: [中文](./translations/zh/README-zh.md).*

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.yahoo.bard/fili/badge.svg?style=plastic)](https://maven-badges.herokuapp.com/maven-central/com.yahoo.bard/fili/)
[![Gitter](https://img.shields.io/gitter/room/yahoo/fili.svg?maxAge=2592000)](https://gitter.im/yahoo/fili) 
[![Screwdriver](https://cd.screwdriver.cd/pipelines/6154/badge)](https://cd.screwdriver.cd/pipelines/6154) 
[![Codacy grade](https://img.shields.io/codacy/grade/91fa6c38f25d4ea0ae3569ee70a33e38.svg?maxAge=21600)](https://www.codacy.com/app/Fili/fili/dashboard) 
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=yahoo_fili&metric=alert_status)](https://sonarcloud.io/dashboard?id=yahoo_fili)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=yahoo_fili&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=yahoo_fili) 
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=yahoo_fili&metric=coverage)](https://sonarcloud.io/dashboard?id=yahoo_fili) 
[![Lines of Code](https://sonarcloud.io/api/project_badges/measure?project=yahoo_fili&metric=ncloc)](https://sonarcloud.io/dashboard?id=yahoo_fili)

**The Fili groupId has been changed from com.yahoo.fili to com.yahoo.bard.**

Fili is a Java-based framework that makes it easy to build and maintain RESTful web services for time series reporting 
and analytics. Fili's HTTP GET-based [reporting API](docs/end-user-api.md) is clean and user-friendly, hiding the 
complexities of [complicated metric and dimension definition](docs/configuring-metrics.md), data storage, and query 
optimization from end-users. Designed with Big Data and scalability in mind, Fili has first-class support for
[Druid](http://druid.io) as a back-end, but Fili's flexible pipeline-style architecture can handle nearly any back-end
for data storage.

Fili exposes the same core concepts that all time series reporting and analytics systems expose:

- [Metrics](docs/end-user-api.md#metrics)
- [Dimensions](docs/end-user-api.md#dimensions)
- [Tables](docs/end-user-api.md#tables)
- Time ([Reporting Time Grain](docs/end-user-api.md#time-grain) and [Interval](docs/end-user-api.md#interval))

Other systems expose additional concepts like Views, Partitions, and metric formulas, but Fili chooses not to expose
end-users to those low-level concerns. By limiting the mental model of Fili's API to just these core domain concepts, 
Fili's API allows end-users to better extract business value by focusing on finding answers to the _what_ and _why_ 
questions from their data, rather than on _how_ to ask their question. 

Fili's [simple and clear API](docs/end-user-api.md) lets users focus on their business-driven questions, and the Fili
library takes care of figuring out how to best answer the question for them. 

This simplicity also allows for a huge amount of flexibility around where data is stored and how it's queried and 
retrieved from back-end systems. Having such flexibility allows those maintaining a web service built with Fili to move
data around, better optimize queries, and even swap out entire back-end systems without breaking their end-users or 
forcing them to go through a painful migration.

Fili also provides a plethora of other capabilities. Here are some of them:

| Functionality                                   | Operability                                        |
|-------------------------------------------------|----------------------------------------------------|
| Complex metric definition                       | Rate limiting                                      |
| Performance slice routing                       | Query weight checks                                | 
| Dimension joins (both annotation and filtering) | [Rich usage metrics](monitoring-and-operations.md) |
| Partial interval protection                     | Health checks                                      |
| Volatile data handling                          | Caching                                            |
| Modular architecture                            |                                                    |


Community [![Gitter](https://img.shields.io/gitter/room/yahoo/fili.svg?maxAge=2592000)](https://gitter.im/yahoo/fili) [![Users Google Group](https://img.shields.io/badge/google_group-users-blue.svg?maxAge=2592000)](https://groups.google.com/forum/#!forum/fili-users) [![Developers Google Group](https://img.shields.io/badge/google_group-developers-blue.svg?maxAge=2592000)](https://groups.google.com/forum/#!forum/fili-developers)
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

The Fili community generally hangs out [on Gitter](https://gitter.im/yahoo/fili), so please drop by if you have any 
questions, comments, concerns, wishes, hopes, dreams, wants, needs, yearnings, musings, or idle curiosities about Fili.
The core Fili developers love meeting new people and talking with them about how they can best use Fili to solve their
Big Data time series reporting and analytics problems. We know there are gaps in Fili, and definitely lots of new, 
powerful capabilities to add, so hearing about what's working and what could be better will help drive the direction of
Fili.

If you have other thoughts, or are running into trouble and are not able to get help from the community on Gitter,
please [open an issue](https://github.com/yahoo/fili/issues) describing your problem or idea. 

If you would like to get involved with Fili development, check out the [CONTRIBUTING](CONTRIBUTING.md) file.


Quick Start
-----------

Fili comes with a pre-configured [example application](fili-wikipedia-example) to help you get started and serve as a 
jumping-off-point for building your own web service using Fili. The example application lets you report on Wikipedia
article edit information, and picks up where [Druid's quick-start tutorial](http://druid.io/docs/0.9.1.1/tutorials/quickstart.html)
leaves off.   


Versioning
----------

Fili is stable and production-ready today, but the codebase is still under active development, with many large-scale
changes and new features being worked on or planned for the future. 
 
Active development happens on the patch version of the highest minor version. 

### @Deprecated

APIs marked with the `@Deprecated` annotation are planned for removal in upcoming releases. Deprecated APIs will be 
supported for 1 stable release beyond the release in which they were deprecated, but it is strongly recommended to stop 
using them. After deprecated APIs are no longer supported, they may be removed at any time.


Binaries (How to Get It)
------------------------

Binaries for Fili are stored in [Maven Central](https://search.maven.org/search?q=com.yahoo.bard). Dependency information for Maven, Ivy,
and Gradle can be found at https://search.maven.org/search?q=com.yahoo.bard, and some examples are below.

Maven:
```xml
<dependency>
    <groupId>com.yahoo.bard</groupId>
    <artifactId>fili</artifactId>
    <version>x.y.z</version>
</dependency>

<repository>
    <id>fili</id>
    <url>https://repo1.maven.org/maven2</url>
</repository>
```

Gradle:
```groovy
repositories {
    maven { url 'https://repo1.maven.org/maven2' }
}

dependencies {
    compile 'com.yahoo.bard:fili:x.y.z'
}
```

Extending
---------

Fili's easy to extend! It has a bunch of hooks already ([`AbstractBinderFactory`](https://github.com/yahoo/fili/blob/master/fili-core/src/main/java/com/yahoo/bard/webservice/application/AbstractBinderFactory.java))! 
There's a module system for config and modules depending on other modules! There's lots more to say here, but the time
the time, look at the time!


Documentation
-------------

More information about Fili can be found at [fili.io](http://fili.io).


Contributing [![Developers Google Group](https://img.shields.io/badge/google_group-developers-blue.svg?maxAge=2592000)](https://groups.google.com/forum/#!forum/fili-developers)
------------

More details can be found in the [CONTRIBUTING](CONTRIBUTING.md) file.


LICENSE
-------

Copyright 2016 Yahoo! Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the 
License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
language governing permissions and limitations under the License.
