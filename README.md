Fili 
====

[![Download](https://api.bintray.com/packages/yahoo/maven/fili/images/download.svg) ](https://bintray.com/yahoo/maven/fili/_latestVersion) [![Gitter](https://img.shields.io/gitter/room/yahoo/fili.svg?maxAge=2592000)](https://gitter.im/yahoo/fili) [![Travis](https://img.shields.io/travis/yahoo/fili/master.svg?maxAge=2592000)](https://travis-ci.org/yahoo/fili/builds/) [![Codacy grade](https://img.shields.io/codacy/grade/91fa6c38f25d4ea0ae3569ee70a33e38.svg?maxAge=2592000)](https://www.codacy.com/app/Fili/fili/dashboard) [![Codacy coverage](https://img.shields.io/codacy/coverage/91fa6c38f25d4ea0ae3569ee70a33e38.svg?label=test coverage&maxAge=2592000)](https://www.codacy.com/app/Fili/fili/dashboard) [![VersionEye](https://img.shields.io/versioneye/d/user/projects/57eb1529bd6fa600512e4af1.svg?maxAge=2592000)](https://www.versioneye.com/user/projects/57eb1529bd6fa600512e4af1)

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

The biggest difference between Fili and those other systems, however, is that Fili _only_ exposes these concepts. With
just these core concepts, end-users can ask a huge range of questions, extracting business value by focusing on finding 
answers to the _what_ and _why_. Other reporting systems expose end-users to other concepts like Views, Partitions,
and metric formulas, forcing users to worry too much about _how_ to answer their questions, but not Fili. 

Fili's simplicity and clarity lets users focus on their business-driven questions, and takes care of figuring out how to
best answer the question for them. 

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


Community [![Gitter](https://img.shields.io/gitter/room/yahoo/fili.svg?maxAge=2592000)](https://gitter.im/yahoo/fili)
---------------------------------------------------------------------------------------------------------------------

The Fili community generally hangs out [on Gitter](https://gitter.im/yahoo/fili), so please drop by if you have any 
questions, comments, concerns, wishes, hopes, dreams, wants, needs, yearnings, musings, or idle curiosities about Fili.
The core Fili developers love meeting new people and talking with them about how they can best use Fili to solve their
Big Data time series reporting and analytics problems. We know there are gaps in Fili, and definitely lots of new, 
powerful capabilities to add, so hearing about what's working and what could be better will help drive the direction of
Fili.

If you have other thoughts, or are running into trouble and are not able to get help from the community on Gitter,
please [open an issue](https://github.com/yahoo/fili/issues) describing your problem or idea. 

If you would like to get involved with Fili development, there's some ["contribution quickstart" info down below](#contributing),
and much more information in our [CONTRIBUTING](CONTRIBUTING.md) file.


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

Binaries for Fili are stored in [Bintray](https://bintray.com/yahoo/maven/fili). Dependency information for Maven, Ivy,
and Gradle can be found at https://bintray.com/yahoo/maven/fili, and some examples are below.

Maven:
```xml
<dependency>
    <groupId>com.yahoo.fili</groupId>
    <artifactId>fili</artifactId>
    <version>x.y.z</version>
</dependency>

<repository>
    <id>fili</id>
    <url>http://yahoo.bintray.com/maven</url>
</repository>
```

Gradle:
```groovy
repositories {
    maven { url 'http://yahoo.bintray.com/maven' }
}

dependencies {
    compile 'com.yahoo.fili:fili:x.y.z'
}
```

The most bleeding-edge version is: [![Bleeding-edge](https://api.bintray.com/packages/yahoo/maven/fili/images/download.svg) ](https://bintray.com/yahoo/maven/fili/_latestVersion)

The most recent stable version is: [![Stable](https://img.shields.io/badge/Stable-0.1.17-blue.svg) ](https://bintray.com/yahoo/maven/fili/0.1.17)


Extending
---------

Fili's easy to extend! It has a bunch of hooks already (`AbstractBinderFactory`)! There's a module system for config and
modules depending on other modules! There's lots more to say here, but the time the time, look at the time!


Contributing
------------

More details can be found in the [CONTRIBUTING](CONTRIBUTING.md) file, but here are the basics to get you started.

### Bug Reports & Feature Requests

If you find a bug, or something doesn't seem to be working right, please [open an issue](https://github.com/yahoo/fili/issues)
and include as much information about how you have things set up as possible, and include any relevant logs if you can.

If there's a feature, capability, or enhancement you would like to see in Fili, please [open an issue](https://github.com/yahoo/fili/issues)
and describe what you would like to see changed.

### Building

PRs are **very** welcome! Since Fili is a multi-module Maven project you'll need a [recent version of Maven](https://maven.apache.org/download.cgi). 
Once you have Maven installed, to build and run the tests:

```bash
$ git clone git@github.com:yahoo/fili.git
$ cd fili/
$ mvn test
```

### Testing & Code Style [![Codecov](https://img.shields.io/codecov/c/github/yahoo/fili.svg?label=test coverage&maxAge=2592000)](https://codecov.io/gh/yahoo/fili) [![Coveralls](https://img.shields.io/coveralls/yahoo/fili.svg?label=test coverage&maxAge=2592000)](https://coveralls.io/github/yahoo/fili)

We're _big_ believers in testing our code, both for correctness, as well as to ensure that changes don't unintentionally
break existing contracts unintentionally. We rely heavily on the [Spock](http://spockframework.org/) framework for our 
tests, and see a lot of benefit from it's conciseness, built-in [mocking framework](http://spockframework.org/spock/docs/1.1-rc-2/interaction_based_testing.html), 
and the fact that it uses [Groovy](http://www.groovy-lang.org/). :smile:

We also strive for very high-quality code, with the belief that quality code is easier to maintain, easier to understand,
and has fewer bugs. To help keep the quality bar high, we have an automated style checker ([Checkstyle](http://checkstyle.sourceforge.net/)) 
with rules that _should_ catch most of the common style issues. The full details of what the checker looks for can be 
found in our [checkstyle config](checkstyle-style.xml), but the [Google Java style guide](https://google.github.io/styleguide/javaguide.html) 
covers most of it, and is very close to our style.


Design and Architecture
-----------------------

Fili was designed from the ground-up to be amazing, scalable, and extensible. There's some nifty parts about Fili's
architecture that let it be so cool. We have lots of great diagrams and schematics, but they've been misplaced at the
moment. For now, it's safe to say that our architecture is _under construction_!


LICENSE
-------

Copyright 2016 Yahoo! Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the 
License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
language governing permissions and limitations under the License.
