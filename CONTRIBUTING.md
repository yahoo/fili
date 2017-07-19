Contributing [![Developers Google Group](https://img.shields.io/badge/google_group-developers-blue.svg?maxAge=2592000)](https://groups.google.com/forum/#!forum/fili-developers)
============

If you are considering contributing to Fili, thank you! We welcome all contributions, big or small. 

The only thing we ask is that you try to abide by the style and conventions of the project. Many of the conventions are
 enforced by automated checks, but we also have documented the conventions in a more human-friendly format. 
 [The contributing guidelines](docs/contributing) are still being made more complete, extended, and enhanced, so feel 
 free to contribute to those as well!

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
$ mvn clean test
```

### Testing & Code Style

We're _big_ believers in testing our code, both for correctness, as well as to ensure that changes don't unintentionally
break existing contracts unintentionally. We rely heavily on the [Spock](http://spockframework.org/) framework for our 
tests, and see a lot of benefit from it's conciseness, built-in [mocking framework](http://spockframework.org/spock/docs/1.1-rc-2/interaction_based_testing.html), 
and the fact that it uses [Groovy](http://www.groovy-lang.org/). :smile:

We also strive for very high-quality code, with the belief that quality code is easier to maintain, easier to understand,
and has fewer bugs. To help keep the quality bar high, we have an automated style checker ([Checkstyle](http://checkstyle.sourceforge.net/)) 
with rules that _should_ catch most of the common style issues. The full details of what the checker looks for can be 
found in our [checkstyle config](checkstyle-style.xml), but the [Google Java style guide](https://google.github.io/styleguide/javaguide.html) 
covers most of it, and is very close to our style.

### Credit

Special thanks given to Groovy's JsonSlurper which served as a base for one of Fili's [sorting functionalities](https://github.com/yahoo/fili/blob/master/fili-core/src/test/java/com/yahoo/bard/webservice/util/JsonSlurper.java)


Design and Architecture
-----------------------

Fili was designed from the ground-up to be amazing, scalable, and extensible. There's some nifty parts about Fili's
architecture that let it be so cool. We have lots of great diagrams and schematics, but they've been misplaced at the
moment. For now, it's safe to say that our architecture is _under construction_!

