Fili Web Service [![Travis](https://img.shields.io/travis/yahoo/fili.svg?maxAge=2592000?style=plastic)](Travis)  [![Download](https://api.bintray.com/packages/yahoo/maven/fili/images/download.svg) ](https://bintray.com/yahoo/maven/fili/_latestVersion)
===============================================================================================================


Repository for the Fili web service.

## Monitoring and Operations

There is [a list of Key Performance Indicators (KPIs)](kpis.md) for the web service. It doesn't go into detail about what each of the KPIs mean, but it gives a rough overview of why they matter.  


## Useful Links

- [Project Site](https://github.com/yahoo/fili)


### How do I enable Redis?

To enable Redis on local machines:

- set the `test__dimension_backend` property to `redis`
- set the `test__key_value_store_tests` property to `redis`

in `src/test/resources/userConfig.properties` or in an environment variable.

## Miscellaneous Information

### Maven doesn't run on my Mac. Help!

Maven picks up the wrong version of Java. If you run `mvn --version` you'll see that it probably says that the Java version isn't 1.8 but is 1.7, or some even earlier version.

To remedy this, you need to make sure you have the `JAVA_HOME` environment variable exported. To export this variable, put the following line in your `~/.bash_profile` file:
 
```bash
export JAVA_HOME=`/usr/libexec/java_home -v 1.8`
```

Additionally, to work with yahoo.bintray.com/maven, you need to copy the [settings.xml](settings.xml) file into `~/.m2/

The easiest way to run tests on a Mac is to use in memory dimension backend and key value store: 

```bash
mvn -Dtest__dimension_backend=memory -Dtest__key_value_store_tests=memory clean test
```

### I want to contribute to Fili. Are there any code style guidelines I should follow?

For the moment, we have distilled the most important code style conventions with respect to Fili's code as IntelliJ settings.

If you are using IntelliJ, you may import these code style settings by importing: [Fili-Project.xml](Fili-Project.xml)

They will appear as a new Scheme named *Bard-Project* under your Editor &rarr; Code Style section.

Alternatively, you might check the xml file that is included in the jar and map its settings to your development environment.

We have also included a large amount of checkstyle rules, that should catch 98% of the code style guidelines. The 
[Google Java style guide](https://google.github.io/styleguide/javaguide.html) is very close to our style.
