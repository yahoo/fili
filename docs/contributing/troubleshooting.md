Troubleshooting
===============

If you're having trouble getting Fili to compile, or the tests to run, here are some things you might try.

Maven doesn't run on my Mac
---------------------------

Maven often picks up the wrong version of Java. If you run `mvn --version` you'll see that it probably says that the
Java version isn't 1.8 but is 1.7, or some even earlier version. This is problematic, since Fili requires Java v1.8!

To remedy this, you need to make sure you have the `JAVA_HOME` environment variable exported. To export this variable, 
put the following line in your `~/.bash_profile` file:
 
```bash
export JAVA_HOME=`/usr/libexec/java_home -v 1.8`
```
<sub>Note: This assumes you have Java v1.8 installed</sub>

Additionally, to work with `yahoo.bintray.com/maven`, the repo to which Fili's artifacts are published, you need to copy
the [settings.xml](settings.xml) file into `~/.m2/

Tests don't seem to run correctly with a fresh checkout
-------------------------------------------------------

The easiest way to run tests on a Mac is to use in memory dimension backend and key value store: 

```bash
mvn -Dtest__dimension_backend=memory -Dtest__key_value_store_tests=memory clean test
```

In general this should not be needed, since memory-backed dimensions are the default for test cases, but there may be
instances where non-memory-backed-dimensions are attempting to be used, and you may be lacking a needed external
dependency like Redis.
