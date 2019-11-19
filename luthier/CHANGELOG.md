Change Log
==========

All notable changes to Fili will be documented here. Changes are accumulated as new paragraphs at the top of the current
major version. Each change has a link to the pull request that makes the change and to the issue that triggered the
pull request if there was one.

Current
-------

### Added:

- [Luthier now uses LuaJ to run the Lua scripts directly](https://github.com/yahoo/fili/pull/966)
    * Add a new interface, LuthierConfigNode. This is effectively a
    glorified subset of the ObjectNode interface, because the original
    version of Luthier read in JSON and worked with ObjectNodes. The
    ResourceNodeSupplier has been turned into an interface
    (LuthierSupplier). The Luthier framework relies on these interfaces
    rather than jackson-isms

### Changed:

- [Add Max and Min factories to Luther](https://github.com/yahoo/fili/issues/993)
    * Repackaged slightly to reduce class density in the factory packages.
    * Added `DoubleMaxFactory`, `DoubleMinFactory`, `LongMaxFactory`, `LongMinFactory`
    

- [Luthier now uses LuaJ to run the Lua scripts directly](https://github.com/yahoo/fili/pull/966)

    * The KeyValueStoreDimensionFactory has been tweaked so that it no
    longer camel cases field names. Field names should be exactly what the
    customer writes in their configuration. Anything else is just asking for
    frustrated customers.

    * The lua configuration has been tweaked so that now `config.lua`
    returns a function that takes the app name and returns a table of
    configuration. The LuaJNodeSupplier invokes the returned function,
    passing in the app-name as extracted from configuration. This allows us to set the
    app name using Fili's configuration framework.

    * Imports are changed to the blatantly platform agnostic ".", rather
    than Unixy "/". This plus the use of LuaJ should make Luthier fully
    platform agnostic.

    * The config.lua script lives in the top level `src/main/resources`
    directory. App-independent configuration lives in the
    `src/main/resources/lib` directory, while all app-specific configuration
    should live in `src/main/resources/<appName>`. All imports are relative
    to the primary script being executed. In this case, `config.lua`.

### Deprecated:

### Removed:
- [Luthier now uses LuaJ to run the Lua scripts directly](https://github.com/yahoo/fili/pull/966)
    * We remove any scripts and code that work with JSON-based
    configuration, because they're no longer necessary, and their tests are
    complicated by the fact that they need external JSON files (which are no
    longer being auto-generated). See commit 14f7bd59 if you're interested
    in what the JSON-based configuration looked like.

    * Lua no longer performs any sort of filesystem manipulation, since it
    no longer needs to generate JSON files. Similarly, we remove the
    json.lua dependency since it is no longer necessary. Similarly, we
    remove the verification script as no longer appropriate.

    * Several shell scripts that are no longer necessary have been removed.
    The only one that remains is `runApp.sh`, since it abstracts over the
    rather fiddly mvn parameters needed to execute a local Fili instance. However,
    it no longer takes an app name to run.


