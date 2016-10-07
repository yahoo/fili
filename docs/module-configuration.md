Module Configuration
====================

Fili has two main configuration avenues, the domain object configuration (Metrics, Dimensions, and Tables) which happens
via compiled Java code, and module configuration via properties. The domain configuration is [covered](setup.md)
[elsewhere](configuring-metrics), and we'll only cover the module configuration infrastructure here. 

The system for property configuration that Fili uses lives in it's own [sub-module](../fili-system-config). This system
is extensible and reusable so that other Fili modules, and even other projects, can leverage it for their own property
config needs. That sub-module has it's own deep set of documentation, so we'll be focusing only on how to use it for
configuring Fili.


Configuration Sources and Overrides
-----------------------------------

Configuration for Fili modules can come from multiple locations, and allows for overriding other settings. This is
particularly useful when overriding a property set in a module to turn off a feature, or to override a default 
configuration for your application in a certain environment, for example.

Configuration sources are shown below, and are resolved in priority order, with higher-priority sources overriding
settings from lower-priority sources. Sources that are files must be available to Fili on the Classpath in order to be
loaded.

| Priority | Source                              | Notes                                                                  |
| -------: | ----------------------------------- | ---------------------------------------------------------------------- |
| (High) 1 | Environment variables               |                                                                        |
|        2 | Java properties                     |                                                                        |
|        3 | `userConfig.properties`<sup>*</sup> | For a controlling a specific, non-standard environment, like a dev box |
|        4 | `testApplicationConfig.properties`  | For test runner overrides                                              |
|        5 | `applicationConfig.properties`      | Every application MUST provide one of these                            |
|  (Low) 6 | `moduleConfig.properties`           | `moduleConfig.properties` files will be applied in dependency order    |

<sub>* Since `userConfig.properties` is often used while developing to turn features on and off, `.gitignore` includes a
rule to ignore this file by default to help prevent checking it in accidentally.</sub>
