FAQ
===

Frequently asked questions about Fili.

How do I enable Redis-backed dimensions?
----------------------------------------

To enable Redis on local machines, you need to set a couple of properties. In `src/test/resources/userConfig.properties`
or in an environment variable, override the following properties:

- set the `test__dimension_backend` property to `redis`
- set the `test__key_value_store_tests` property to `redis`


I use IntelliJ. Is there any way to easily sync up with Fili's code styles?
---------------------------------------------------------------------------

For the moment, we have distilled the most important code style conventions with respect to Fili's code as IntelliJ 
settings. If you are using IntelliJ, you may import these code style settings by importing the 
[Fili-Project-intellij-code-style.xml](/Fili-Project-intellij-code-style.xml) file in the root of the repo. The setting
for the project will appear as a new Scheme named *Bard-Project* under your `Editor &rarr; Code Style` section.

Alternatively, you might check the xml file that is included in the jar and map its settings to your development
environment.
