FAQ
===

Frequently asked questions about Fili.

How do I enable Redis-backed dimensions?
----------------------------------------

To enable Redis on local machines, you need to set a couple of properties. In `src/test/resources/userConfig.properties`
or in an environment variable, override the following properties:

- set the `test__dimension_backend` property to `redis`
- set the `test__key_value_store_tests` property to `redis`
