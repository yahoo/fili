Troubleshooting
===============

The following offers some solutions to common issues when setting up and using Fili.
 If you come across any issues that you think belong here, please feel free to contribute.

Table of Contents
-----------------

- [My query isn't working](#debugging queries)


Debugging Queries
-----------------

If you make a query to Fili that doesn't work as expected it may be helpful to add the `format=debug` to the end of your query like below.

```
GET http://localhost:9998/v1/data/wikipedia/day?metrics=added&dateTime=2000-01-01/3000-01-01&format=debug
```

This lets you see the exact query which would have been sent to Druid. [See the Druid Querying Docs][druid-docs]



[druid-docs]: http://druid.io/docs/latest/querying/querying.html
