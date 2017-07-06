File Configuration
==================

How To Run It
-------------
1. Configure your json schema (follow the example below)
2. Place your `tables.json` under `src/main/resources` in `fili-generic-example`
3. In `applicationConfig.properties`, set `bard__table_config` to `tables.json`.
    
    **Note: The generic example normally tries to automatically configure itself, but specifying
    `bard__table_config` forces it to load from the file.**

4. Run in `GenericMain` in Intellij or `mvn -pl fili-generic-example exec:java`


Table Schema Configuration
--------------------------
### Json Layout

How to configure Fili using a json schema of your table.

The json format is:

```json
{
  "tables": [
    {
      PLACE TABLE HERE  
    }
  ]
}
```

### Full Table Schema

#### Table Schema

| field                 | required | description           
|-----------------------|----------|-----------------------
| `"apiTableName"`      | yes      | The unique name of the table for fili.
| `"physicalTableName"` | no       | The physical name for the table in the backend (i.e. Druid). By default assumed to be the same as `"apiTableName"`.
| `"zonedTimeGrain"`    | yes      | The minimum valid timegrain for this table along with it's timezone.
|  `"timeGrains"`       | yes      | An array of valid timegrains {`MINUTE`, `HOUR`, `DAY`, `WEEK`, `MONTH`, `QUARTER`, `YEAR`}.
| `"category"`          | no       | The category for the logical table.
| `"longName"`          | no       | The longer, more descriptive name for the logical table.
| `"description"`       | no       | The description for the logical table.
| `"metrics"`           | yes      | metrics to load
| `"dimensions"`        | yes      | dimensions to load


#### Metrics Schema

| field                  | required | description           
|------------------------|----------|-----------------------
| `"apiMetricName"`      | yes      | The unique name of the metric for fili.
| `"dependentMetrics"`   | depends  | The physical name for the metric in the backend (i.e. Druid). By default assumed to be `"apiMetricName"`. Required for `arithmetic` type.
| `"type"`               | yes      | The type of aggregation to be performed {`longMin`, `longMax`, `longSum`, `doubleMin`, `doubleMax`, `doubleSum`, `arithmetic`, `aggregationAverage`, `count`, `rowNum`}.
| `"params"`             | depends  | For `arithmetic`, should be one of {`"+"`,`"-"`,`"*"`,`"/"`,}. For `aggregationAverage`, should be a single time grain.
| `"timeGrains"`         | no       | If you would like to override the timegrains for this metrics. For example, a metric could only be valid for a `["DAY"]` grain while the table supports both `["HOUR", "DAY"]`.


#### Dimensions Schema

| field                     | required | description           
|---------------------------|----------|-----------------------
| `"apiDimensionName"`      | yes      | The unique name of the metric in fili.
| `"physicalDimensionName"` | no       | The physical name for the dimension in the backend (i.e. Druid). By default assumed to be the same as `"apiDimensionName"`.
| `"description"`           | no       | A description for the dimension.
| `"longName"`              | no       | A more descriptive name for the dimension.
| `"category"`              | no       | A category describing this dimension, defaults to `"General"`.


Example
-------

Let's say we want to set up a table called `wikipedia` (as in the Wikipedia Example).

1. We want to refer to the table as `wikipedia` in fili and use `wikiticker` table from Druid.

    ```json
    "apiTableName": "wikipedia",
    "physicalTableName": "wikiticker",
    ```

2. Let's make this table valid over `HOUR` and `DAY` timegrains. The `"zonedTimeGrain/timeGrain"` should be the
  smaller of the timegrains.
 
    ```json
    "zonedTimeGrain": {
      "timeGrain": "HOUR",
      "timeZone": "UTC"
    },
    "timeGrains": [
      "HOUR",
      "DAY"
    ]
    ```
3. Let's add some metrics. We'll make `added` be a `doubleSum` and `deleted` be a `longSum`. We'll also give them a 
more descriptive api name. We'll also manually calculate the delta for each edit as `manualDelta` and compare it to `delta` from Druid.

    *Note: There is no need to have different api and physical names unless you have another metric with the same name.*

    ```json
    "metrics": [
      {
        "apiMetricName": "charactersAdded",
        "dependentMetrics": [ "added" ],
        "type": "doubleSum"
      },
      {
        "apiMetricName": "charactersDeleted",
        "dependentMetrics": [ "deleted" ],
        "type": "longSum"
      },
      {
        "apiMetricName": "delta",
        "type": "doubleSum"
      },
      {
        "apiMetricName": "manualDelta",
        "dependentMetrics": [ "charactersAdded", "charactersDeleted" ],
        "type": "arithmetic",
        "params": [ "-" ]
      }
    ]
    ```

4. Let's add some dimensions. We'll add `countryIsoCode`.

    ```json
    "dimensions": [
      {
        "apiDimensionName": "countryIsoCode",
        "description": "The iso code of the country the edit came from.",
        "longName": "wiki page countryIsoCode",
        "category": "External"
      }
    ]
    ```
    
5. Putting it all together we have

    ```json
    {
      "tables": [
        {
          "apiTableName": "wikipedia",
          "physicalTableName": "wikiticker",
          "zonedTimeGrain": {
            "timeGrain": "HOUR",
            "timeZone": "UTC"
          },
          "timeGrains": [
            "HOUR",
            "DAY"
          ],
          "metrics": [
            {
              "apiMetricName": "charactersAdded",
              "dependentMetrics": [ "added" ],
              "type": "doubleSum"
            },
            {
              "apiMetricName": "charactersDeleted",
              "dependentMetrics": [ "deleted" ],
              "type": "longSum"
            },
            {
              "apiMetricName": "delta",
                 "type": "doubleSum"
            },
            {
              "apiMetricName": "manualDelta",
              "dependentMetrics": [ "charactersAdded", "charactersDeleted" ],
              "type": "arithmetic",
              "params": [ "-" ]
            }
          ],
          "dimensions": [
            {
              "apiDimensionName": "countryIsoCode",
              "description": "The iso code of the country the edit came from.",
              "longName": "wiki page countryIsoCode",
              "category": "External"
            }
          ]
        }
      ]
    }
    ```

6. Now that our table is configured, look at the steps to run it and try it out!