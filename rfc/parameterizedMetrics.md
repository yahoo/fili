Parameterized Metric Support
============================

Parameterized metrics are a subset of dynamic request metrics.  Specifically they focus on modifying the calculation of metrics by altering values used in the calculation of existing metrics or mapping their values at aggregation time.

The expected contract of parameterized metrics is as follows: Given a base metric with api name `metricName`, it can be suffixed with a procedure-call list of keyword parameter names and values.  These values will be used in the calculation of the resulting new metric.

So
```
"metricName(parameterName=parameterValue)"
```

will result in a dynamic metric based on `metricName`, but substituting the parameter value when calculating it.  Also, the above string will be used as the column name in the resulting report response schema. 

Motivation
----------

Standard metrics in Fili are configured at load time.  However if reporting requires many different similar caluclations, such as adjusting precision or converting values of different units into a shared aggregate, or applying custom metric filters, it may be impractical or even impossible to configure all the interesting report metrics.

With parameterized metrics, open-ended variations may be supported for a given conceptual metric calculation, simplifying configuration and possibly unlocking otherwise impractical freedom in metric calculation.


The structure of a parameterized metric:
----------------------------------------

```
metricName(parameterName=value)

metricName [ '(' parameterName=value [ ',' parameterName=value ]*  ')' ]?
```

metricName -> an api metric name which should appear as a resource at the `/v1/metrics/metricName` endpoint.

parameterName -> an identifier for a parameter supported by this metric.  Identifier names should follow the same format rules as api resource identifiers.

value -> a value to be used for creating a modified version of baseMetric.  URL encoding may be required if some of the characters in the value contain special characters.  Escaping values containing ampersand is mandatory and it may also be necessary to encode values containing comma or parentheses depending on implementation.

Values containing characters delimiters such as comma or parantheses must have them nested in other scoping characters such as quotes, square brackets or parantheses.

If multiple parameter names are used for this metric they should be joined by commas with no intermediate spaces (similar to the filters value syntax)

i.e.

```
metricName(parameterName=value,otherParameter=otherValue)
```


Response schema
---------------

Unless aliasing is supported and used (see below), responses will use the full string of the metric expression as the column name for values resulting from parametrized metric evaluation.

Example query:
```json
...&amp;metrics=revenue,netRevenue,revenue(currency=USD)
```

Example response:
```json
{
  "rows": [
    {
      "dateTime": "2018-02-01 00:00:00.000",
      "mediaType|id": "-2",
      "currency|id": "AUD",
      "revenue": "100",
      "revenue|currency": "AUD",
      "netRevenue": "50",
      "netRevenue|currency": "CAD",
      "revenue(currency=USD)": "120",
      "revenue(currency=USD)|currency": "USD"
    }
}
```

Reserved parameter names
------------------------

Two optional features which may be supported in some fili implementations are 'filters' and 'as'.  These parameter names should be treated as reserved when configuring metrics.

Dynamic aggregation filters, if supported, use the parameter name `filters`. Values will be parsed according to the same rules as the entries in the `filters` query parameter.  The expected behavior of the filters parameter is that rows in the base fact will not be aggregated unless the included filters is true.  This is potentially costly in performance and global filters should always be preferred to dynamic filters, if possible.

The `as` parameter name, if supported, will create an alias for the dynamic metric which will be used in the schema of the response as well as in the having clause of the request.


Aliasing with 'as'
------------------

Aliasing metrics provides a shorthand for the response and for the having clause, giving a parameterized metric expression an alternate, readable, name.

Example request:

```
?metrics=users(filters=dim1|id-in['a','b'],as=DimOneInAOrB)&having=DimOneInAOrB-gt[50]
```

This would create a variation of the standard `users` aggregation which only counted users from records where the dimension `dim1` had values "a" or "b", and would display result rows only which for which this metric had at least 50 users.

Example response:
```json
{
  "rows": [
    {
      "dateTime": "2018-02-01 00:00:00.000",
      "DimOneInAOrB": "100"
    }
}
```

Aliasing does not work WITHIN the metric clause itself.  Parameterized metrics can be defined in other blocks, but aliases cannot be parameterized themselves. 

Illegal:
```
?metrics=metric1(filters=dim1|id-in['a','b'],as=aliasName),aliasName
```

Also illegal:
```
?metrics=metric1(as=aliasName),&amp;having=aliasName(filters=dim1|id-in['a'])
```

Duplicate Metrics
-----------------

Currently metrics with identical meanings and representations within an API Request produce a 400 error of the form 'Duplicate metric(s) are not allowed in the API request.'  Parameterized metrics may allow the creation of metrics with the same meaning but different definitions. (Such as the same metric expression with distinct aliases or two terms with parameters in a different order but the same pairs).  These may or may not be considered 'duplicates' as an implementation detail.  

Regardless, the actual string label used to define a metric, and not a 'normalized' form should be preserved in the response schema.


Parameterized Metric Metadata
=============================

The metrics endpoint (and the corresponding serialization in the fullView format on the tables endpoint) will now contain an optional field for parameters.

```
{ 
    name:"grossRevenue",
    type:"money"
    parameters: {
        currency: {  
            type: dimension, 
            dimensionName: "currencyUnits",
            defaultValue:"billing" 
        }
    }
}
```

Parameters will be a JSON object whose keys are parameter names supported on this metric.

The values of a parameter entry will contain a mandatory field 'type'.

In this example, the `dimension` supports fields `dimensionName` and `defaultValue`.  Dimension name is the name of a dimension whose values are legal for this parameter.  

The `defaultValue` field is an optimization for UI clients to provide a 'recommended' value for the parameter, if appropriate.  `defaultValue`, if used, is recommended to be equivalent to not supplying a value for this parameter at all.

For conciseness, the default output format for the metric endpoint will NOT include reserved word system-wide parameters.  A query parameter for verbose expansion may be subsequently defined. 

Parameter Types
---------------

Recommended types to be implemented:

`identifier`: The parameter value will be parsed as an identifier string.

`filters`: The parameter accepts values parsed identically to the filters query parameter.

`dimension`: The parameter accepts values from a dimension.

Proposed future type extensions:

`enum`: The parameter accepts values from an included list of values.

`number`: The parameter accepts simple (signed) numeric values.

`date`: The parameter accepts values that parse as the DateTime part of the interval used within dateTime query parameter.

`interval`: The parameter accepts intervals as used within dateTime query parameter.

Parameter Identifiers
---------------------

Parameters need to have the same constraints as resource names such as those at the metrics endpoints. By default any java variable identifier style (non numeric alpha followed by alphanumeric, '_' if necessary).  Mix case is encouraged but not mandatory. 

Parameter keywords are presumed to be case sensitive, although fili implementations can soften this restriction.

Reserved Parameter Types
------------------------

The `filters` reserved parameter should have an implicit type of `filters`

The `as` reserved parameter should have an implicit type of `identifier`

Parameter Metadata
------------------

The dimension parameter type: 

```json
{
  type: dimension, 
  dimensionName: [DIMENSION_NAME],
  defaultValue: [DEFAULT_VALUE] 
}
```

**dimensionName**:  Mandatory.  This must be a dimension available at /v1/data/dimensions/DIMENSION_NAME

This dimension does not need to be attached to any tables in the system.

**defaultValue**:  Optional.  "" if not default value is defined.

This value should usually be defined with the same behavior as the parameter being omitted.
    
Error Handling
==============

Based on the current requirements, support for anything but the simplest set of parameters will require a grammar.  The following errors should be generally expected:

**"Unparseable Metrics"**:  If the grammar or tokenizing logic cannot process the metrics expression at all.

**"Invalid Metric Name"**: If the metric name before the parameters cannot be associated with a base metric name.

**"Unsupported Parameter"**: If the parameter name in a parameter list doesn't exist on that metric.

**"Invalid Parameter Value"**: If the parameter value doesn't match the parsing rules for the type of the parameter.  

**"Duplicate Metric"**: If an 'identical' metric is defined on the metrics list.

**"Unexpected Having Metric"**: If a metric described in the having expression isn't represented in the metrics section.
