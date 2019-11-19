// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.config.names.DimensionName;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;

import java.util.LinkedHashSet;
import java.util.List;

import javax.validation.constraints.NotNull;

/**
 * A Default Lookup Dimension holds all of the information needed to construct a Lookup Dimension.
 */
public class DefaultLookupDimensionConfig extends DefaultKeyValueStoreDimensionConfig implements LookupDimensionConfig {
  private final List<String> namespaces;

  /**
   * Construct a LookupDefaultDimensionConfig instance from dimension name, dimension fields and
   * default dimension fields.
   *
   * @param apiName  The API Name is the external, end-user-facing name for the dimension.
   * @param physicalName  The internal, physical name for the dimension.
   * @param description  A description of the dimension and its meaning.
   * @param longName  The Long Name is the external, end-user-facing long  name for the dimension.
   * @param category  The Category is the external, end-user-facing category for the dimension.
   * @param fields  The set of fields for this dimension.
   * @param defaultDimensionFields  The default set of fields for this dimension to be shown in the response.
   * @param keyValueStore  The key value store holding dimension row data.
   * @param searchProvider  The search provider for field value lookups on this dimension.
   * @param namespaces  A list of namespaces used to configure the Lookup dimension.
   */
  public DefaultLookupDimensionConfig(
      @NotNull DimensionName apiName,
      String physicalName,
      String description,
      String longName,
      String category,
      @NotNull LinkedHashSet<DimensionField> fields,
      @NotNull LinkedHashSet<DimensionField> defaultDimensionFields,
      @NotNull KeyValueStore keyValueStore,
      @NotNull SearchProvider searchProvider,
      @NotNull List<String> namespaces
  ) {
    super (
        apiName,
        physicalName,
        description,
        longName,
        category,
        fields,
        defaultDimensionFields,
        keyValueStore,
        searchProvider
    );
    this.namespaces = namespaces;
  }

  /**
   * Construct a LookupDefaultDimensionConfig instance from dimension name and only using default dimension fields.
   *
   * @param apiName  The API Name is the external, end-user-facing name for the dimension.
   * @param physicalName  The internal, physical name for the dimension.
   * @param description  A description of the dimension and its meaning.
   * @param longName  The Long Name is the external, end-user-facing long  name for the dimension.
   * @param category  The Category is the external, end-user-facing category for the dimension.
   * @param fields  The set of fields for this dimension, this set of field will also be used for the default fields.
   * @param keyValueStore  The key value store holding dimension row data.
   * @param searchProvider  The search provider for field value lookups on this dimension.
   * @param namespaces  A list of namespaces used to configure the Lookup dimension.
   */
  public DefaultLookupDimensionConfig(
      @NotNull DimensionName apiName,
      String physicalName,
      String description,
      String longName,
      String category,
      @NotNull LinkedHashSet<DimensionField> fields,
      @NotNull KeyValueStore keyValueStore,
      @NotNull SearchProvider searchProvider,
      @NotNull List<String> namespaces
  ) {
    this (
        apiName,
        physicalName,
        description,
        longName,
        category,
        fields,
        fields,
        keyValueStore,
        searchProvider,
        namespaces
    );
  }

  @Override
  public List<String> getNamespaces() {
    return namespaces;
  }
}
