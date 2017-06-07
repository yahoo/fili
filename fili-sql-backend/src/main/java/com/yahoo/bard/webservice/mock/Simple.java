// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.mock;

import com.yahoo.bard.webservice.data.config.dimension.DefaultDimensionField;
import com.yahoo.bard.webservice.data.config.dimension.DefaultKeyValueStoreDimensionConfig;
import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.MapStoreManager;
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension;
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager;
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.druid.model.aggregation.DoubleSumAggregation;
import com.yahoo.bard.webservice.druid.model.datasource.DataSource;
import com.yahoo.bard.webservice.druid.model.datasource.TableDataSource;
import com.yahoo.bard.webservice.druid.model.filter.SearchFilter;
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.ConstantPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation;
import com.yahoo.bard.webservice.druid.model.query.TimeSeriesQuery;
import com.yahoo.bard.webservice.metadata.DataSourceMetadata;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.ConstrainedTable;
import com.yahoo.bard.webservice.table.StrictPhysicalTable;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.util.Utils;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by hinterlong on 5/31/17.
 */
public class Simple {


}
