// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.auto;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.util.Utils;

import org.apache.druid.timeline.DataSegment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Static configuration for Wikipedia.
 */
public class StaticWikiConfigLoader implements Supplier<List<? extends DataSourceConfiguration>> {

    public static DataSourceConfiguration getWikiDruidConfig() {
        return new DataSourceConfiguration() {
            @Override
            public String getName() {
                return "wikiticker";
            }

            @Override
            public TableName getTableName() {
                return this::getName;
            }

            @Override
            public Set<String> getMetrics() {
                return Utils.asLinkedHashSet(
                        "count",
                        "added",
                        "deleted",
                        "delta",
                        "user_unique"
                );
            }

            @Override
            public List<DataSegment> getDataSegments() {
                return Collections.emptyList();
            }

            @Override
            public Set<String> getDimensions() {
                return Utils.asLinkedHashSet(
                        "channel",
                        "cityName",
                        "comment",
                        "countryIsoCode",
                        "countryName",
                        "isAnonymous",
                        "isMinor",
                        "isNew",
                        "isRobot",
                        "isUnpatrolled",
                        "metroCode",
                        "namespace",
                        "page",
                        "regionIsoCode",
                        "regionName",
                        "user"
                );
            }

            @Override
            public TimeGrain getValidTimeGrain() {
                return DefaultTimeGrain.HOUR;
            }
        };
    }

    @Override
    public List<? extends DataSourceConfiguration> get() {
        List<DataSourceConfiguration> dataSourceConfigurations = new ArrayList<>();
        dataSourceConfigurations.add(getWikiDruidConfig());
        return dataSourceConfigurations;
    }
}
