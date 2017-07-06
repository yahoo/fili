// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.auto;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;

import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Created by kevin on 3/7/2017.
 */
public class StaticWikiConfigLoader implements Supplier<List<? extends DataSourceConfiguration>> {

    public static DataSourceConfiguration getWikiDruidConfig() {
        return new DataSourceConfiguration() {
            @Override
            public String getPhysicalTableName() {
                return "wikiticker";
            }

            @Override
            public String getApiTableName() {
                return getPhysicalTableName();
            }

            @Override
            public TableName getTableName() {
                return this::getPhysicalTableName;
            }

            @Override
            public List<String> getMetrics() {
                return Arrays.asList(
                        "count",
                        "added",
                        "deleted",
                        "delta",
                        "user_unique"
                );
            }

            @Override
            public List<String> getDimensions() {
                return Arrays.asList(
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
            public ZonedTimeGrain getZonedTimeGrain() {
                return new ZonedTimeGrain(
                        DefaultTimeGrain.HOUR,
                        DateTimeZone.UTC
                );
            }

            @Override
            public List<TimeGrain> getValidTimeGrains() {
                return Collections.singletonList(DefaultTimeGrain.HOUR);
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
