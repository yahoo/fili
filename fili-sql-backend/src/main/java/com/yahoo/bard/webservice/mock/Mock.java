package com.yahoo.bard.webservice.mock;

import com.yahoo.bard.webservice.data.time.DefaultTimeGrain;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.druid.model.datasource.DataSource;
import com.yahoo.bard.webservice.druid.model.datasource.TableDataSource;
import com.yahoo.bard.webservice.druid.model.query.TimeSeriesQuery;
import com.yahoo.bard.webservice.metadata.DataSourceMetadata;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.ConstrainedTable;
import com.yahoo.bard.webservice.table.StrictPhysicalTable;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Created by hinterlong on 5/31/17.
 */
public class Mock {
    private Mock() {

    }

    public static MockDruidResponse mockDruidResponse() {
        MockDruidResponse mockResponse = new MockDruidResponse();
        MockDruidResponse.TimeStampResult time1 = new MockDruidResponse.TimeStampResult(DateTime.now());
        time1.add("sample_name1", 0d);
        time1.add("sample_name2", 1d);
        mockResponse.results.add(time1);
        MockDruidResponse.TimeStampResult time2 = new MockDruidResponse.TimeStampResult(DateTime.now());
        time2.add("sample_name1", 0d);
        time2.add("sample_name2", 1d);
        mockResponse.results.add(time2);
        return mockResponse;
    }

    public static TimeSeriesQuery timeSeriesQuery(String name) {
        return new TimeSeriesQuery(
                dataSource(name),
                DefaultTimeGrain.DAY,
                null,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptySet()

        );
    }

    private static DataSource dataSource(String name) {
        ZonedTimeGrain zonedTimeGrain = new ZonedTimeGrain(DefaultTimeGrain.HOUR, DateTimeZone.UTC);
        Set<Column> columns = Collections.emptySet();
        Map<String, String> logicalToPhysicalColumnNames = Collections.emptyMap();
        DataSourceMetadataService metadataService = new DataSourceMetadataService();
        metadataService.update(() -> name, new DataSourceMetadata(name, Collections.emptyMap(), Collections.emptyList()));
        return new TableDataSource(new ConstrainedTable(
                new StrictPhysicalTable(
                        () -> name,
                        zonedTimeGrain,
                        columns,
                        logicalToPhysicalColumnNames,
                        metadataService
                ),
                new DataSourceConstraint(
                        Collections.emptySet(),
                        Collections.emptySet(),
                        Collections.emptySet(),
                        Collections.emptySet(),
                        Collections.emptySet(),
                        Collections.emptySet(),
                        Collections.emptySet(),
                        Collections.emptyMap()
                )

        ));
    }
}
