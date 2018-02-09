package com.yahoo.bard.webservice.application.metadataViews;

import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.web.TableView;

import javax.ws.rs.container.ContainerRequestContext;

public class IndividualTableViewProvider implements MetadataViewProvider<LogicalTable> {

    @Override
    public Object apply(
            ContainerRequestContext containerRequestContext,
            LogicalTable logicalTable
    ) {
        TableView resultRow = new TableView();
        resultRow.put("name", logicalTable.getName());
        resultRow.put("longName", logicalTable.getLongName());
        resultRow.put("description", logicalTable.getDescription());
        resultRow.put("category", logicalTable.getCategory());
        return resultRow;
    }
}
