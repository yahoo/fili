// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.names;

import com.yahoo.bard.webservice.data.config.names.DimensionName;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.util.EnumUtils;

/**
 * Hold all the Wikipedia API dimension names.
 */
public enum WikiApiDimensionConfigInfo implements DimensionName {
    PAGE("Page is a document that is suitable for World Wide Web and web browsers", "wiki page"),
    LANGUAGE("Language used to write the wiki page", "wiki language"),
    USER("User is a person who generally use or own wiki services", "wiki user"),
    UNPATROLLED("Unpatrolled are class of pages that are not been patrolled", "wiki unpatrolled"),
    NEW_PAGE("New Page is the first page that is created in wiki ", "wiki newPage"),
    ROBOT("Robot is an tool that carries out repetitive and mundane tasks", "wiki robot"),
    ANONYMOUS("Anonymous are individual or entity whose identity is unknown", "wiki anonymous"),
    NAMESPACE("Namespace is a set of wiki pages that begins with a reserved word", "wiki namespace"),
    CONTINENT("Name of the Continent to which the wiki page belongs ", "wiki continent"),
    COUNTRY("Name of the Country to which the wiki page belongs", "wiki country"),
    REGION("Name of the Region to which the wiki page belongs", "wiki region"),
    CITY("Name of the City to which the wiki page belongs", "wiki city")
    ;

    private final String camelName;
    private final String description;
    private final String longName;

    /**
     * Constructor of Wikipedia Dimension Name.
     *
     * @param description  A description of the dimension and its meaning.
     * @param longName  The Long Name is the external, end-user-facing long  name for the dimension.
     */
    WikiApiDimensionConfigInfo(String description, String longName) {
        this.camelName = EnumUtils.camelCase(name());
        this.description = description;
        this.longName = longName;
    }

    /**
     * View the dimension name.
     *
     * @return The camelCase version of the dimension.
     */
    @Override
    public String asName() {
        return camelName;
    }

    public String getDescription() {
        return this.description;
    }

    public String getLongName() {
        return this.longName;
    }

    public String getCategory() {
        return Dimension.DEFAULT_CATEGORY;
    }
}
