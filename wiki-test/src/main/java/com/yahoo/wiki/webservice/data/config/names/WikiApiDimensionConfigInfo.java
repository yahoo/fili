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
    COMMENT("Comment for the edit to the wiki page", "wiki comment"),
    COUNTRY_ISO_CODE("Iso Code of the country to which the wiki page belongs ", "wiki countryIsoCode"),
    REGION_ISO_CODE("Iso Code of the region to which the wiki page belongs ", "wiki regionIsoCode"),
    PAGE("Page is a document that is suitable for World Wide Web and web browsers", "wiki page"),
    USER("User is a person who generally use or own wiki services", "wiki user"),
    IS_UNPATROLLED("Unpatrolled are class of pages that are not been patrolled", "wiki isUnpatrolled"),
    IS_NEW("New Page is the first page that is created in wiki ", "wiki isNew"),
    IS_ROBOT("Robot is an tool that carries out repetitive and mundane tasks", "wiki isRobot"),
    IS_ANONYMOUS("Anonymous are individual or entity whose identity is unknown", "wiki isAnonymous"),
    IS_MINOR("Minor is a person who is legally considered a minor", "wiki isMinor"),
    NAMESPACE("Namespace is a set of wiki pages that begins with a reserved word", "wiki namespace"),
    CHANNEL("Channel is a set of wiki pages on a certain channel", "wiki channel"),
    COUNTRY_NAME("Name of the Country to which the wiki page belongs", "wiki countryName"),
    REGION_NAME("Name of the Region to which the wiki page belongs", "wiki regionName"),
    METRO_CODE("Metro Code to which the wiki page belongs", "wiki metroCode"),
    CITY_NAME("Name of the City to which the wiki page belongs", "wiki cityName");

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
