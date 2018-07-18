-- Copyright 2018 Yahoo Inc.
-- Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

local parser = require("utils.jsonParser")
local dimension_utils = require("utils.dimensionUtils")

local M = {
    dimensions = {}
}

-------------------------------------------------------------------------------
-- Default
-------------------------------------------------------------------------------

DEFAULT = {
    CATEGORY = "General"
}

-------------------------------------------------------------------------------
-- FieldSets
-------------------------------------------------------------------------------

-- fieldSet name = {pk(field as primary key), f(other fields)}
FIELDSETS = {
    DEFAULT = { pk "ID", field "DESC" },
    COUNTRY = { pk "ID", field ("DESC", "COUNTY", "STATE")},
    PAGE = { pk "ID", field "DESC" }
}

-------------------------------------------------------------------------------
-- Dimensions
-------------------------------------------------------------------------------

-- dimension name = {longName, description, fields, categories}
DIMENSIONS = {
    COMMENT = {"wiki comment", "Comment for the edit to the wiki page", FIELDSETS.DEFAULT, nil},
    COUNTRY_ISO_CODE = { "wiki countryIsoCode", "Iso Code of the country to which the wiki page belongs", FIELDSETS.COUNTRY, DEFAULT.CATEGORY },
    REGION_ISO_CODE = { "wiki regionIsoCode", "Iso Code of the region to which the wiki page belongs", FIELDSETS.DEFAULT, nil },
    PAGE = { "wiki page", "Page is a document that is suitable for World Wide Web and web browsers", FIELDSETS.PAGE, nil },
    USER = { "wiki user", "User is a person who generally use or own wiki services", { pk "ID", field("DESC", "AGE", "SEX") }, nil },
    IS_UNPATROLLED = {"wiki isUnpatrolled", "Unpatrolled are class of pages that are not been patrolled", FIELDSETS.DEFAULT, nil},
    IS_NEW = {"wiki isNew", "New Page is the first page that is created in wiki", FIELDSETS.DEFAULT, nil},
    IS_ROBOT = {"wiki isRobot", "Robot is an tool that carries out repetitive and mundane tasks", FIELDSETS.DEFAULT, nil},
    IS_ANONYMOUS = {"wiki isAnonymous", "Anonymous are individual or entity whose identity is unknown", FIELDSETS.DEFAULT, nil},
    IS_MINOR = {"wiki isMinor", "Minor is a person who is legally considered a minor", FIELDSETS.DEFAULT, nil},
    NAMESPACE = {"wiki namespace", "Namespace is a set of wiki pages that begins with a reserved word", FIELDSETS.DEFAULT, nil},
    CHANNEL = {"wiki channel", "Channel is a set of wiki pages on a certain channel", FIELDSETS.DEFAULT, nil},
    COUNTRY_NAME = {"wiki countryName", "Name of the Country to which the wiki page belongs", FIELDSETS.DEFAULT, nil},
    REGION_NAME = {"wiki regionName", "Name of the Region to which the wiki page belongs", FIELDSETS.DEFAULT, nil},
    METRO_CODE = {"wiki metroCode", "Metro Code to which the wiki page belongs", FIELDSETS.DEFAULT, nil},
    CITY_NAME = {"wiki cityName", "Name of the City to which the wiki page belongs", FIELDSETS.DEFAULT, nil}
}

dimension_utils.add_dimensions(DIMENSIONS, M.dimensions)
parser.save("../DimensionConfig.json", M)

return M
