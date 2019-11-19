// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.database;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

//CHECKSTYLE:OFF
//This is all generated code for a wikiticker entry
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "time",
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
        "user",
        "delta",
        "added",
        "deleted"
})
public class WikitickerEntry implements Serializable {

    private final static long serialVersionUID = -1031169649797854741L;
    @JsonProperty("time")
    private String time;
    @JsonProperty("channel")
    private String channel;
    @JsonProperty("cityName")
    private String cityName;
    @JsonProperty("comment")
    private String comment;
    @JsonProperty("countryIsoCode")
    private String countryIsoCode;
    @JsonProperty("countryName")
    private String countryName;
    @JsonProperty("isAnonymous")
    private boolean isAnonymous;
    @JsonProperty("isMinor")
    private boolean isMinor;
    @JsonProperty("isNew")
    private boolean isNew;
    @JsonProperty("isRobot")
    private boolean isRobot;
    @JsonProperty("isUnpatrolled")
    private boolean isUnpatrolled;
    @JsonProperty("metroCode")
    private String metroCode;
    @JsonProperty("namespace")
    private String namespace;
    @JsonProperty("page")
    private String page;
    @JsonProperty("regionIsoCode")
    private String regionIsoCode;
    @JsonProperty("regionName")
    private String regionName;
    @JsonProperty("user")
    private String user;
    @JsonProperty("delta")
    private int delta;
    @JsonProperty("added")
    private int added;
    @JsonProperty("deleted")
    private int deleted;
    @JsonIgnore
    private Map<String, String> additionalProperties = new HashMap<>();

    /**
     * No args constructor for use in serialization
     */
    public WikitickerEntry() {
    }

    /**
     * @param countryIsoCode
     * @param countryName
     * @param added
     * @param isRobot
     * @param isNew
     * @param delta
     * @param cityName
     * @param regionName
     * @param isAnonymous
     * @param deleted
     * @param namespace
     * @param time
     * @param page
     * @param isMinor
     * @param regionIsoCode
     * @param metroCode
     * @param isUnpatrolled
     * @param comment
     * @param user
     * @param channel
     */
    public WikitickerEntry(
            String time,
            String channel,
            String cityName,
            String comment,
            String countryIsoCode,
            String countryName,
            boolean isAnonymous,
            boolean isMinor,
            boolean isNew,
            boolean isRobot,
            boolean isUnpatrolled,
            String metroCode,
            String namespace,
            String page,
            String regionIsoCode,
            String regionName,
            String user,
            int delta,
            int added,
            int deleted
    ) {
        super();
        this.time = time;
        this.channel = channel;
        this.cityName = cityName;
        this.comment = comment;
        this.countryIsoCode = countryIsoCode;
        this.countryName = countryName;
        this.isAnonymous = isAnonymous;
        this.isMinor = isMinor;
        this.isNew = isNew;
        this.isRobot = isRobot;
        this.isUnpatrolled = isUnpatrolled;
        this.metroCode = metroCode;
        this.namespace = namespace;
        this.page = page;
        this.regionIsoCode = regionIsoCode;
        this.regionName = regionName;
        this.user = user;
        this.delta = delta;
        this.added = added;
        this.deleted = deleted;
    }

    @JsonProperty("time")
    public String getTime() {
        return time;
    }

    @JsonProperty("time")
    public void setTime(String time) {
        this.time = time;
    }

    @JsonProperty("channel")
    public String getChannel() {
        return channel;
    }

    @JsonProperty("channel")
    public void setChannel(String channel) {
        this.channel = channel;
    }

    @JsonProperty("cityName")
    public String getCityName() {
        return cityName;
    }

    @JsonProperty("cityName")
    public void setCityName(String cityName) {
        this.cityName = cityName;
    }

    @JsonProperty("comment")
    public String getComment() {
        return comment;
    }

    @JsonProperty("comment")
    public void setComment(String comment) {
        this.comment = comment;
    }

    @JsonProperty("countryIsoCode")
    public String getCountryIsoCode() {
        return countryIsoCode;
    }

    @JsonProperty("countryIsoCode")
    public void setCountryIsoCode(String countryIsoCode) {
        this.countryIsoCode = countryIsoCode;
    }

    @JsonProperty("countryName")
    public String getCountryName() {
        return countryName;
    }

    @JsonProperty("countryName")
    public void setCountryName(String countryName) {
        this.countryName = countryName;
    }

    @JsonProperty("isAnonymous")
    public boolean getIsAnonymous() {
        return isAnonymous;
    }

    @JsonProperty("isAnonymous")
    public void setIsAnonymous(boolean isAnonymous) {
        this.isAnonymous = isAnonymous;
    }

    @JsonProperty("isMinor")
    public boolean getIsMinor() {
        return isMinor;
    }

    @JsonProperty("isMinor")
    public void setIsMinor(boolean isMinor) {
        this.isMinor = isMinor;
    }

    @JsonProperty("isNew")
    public boolean getIsNew() {
        return isNew;
    }

    @JsonProperty("isNew")
    public void setIsNew(boolean isNew) {
        this.isNew = isNew;
    }

    @JsonProperty("isRobot")
    public boolean getIsRobot() {
        return isRobot;
    }

    @JsonProperty("isRobot")
    public void setIsRobot(boolean isRobot) {
        this.isRobot = isRobot;
    }

    @JsonProperty("isUnpatrolled")
    public boolean getIsUnpatrolled() {
        return isUnpatrolled;
    }

    @JsonProperty("isUnpatrolled")
    public void setIsUnpatrolled(boolean isUnpatrolled) {
        this.isUnpatrolled = isUnpatrolled;
    }

    @JsonProperty("metroCode")
    public String getMetroCode() {
        return metroCode;
    }

    @JsonProperty("metroCode")
    public void setMetroCode(String metroCode) {
        this.metroCode = metroCode;
    }

    @JsonProperty("namespace")
    public String getNamespace() {
        return namespace;
    }

    @JsonProperty("namespace")
    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    @JsonProperty("page")
    public String getPage() {
        return page;
    }

    @JsonProperty("page")
    public void setPage(String page) {
        this.page = page;
    }

    @JsonProperty("regionIsoCode")
    public String getRegionIsoCode() {
        return regionIsoCode;
    }

    @JsonProperty("regionIsoCode")
    public void setRegionIsoCode(String regionIsoCode) {
        this.regionIsoCode = regionIsoCode;
    }

    @JsonProperty("regionName")
    public String getRegionName() {
        return regionName;
    }

    @JsonProperty("regionName")
    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }

    @JsonProperty("user")
    public String getUser() {
        return user;
    }

    @JsonProperty("user")
    public void setUser(String user) {
        this.user = user;
    }

    @JsonProperty("delta")
    public int getDelta() {
        return delta;
    }

    @JsonProperty("delta")
    public void setDelta(int delta) {
        this.delta = delta;
    }

    @JsonProperty("added")
    public int getAdded() {
        return added;
    }

    @JsonProperty("added")
    public void setAdded(int added) {
        this.added = added;
    }

    @JsonProperty("deleted")
    public int getDeleted() {
        return deleted;
    }

    @JsonProperty("deleted")
    public void setDeleted(int deleted) {
        this.deleted = deleted;
    }

    @JsonAnyGetter
    public Map<String, String> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, String value) {
        this.additionalProperties.put(name, value);
    }
}
//CHECKSTYLE:ON
