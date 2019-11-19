// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util

import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.Paths
import java.util.stream.Stream

class UtilsSpec extends Specification {

    private static final TEMP_DIR = "target/tmp/util/"

    def cleanup() {
        Files.deleteIfExists(Paths.get(TEMP_DIR))
    }

    def "Get by sub-type returns all instances of that sub-type"() {
        given: "a collection of sets which share the same HashSet class"
        Collection set = [["a"] as HashSet, ["b"] as LinkedHashSet]

        expect: "getting by LinkedHashSet returns LinkedHashSet instance"
        Utils.getSubsetByType(set, LinkedHashSet) == [["b"] as LinkedHashSet] as LinkedHashSet

        and: "getting by HashSet returns both HashSet and LinkedHashSet instances"
        Utils.getSubsetByType(set, HashSet) == set as LinkedHashSet
    }

    def "File path without parent dir generates the parent"() {
        given: "a file path without a parent directory"
        String path = TEMP_DIR + "data.txt"

        expect: "parent directory does not exist yet"
        ! new File(TEMP_DIR).exists()

        when: "create a parent for the path"
        Utils.createParentDirectories(path)

        then: "the parent now exists"
        new File(TEMP_DIR).exists()
    }

    def "All files & dirs under a directory, including the directory itself, are deleted"() {
        setup: "create the parent directory"
        Files.createDirectory(Paths.get(TEMP_DIR))

        and: "initialize directory contents"
        String file1 = TEMP_DIR + "data1.txt"
        String file2 = TEMP_DIR + "data2.txt"
        String subDir = TEMP_DIR + "subDir/"
        String subFile = TEMP_DIR + "subDir/data.txt"

        and: "create the directory contents"
        Files.createFile(Paths.get(file1))
        Files.createFile(Paths.get(file2))
        Files.createDirectory(Paths.get(subDir))
        Files.createFile(Paths.get(subFile))

        expect: "all files and sub-dir exist"
        new File(file1).isFile()
        new File(file2).isFile()
        new File(subDir).isDirectory()
        new File(subFile).isFile()

        new File(file1).exists()
        new File(file2).exists()
        new File(subDir).exists()
        new File(subFile).exists()

        when: "delete the parent directory"
        Utils.deleteFiles(TEMP_DIR)

        then: "all files & dirs under a directory, including the directory itself, are deleted"
        ! new File(file1).exists()
        ! new File(file2).exists()
        ! new File(subDir).exists()
        ! new File(subFile).exists()
        ! new File(TEMP_DIR).exists()
    }

    def "Reduce operation returns the minimum of a stream in values"() {
        expect:
        Stream.of(1, 2, 3).reduce() { a, b -> Utils.getMinValue(a, b) }.get() == 1
    }
}
