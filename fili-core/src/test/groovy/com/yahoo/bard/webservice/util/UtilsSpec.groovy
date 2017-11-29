// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util

import org.apache.commons.io.FileUtils

import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.Path

class UtilsSpec extends Specification {

    String fromDir
    String toDir

    Path fromPath
    Path toPath

    Path file1
    Path file2
    Path file3
    Path subDir
    Path file4

    void setup() {
        fromDir = "target/tmp/dimensionCache/animal/new_lucene_indexes"
        toDir = "target/tmp/dimensionCache/animal/lucene_indexes"

        fromPath = Files.createDirectory(new File(fromDir).getAbsoluteFile().toPath())
        toPath = Files.createDirectory(new File(toDir).getAbsoluteFile().toPath())

        file1 = fromPath.resolve("segments_1")
        file2 = fromPath.resolve("_1.cfs")
        file3 = fromPath.resolve("_1.si")
        subDir = fromPath.resolve("subDir")

        Files.createFile(file1)
        Files.createFile(file2)
        Files.createFile(file3)
        Files.createDirectory(subDir)
        file4 = subDir.resolve("subDirFile")
        Files.createFile(file4)
    }

    void cleanup() {
        FileUtils.deleteDirectory(new File(fromDir))
        FileUtils.deleteDirectory(new File(toDir))
    }

    def "moveDirEntries moves all files under a directory to a new direcotry"() {
        expect: "all entries exist in old directory"
        Files.exists(fromPath)
        Files.exists(file1)
        Files.exists(file2)
        Files.exists(file3)
        Files.exists(subDir)
        Files.exists(file4)

        and: "new directory is empty"
        !Files.exists(toPath.resolve("segments_1"))
        !Files.exists(toPath.resolve("_1.cfs"))
        !Files.exists(toPath.resolve("_1.si"))
        !Files.exists(toPath.resolve("subDir"))
        !Files.exists(toPath.resolve("subDir").resolve("subDirFile"))

        when: "all files are moved from old(fromDir) to new(toDir) directory"
        Utils.moveDirEntries(fromDir, toDir)

        then: "all files in old directory are gone, but directories(empty now) remain at the same place"
        Files.exists(fromPath)
        !Files.exists(file1)
        !Files.exists(file2)
        !Files.exists(file3)
        Files.exists(subDir)
        !Files.exists(file4)

        and: "the files are now in new directory while preserving the same directory structure"
        Files.exists(toPath.resolve("segments_1"))
        Files.exists(toPath.resolve("_1.cfs"))
        Files.exists(toPath.resolve("_1.si"))
        Files.exists(toPath.resolve("subDir"))
        Files.exists(toPath.resolve("subDir").resolve("subDirFile"))
    }

    def "deleteFiles deletes all entries under a specified directory and the directory itself"() {
        expect: "all files and sub-directories exist in the directory"
        Files.exists(fromPath)
        Files.exists(file1)
        Files.exists(file2)
        Files.exists(file3)
        Files.exists(subDir)
        Files.exists(file4)

        when: "we delete the directory"
        Utils.deleteFiles(fromDir)

        then: "all entries under the directory and the directory itself are gone, as well"
        !Files.exists(fromPath)
        !Files.exists(file1)
        !Files.exists(file2)
        !Files.exists(file3)
        !Files.exists(subDir)
        !Files.exists(file4)
    }
}
