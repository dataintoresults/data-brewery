/*******************************************************************************
 *
 * Copyright (C) 2019 by Obsidian SAS : https://dataintoresults.com/
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package com.dataintoresults.etl.datastore.flat

import java.nio.file._

import org.scalatest.FunSuite
import org.scalatest.Assertions._
import org.scalatest.AppendedClues._

import com.dataintoresults.etl.impl.ColumnBasic
import com.dataintoresults.etl.util.EtlHelper
import com.dataintoresults.etl.impl.EtlImpl
import com.dataintoresults.etl.core.Etl
import com.dataintoresults.util.Using._

class FlatFileStoreHelperTest extends FunSuite {

  test("FlatFileStoreHelperTest tests") {
    val tempDir = Files.createTempDirectory("FlatFileStoreHelperTest_")
    val subDir1 = Files.createDirectories(tempDir.resolve("dir1/subdir1"))
    val subDir2 = Files.createDirectories(tempDir.resolve("dir1/subdir2"))
    val subDir3 = Files.createDirectories(tempDir.resolve("dir2/subdir3"))

    val csv1 = Files.createFile(subDir1.resolve("file121.csv"))
    val csv2 = Files.createFile(subDir1.resolve("file122.csv"))
    val csv3 = Files.createFile(subDir1.resolve("file3.csv"))

    // Non conforming file
    val csv4 = Files.createFile(subDir1.resolve("fi1le124.csv"))

    // File in subDir2 instead subDir1
    val csv5 = Files.createFile(subDir2.resolve("file121.csv"))

    // File in subDir3 instead subDir1
    val csv6 = Files.createFile(subDir3.resolve("file121.csv"))


    assertResult(Seq(tempDir).mkString(",")) {
      FlatFileStoreHelper.listFiles("", tempDir.toString).mkString(",")
    } withClue "Identity when pattern is a path and basePath empty"

    assertResult(Seq(tempDir).mkString(",")) {
      FlatFileStoreHelper.listFiles(tempDir.toString,"").mkString(",")
    } withClue "Identity when basePath is a path and pattern empty"

    assertResult(Seq(subDir1).mkString(",")) {
      FlatFileStoreHelper.listFiles(tempDir.toString,"/dir1/subdir1").mkString(",")
    } withClue "Existing file with basePath and non regex pattern"

    assertResult(Seq(csv1, csv2, csv3).mkString(",")) {
      FlatFileStoreHelper.listFiles(tempDir.toString,"/dir1/subdir1/file*.csv").mkString(",")
    } withClue "Existing file with basePath and regex pattern on the last level"

    assertResult(Seq(csv1, csv5).mkString(",")) {
      FlatFileStoreHelper.listFiles(tempDir.toString,"/dir1/*/file121.csv").mkString(",")
    } withClue "Existing file with basePath and regex pattern on a middle level"

    assertResult(Seq(csv1, csv2, csv3, csv5, csv6).mkString(",")) {
      FlatFileStoreHelper.listFiles(tempDir.toString,"/*/*/file*.csv").mkString(",")
    } withClue "Existing file with basePath and regex pattern on the many level"



  }
  
}