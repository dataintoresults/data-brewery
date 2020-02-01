package com.dataintoresults.etl.core

import java.io.StringWriter
import java.time.LocalDate

import org.scalatest.FunSuite
import org.scalatest.Assertions._


class DataSetTest extends FunSuite {
  test("DataSet from given Data (1 column version)") {
    val data = DataSet.from(Seq("a"), Seq(1, 2))
    assertResult("a", "Keep column name")(data.columns(0).name) 
    assertResult("int", "Find int type")(data.columns(0).colType) 
    assertResult(1, "Cell 1 check")(data.rows(0)(0)) 
    assertResult(2, "Cell 2 check")(data.rows(1)(0)) 
  }
  
  test("DataSet from given Data (2 column version)") {
    val now = LocalDate.now
    val data = DataSet.from(Seq("a", "b"), Seq("aa", "bb"), Seq(now, null))
    assertResult("a", "Keep column 1 name")(data.columns(0).name) 
    assertResult("b", "Keep column 2 name")(data.columns(1).name) 
    assertResult("text", "Find string type")(data.columns(0).colType) 
    assertResult("date", "Find date type")(data.columns(1).colType) 
    assertResult("bb", "Cell (1,2) check (string)")(data.rows(1)(0))
    assertResult(now, "Cell (1,1) check (date")(data.rows(0)(1))
    assert(data.rows(1)(1) == null, "Cell 2, 2 check (null date)") 
  }
  
  test("DataSet from given Data (5 column version)") {
    val now = LocalDate.now
    
    val nullInt = null.asInstanceOf[scala.runtime.RichInt]
    val data = DataSet.from(Seq("a", "b", "c", "d", "e"), Seq("aa", "bb"), Seq(now, null), Seq[scala.runtime.RichInt](1, nullInt), Seq(5L, 6L), Seq(3.0, 6.0))
    assertResult("a", "Keep column 1 name")(data.columns(0).name) 
    assertResult("b", "Keep column 2 name")(data.columns(1).name) 
    assertResult("c", "Keep column 3 name")(data.columns(2).name) 
    assertResult("d", "Keep column 4 name")(data.columns(3).name) 
    assertResult("e", "Keep column 5 name")(data.columns(4).name) 
    assertResult("text", "Find string type")(data.columns(0).colType) 
    assertResult("date", "Find date type")(data.columns(1).colType) 
    assertResult("int", "Find int type")(data.columns(2).colType) 
    assertResult("bigint", "Find bigint type")(data.columns(3).colType) 
    assertResult("double", "Find double type")(data.columns(4).colType) 
    assertResult("bb", "Cell (1,2) check (string)")(data.rows(1)(0))
    assertResult(now, "Cell (1,1) check (date")(data.rows(0)(1))
    assert(data.rows(1)(1) == null, "Cell 2, 2 check (null date)") 
  }
}