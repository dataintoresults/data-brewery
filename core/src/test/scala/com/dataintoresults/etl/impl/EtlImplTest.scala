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

package com.dataintoresults.etl.impl

import java.io.StringReader
import java.time.{LocalDate, LocalDateTime}
import java.nio.file.Paths
import scala.math.BigDecimal

import org.scalatest.FunSuite
import org.scalatest.Assertions._


class EtlImplTest extends FunSuite {
  
  test("Etl need to preprocess") {
		
		val etl = new EtlImpl()
		etl.load(Paths.get("C:/Users/sderi/OneDrive/Projets/Obsidian/3 - Projets/008 - VodFactory/dwh/dw.xml"))
		println(etl.save())
    assertResult(etl.save().toString)("b")
	}
}