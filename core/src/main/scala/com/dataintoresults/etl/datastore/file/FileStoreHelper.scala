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

package com.dataintoresults.etl.datastore.file

import java.nio.file.{Path, Paths, Files}
import scala.collection.JavaConverters._
import java.util.regex._

object FileStoreHelper {

	/**
	 * List file with a *basePath* and a set of *pattern* (pattern is a string where is pattern 
	 * is separated by *patternSeparator*).
	 */
	def listFiles(basePath: String, pattern: String, patternSeparator: String): Seq[Path] = {
		pattern.split(Pattern.quote(patternSeparator)).flatMap(p => listFiles(basePath, p))
	}
	
	def listFiles(basePath: String, pattern: String): Seq[Path] = {
		if(!pattern.contains("""*""")) {
			// No pattern, simply return the file
			Seq(Paths.get(basePath + pattern))
		}
		else {
			val patternLevels = pattern.replaceAll("""\\""", "/").split("/")
			val fixedPattern = patternLevels.takeWhile(!_.contains("*")).mkString("/")
			val levelsToMatch = patternLevels.dropWhile(!_.contains("*"))
			val basePattern = Paths.get(basePath + fixedPattern)

			var solution = Seq(basePattern) 

			levelsToMatch.foreach(level => {
				val levelPattern = level.replaceAll(Pattern.quote("."), "[.]").replaceAll(Pattern.quote("*"), ".*").r
				solution = solution.flatMap{ root => 
					Files.walk(root, 1).iterator().asScala
						.filter(p => 
							levelPattern.unapplySeq(p.getName(p.getNameCount()-1).toString).isDefined
						)
				}
			})

			solution.sortBy(_.toString)
			// level 1 Seq[Path] -> Seq[Path]
			// flatMap
		}
	}
}