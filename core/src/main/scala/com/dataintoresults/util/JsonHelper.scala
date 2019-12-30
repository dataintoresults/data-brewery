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

package com.dataintoresults.util

object JsonHelper {  
  def escape(string: String): String = {
    if (string == null || string.length() == 0) {
        "";
    }
    else {
      val len = string.length();
      val sb = new StringBuilder(len + 4);

      for (i <- 0 until len) {
        val c = string.charAt(i)
        c match  {
          case '\\' => 
              sb.append('\\')
              sb.append(c)
          case '"' => 
            sb.append('\\')
            sb.append(c)
          case '/' =>
            sb.append('\\')
            sb.append(c)
          case '\b' => sb.append("\\b")
          case '\t' => sb.append("\\t")
          case '\n' => sb.append("\\n")
          case '\f' => sb.append("\\f")
          case '\r' => sb.append("\\r");
          case _ if c < ' ' => {
            val t = "000" + Integer.toHexString(c);
            sb.append("\\u" + t.substring(t.length() - 4));
          }
          case _ if c > 0xC0 => {
            val t = "000" + Integer.toHexString(c);
            sb.append("\\u" + t.substring(t.length() - 4));
          }
          case _ => sb.append(c)
        }
      }

      sb.toString();
    }
  }
}