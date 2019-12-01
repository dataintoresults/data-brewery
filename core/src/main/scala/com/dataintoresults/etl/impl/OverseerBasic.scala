/*******************************************************************************
 *
 * Copyright (C) 2018 by Obsidian SAS : https://dataintoresults.com/
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

import com.dataintoresults.etl.core.Overseer
import com.dataintoresults.etl.core.DataSource
import com.dataintoresults.etl.core.DataSink
import com.dataintoresults.util.Using.using

class OverseerBasic extends Overseer {
  
  def runJob(extract: DataSource, load: DataSink) : Boolean = {    
    using(extract) { extract =>
      using(load) { load =>
        // Giving a change for the datasink to react to the incoming data structure
        load.setIncomingStruture(extract.structure)
        while(extract.hasNext)
          load.put(extract.next())          
      } 
    }
    
    true
  }
  
  
  def close() : Unit = {
  }
}