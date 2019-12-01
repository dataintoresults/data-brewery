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

import akka.stream._
import akka.stream.scaladsl._
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription

import akka.{ NotUsed, Done }
import akka.actor.ActorSystem
import scala.concurrent._
import scala.concurrent.duration._


import com.dataintoresults.etl.core.Overseer
import com.dataintoresults.etl.core.DataSource
import com.dataintoresults.etl.core.DataSink

/**
 * Does not work currently. Dead code.
 */
class OverseerAkkaStream(implicit private val system : ActorSystem = ActorSystem("QuickStart")) extends Overseer {
  implicit private val materializer = ActorMaterializer()
  implicit private val ec = system.dispatcher
  
  def runJob(extract: DataSource, load: DataSink) : Boolean = {
    val source  = Source.unfoldResource[Seq[Any], DataSource](
        () => {   
          //println("source.init")
          extract
        },
        res => {
          //println("source.next") 
          if(res.hasNext()) Some(res.next())
          else None
        },
        res => {
          //println("source.close")
          res.close()
        }
      )

    val jobEnd = Promise[Unit]()
        
    val sink = Sink.fromSubscriber(new Subscriber[Seq[Any]]{
      var startTimestamp = System.currentTimeMillis()
      var nbRow : Long = 0
      var subscription : Subscription = _
      def onComplete() = {
        //println("sink.onComplete")
        load.close()
        val delta = (System.currentTimeMillis - startTimestamp)/1000.0
        val speed = nbRow / delta
        println(s"NbRows = ${nbRow},  Speed = ${speed.toInt}")
        jobEnd success Unit
      }
      def onError(t: Throwable) = {
        //println("sink.onError")
        load.close()
        jobEnd failure t
      }
      def onNext(row : Seq[Any]) = {
        subscription.request(1)
        //println("sink.onNext")
        load.put(row)
        nbRow += 1
      }
      def onSubscribe(subscription: Subscription) = {
        this.subscription = subscription
        //println("sink.onSubscribe")
        subscription.request(1)
      }
    })
    
    
    val g: RunnableGraph[_] = RunnableGraph.fromGraph(GraphDSL.create() {
      implicit builder =>

      // Source
      val I: Outlet[Seq[Any]] = builder.add(source).out
      val O: Inlet[Seq[Any]] = builder.add(sink).in

      import GraphDSL.Implicits._ // allows us to build our graph using ~> combinators

      // Graph
      I ~> O

      ClosedShape // defines this as a "closed" graph, not exposing any inlets or outlets
    })
    
    
    g.run()
    
    Await.result(jobEnd.future, Duration.Inf)
    
    true
  }
  
  
  def close() : Unit = {
    system.terminate()
  }
}



