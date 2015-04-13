/**
 * Copyright (C) 2015 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.sparkta.driver.factory

import java.io.File
import scala.collection.JavaConversions._
import scala.collection.mutable.Map

import akka.event.slf4j.SLF4JLogging
import com.typesafe.config.Config
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ajnavarro
 */
object SparkContextFactory extends SLF4JLogging {

  val sscMap: Map[String, StreamingContext] = Map()
  private var sc: Option[SparkContext] = None
  private var sqlContext: Option[SQLContext] = None

  def sparkSqlContextInstance: Option[SQLContext] = {
    synchronized {
      sqlContext match {
        case Some(_) => sqlContext
        case None => if (sc.isDefined) sqlContext = Some(new SQLContext(sc.get))
      }
    }
    sqlContext
  }

  def sparkContextInstance(generalConfig: Config, jars: Seq[File]): SparkContext =
    synchronized {
      sc.getOrElse(instantiateContext(generalConfig, jars))
    }

  private def instantiateContext(generalConfig: Config, jars: Seq[File]): SparkContext = {
    sc = Some(new SparkContext(configToSparkConf(generalConfig)))
    jars.foreach(f => sc.get.addJar(f.getAbsolutePath))
    sc.get
  }

  private def configToSparkConf(generalConfig: Config): SparkConf = {
    val c = generalConfig.getConfig("spark")
    val properties = c.entrySet()
    val conf = new SparkConf()

    properties.foreach(e => conf.set(e.getKey, c.getString(e.getKey)))

    conf.setIfMissing("spark.streaming.concurrentJobs", "20")
    conf.setIfMissing("spark.sql.parquet.binaryAsString", "true")
    conf.setIfMissing("spark.cassandra.connection.host", "127.0.0.1")

    conf
  }

  def destroySparkStreamingContext(name: String): Unit = {
    synchronized {
      if (sscMap.contains(name)) {
        val ssc = sscMap(name)
        log.debug("Stopping streamingContext with name: " + ssc.sparkContext.appName)
        ssc.stop()
        ssc.awaitTermination()
        log.debug("Stopped streamingContext with name: " + ssc.sparkContext.appName)
        sscMap.remove(name)
      }
    }
  }

  def destroySparkContext: Unit = {
    synchronized {
      if (sc.isDefined) {
        log.debug("Stopping SparkContext with name: " + sc.get.appName)
        sc.get.stop()
        log.debug("Stopped SparkContext with name: " + sc.get.appName)
        sc = None
      }
    }
  }
}
