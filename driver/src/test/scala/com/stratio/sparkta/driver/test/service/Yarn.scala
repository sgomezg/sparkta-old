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

package com.stratio.sparkta.driver.test.service

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

class Yarn extends FlatSpec with ShouldMatchers {

  trait WithConfig {

    System.setProperty("SPARK_YARN_MODE", "true")
    val conf = new SparkConf()
      .setMaster("yarn-client")
      .setAppName("local test")
  }

  "Yarn" should "create and reuse same context" in new WithConfig {
    val sc = new SparkContext("yarn-client","local-test-sparkta")
    val count = sc.parallelize(1 to 100).count
    count should be equals (100)
  }
}
