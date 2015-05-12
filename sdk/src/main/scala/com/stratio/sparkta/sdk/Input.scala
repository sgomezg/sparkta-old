/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.sparkta.sdk

import java.io.Serializable

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
 * Created by ajnavarro on 22/10/14.
 */
abstract class Input(properties: Map[String, Serializable]) extends Parameterizable(properties) {

  def setUp(ssc: StreamingContext): DStream[Event]
}

object Input {

  //TODO it´s ok?
  val RAW_DATA_KEY = "_attachment_body"
}
