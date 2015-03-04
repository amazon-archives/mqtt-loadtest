/*
 *  Copyright 2015 2lemetry, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */



package io.m2m.mqtt

import scala.collection.JavaConversions._
import net.sf.xenqtt.client.PublishMessage
import akka.actor.Actor
import net.sf.xenqtt.message.QoS
import net.sf.xenqtt.client.AsyncMqttClient
import net.sf.xenqtt.client.Subscription
import org.json4s._
import org.json4s.NoTypeHints
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization.{ write, formats }

case class QueueMessage(ip_address: String, queue_id: String, queue_size: Int) extends JsonSerialiazable {

  implicit val fmts = formats(NoTypeHints)

  def json = write(this)
}

case class QueueReporter() extends ClientReporter {
  def reportLostConnection {

  }

  def reportNewConnection {

  }

  def reportMessageArrived(message: PublishMessage) {
  	implicit val formats = DefaultFormats

    val json = parse(message.getPayloadString())
    val report = json.extract[QueueMessage]
    SplunkLogger.report2Splunk(report)
  }
}

class QueueSubscriber extends Actor {
  val url = s"tcp://${Config.config.host}:${Config.config.port}"
  val username = Config.config.user.get
  val pw = Config.config.wirePassword.get
  val clientid = Config.config.queueClientid
  val subTopic = Config.config.queueTopic
  val subQos = QoS.AT_MOST_ONCE
  val clean = true

  val client = new AsyncMqttClient(url, new XenqttCallback(self, QueueReporter()), 10)
  val subscriptions = List(new Subscription(subTopic, subQos))

  def receive = {
    case Init => client.connect(clientid, clean, username, pw)
    case Sub => client.subscribe(subscriptions)
  }
}
