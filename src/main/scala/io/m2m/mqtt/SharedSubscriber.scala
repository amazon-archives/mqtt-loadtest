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
import akka.actor.Actor
import org.slf4j.LoggerFactory
import net.sf.xenqtt.client._
import net.sf.xenqtt.message._


case class SubscribingReporter() extends ClientReporter {
	def reportLostConnection {
  	Reporter.lostSubscriber()
  }

  def reportNewConnection {
  	Reporter.addSubscriber()
  }

  def reportMessageArrived(message: PublishMessage) {
  	Reporter.messageArrived(message.getTopic, message.getPayload)
  }
}


class SharedSubscriber extends Actor {
  implicit val logger = LoggerFactory.getLogger(classOf[SharedSubscriber])

  val host = Config.config.subscribers.host.getOrElse(Config.config.host)
  val url = s"tcp://$host:${Config.config.port}"
  val username = Config.config.user.get
  val pw = Config.config.wirePassword.get
  val clientid = Config.config.subscribers.clientIdPrefix
  val subTopic = Config.config.subscribers.topic
  val subQos = Config.config.subscribers.qos
  val clean = Config.config.subscribers.clean

  val client = new AsyncMqttClient(url, new XenqttCallback(self, SubscribingReporter()), 50)
  val subscriptions = List(new Subscription(subTopic, QoS.AT_LEAST_ONCE))

  def receive = {
    case Init => client.connect(clientid, clean, username, pw)
    case Sub => client.subscribe(subscriptions)
  }

}
