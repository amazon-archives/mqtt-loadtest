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

import akka.actor.Actor
import scala.collection.JavaConversions._
import net.sf.xenqtt.client.AsyncClientListener
import net.sf.xenqtt.client.AsyncMqttClient
import net.sf.xenqtt.client.Subscription
import net.sf.xenqtt.message.QoS
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global
import net.sf.xenqtt.client.PublishMessage

case class Connect(id: Int)
case class Pub(id: Int)

class PubReporter() extends ClientReporter {

	def reportLostConnection = {
  	Reporter.lostPublisher()
  }

  def reportNewConnection = {
  	Reporter.addPublisher()
  }

  def reportMessageArrived(message: PublishMessage) {
    
  }
}

class XenPublisher(client: AsyncMqttClient) extends Actor with LatencyTimer {

	val config = Config.config
	val url = s"tcp://${Config.config.host}:${Config.config.port}"
  val username = config.user.get
  val pw = Config.config.wirePassword.get
  val clean = config.publishers.cleanSession
  val sleepBetweenPublishes = config.publishers.rate
  val pubPrefix = config.publishers.idPrefix
  
  var iteration = 0

  def receive = {
    case Connect(id) => init(id)
    case Pub(id) => publish(id) 
  }

  def init(id: Int) = {
    client.connect(pubPrefix + id, clean, username, pw)
    context.system.scheduler.schedule(1000 millis, sleepBetweenPublishes millis) {
  		self ! Pub(id)
  	}
  }

  def publish(id: Int) = {
  	val payload = Config.config.publishers.payload.get(id, iteration)
    val msgId = generateMessageId()
    val topic = config.pubTopic(id, msgId)
  	client.publish(new PublishMessage(topic, QoS.AT_LEAST_ONCE, payload))
    msgId match {
      case Some(uuid) => Reporter.sentPublish(uuid, topic, payload)
      case None => Reporter.sentPublish()
    }
  	iteration += 1
  }

  def reportLostConnection = {
  	Reporter.lostPublisher()
  }

  def reportNewConnection = {
  	Reporter.addPublisher()
  }

}
