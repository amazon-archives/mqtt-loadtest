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

class XenPublisher(client: AsyncMqttClient) extends Actor {

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
  	client.publish(new PublishMessage(config.pubTopic(id), QoS.AT_LEAST_ONCE, payload))
  	Reporter.sentPublish()
  	iteration += 1
  }

  def reportLostConnection = {
  	Reporter.lostPublisher()
  }

  def reportNewConnection = {
  	Reporter.addPublisher()
  }

}