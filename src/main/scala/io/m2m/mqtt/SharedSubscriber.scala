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
  	Reporter.messageArrived(message.getTopic())
  }
}


class SharedSubscriber extends Actor {
  implicit val logger = LoggerFactory.getLogger(classOf[SharedSubscriber])

  val host = Config.config.subHost.getOrElse(Config.config.host)
  val url = s"tcp://$host:${Config.config.port}"
  val username = Config.config.user.get
  val pw = Client.md5(Config.config.password.get)
  val clientid = Config.config.subscriberClientId
  val subTopic = Config.config.subTopic
  val subQos = Config.config.subQos
  val clean = Config.config.subClean

  val client = new AsyncMqttClient(url, new XenqttCallback(self, SubscribingReporter()), 50)
  val subscriptions = List(new Subscription(subTopic, QoS.AT_LEAST_ONCE))

  def receive = {
    case Init => client.connect(clientid, clean, username, pw)
    case Sub => client.subscribe(subscriptions)
  }

}