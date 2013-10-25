package io.m2m.mqtt

import scala.collection.JavaConversions._
import akka.actor.Actor
import org.slf4j.LoggerFactory
import net.sf.xenqtt.client._
import net.sf.xenqtt.message._


case object MsgPerSec
case object Init

class SharedSubscriber extends Actor {
  implicit val logger = LoggerFactory.getLogger(classOf[SharedSubscriber])

  val url = s"tcp://${Config.config.host}:${Config.config.port}"
  val username = Config.config.user.get
  val pw = Client.md5(Config.config.password.get)
  val clientid = Config.config.subscriberClientId
  val subTopic = Config.config.subTopic
  val subQos = Config.config.subQos
  val clean = Config.config.subClean

  def receive = {
    case Init => init
  }

  def init = {

    val client = new AsyncMqttClient("tcp://pnet.m2m.io:1883", new AsyncSubscriber(), 50)

    client.connect(clientid, clean, username, pw)

    val subscriptions = List(new Subscription(subTopic, QoS.AT_LEAST_ONCE))
    client.subscribe(subscriptions)
  }

  class AsyncSubscriber extends AsyncClientListener {

    override def publishReceived(client: MqttClient, message: PublishMessage) = {
      Reporter.messageArrived(message.getTopic)
      message.ack
    }

    override def disconnected(client: MqttClient, cause: Throwable, reconnecting: Boolean) = {
    	Reporter.lostSubscriber()
      Option(cause) match {
        case Some(ex) => logger.error("Disconnected Exception", ex)
        case None => logger.info("Got Disconneted Unknown")
      }

      reconnecting match {
        case true => logger.info("Attempting to reconnect")
        case false =>
      }
    }

    override def connected(client: MqttClient, returnCode: ConnectReturnCode) = {
      returnCode match {
        case rc if rc != ConnectReturnCode.ACCEPTED => println("Unable to connect to the broker. Reason: " + rc)
        case _ => Reporter.addSubscriber()
      }
    }

    override def published(client: MqttClient, message: PublishMessage) = {

    }

    override def subscribed(client: MqttClient, requestedSubscriptions: Array[Subscription],
      grantedSubscriptions: Array[Subscription], requestsGranted: Boolean) = requestsGranted match {
      case false => logger.error("Unable to subscribe to the following subscriptions: " + requestedSubscriptions.deep.mkString("\n"))
      case true => logger.info("Granted subscriptions: " + grantedSubscriptions.deep.mkString("\n"))
    }

    override def unsubscribed(client: MqttClient, topics: Array[String]) = {

    }

  }
}