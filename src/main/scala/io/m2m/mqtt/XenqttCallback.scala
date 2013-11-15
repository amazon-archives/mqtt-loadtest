package io.m2m.mqtt

import net.sf.xenqtt.client._
import net.sf.xenqtt.message._
import org.slf4j.LoggerFactory
import akka.actor.ActorRef


trait ClientReporter {
	def reportLostConnection
	def reportNewConnection
	def reportMessageArrived(message: PublishMessage)
}

case object Init
case object Sub

class XenqttCallback(ref: ActorRef, reporter: ClientReporter) extends AsyncClientListener {
	  implicit val logger = LoggerFactory.getLogger(classOf[XenqttCallback])


    override def publishReceived(client: MqttClient, message: PublishMessage) = {
      reporter.reportMessageArrived(message)
      message.ack
    }

    override def disconnected(client: MqttClient, cause: Throwable, reconnecting: Boolean) = {
    	reporter.reportLostConnection
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
        case _ => 
        	reporter.reportNewConnection
        	ref ! Sub
      }
    }

    override def published(client: MqttClient, message: PublishMessage) = {
    	Reporter.deliveryComplete(0L)
    }

    override def subscribed(client: MqttClient, requestedSubscriptions: Array[Subscription],
      grantedSubscriptions: Array[Subscription], requestsGranted: Boolean) = requestsGranted match {
      case false => logger.error("Unable to subscribe to the following subscriptions: " + requestedSubscriptions.deep.mkString("\n"))
      case true => logger.info("Granted subscriptions: " + grantedSubscriptions.deep.mkString("\n"))
    }

    override def unsubscribed(client: MqttClient, topics: Array[String]) = {

    }

  }