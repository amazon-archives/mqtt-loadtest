package io.m2m.mqtt

import org.eclipse.paho.client.mqttv3._
import scala.reflect.io.Streamable
import java.io.FileInputStream
import java.util.UUID
import org.joda.time.DateTime
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicInteger

abstract sealed class Client(id: Int) {
  import Config.config

  val client = {
    val c = new MqttClient(s"tcp://${config.host}:${config.port}", baseClientId + id)
    val opts = new MqttConnectOptions
    if (config.user.isDefined) opts.setUserName(config.user.get)
    if (config.password.isDefined) opts.setPassword(md5(config.password.get).toCharArray)
    c.connect(opts)
    c.setCallback(callback)
    Reporter.addSubscriber()
    c
  }

  protected def callback: MqttCallback
  def baseClientId: String

  private def md5(str: String) =
    MessageDigest.getInstance("MD5").digest(str.getBytes("utf8")).map("%02x" format _).mkString
}

case class Subscriber(id: Int) extends Client(id) {

  client.subscribe("io.m2m/loadtest/+/midwithdsn/<iterator>/65", 1)
  protected def callback: MqttCallback = SubHandler
  def baseClientId = Config.config.subBaseClientId
}

case class Publisher(id: Int) extends Client(id) {
  import Config.config

  val sleepBetweenPublishes = config.publishRate
  val topic = client.getTopic(config.pubTopic(id))

  def run() {
    Reporter.addPublisher()
    var iteration = 0
    while(true) {
      val payload = config.payload.get(id, iteration)
      topic.publish(payload, config.pubQos, config.pubRetain)
      Reporter.sentPublish()
      iteration += 1
      Thread.sleep(sleepBetweenPublishes)
    }
  }

  protected def callback: MqttCallback = PubHandler
  def baseClientId = Config.config.pubBaseClientId
}

abstract class LoadTestMqttCallback extends MqttCallback {
  def deliveryComplete(deliveryToken: MqttDeliveryToken) = Reporter.deliveryComplete(deliveryToken)
  def messageArrived(topic: MqttTopic, message: MqttMessage) = Reporter.messageArrived(topic, message)
}

object SubHandler extends LoadTestMqttCallback {
  def connectionLost(error: Throwable) {
    error.printStackTrace()
    Reporter.lostSubscriber()
  }
}

object PubHandler extends LoadTestMqttCallback {
  def connectionLost(error: Throwable) {
    error.printStackTrace()
    Reporter.lostPublisher()
  }
}

object Reporter extends MqttCallback {
  val start = DateTime.now().millisOfDay().get()
  val pubSent = new AtomicInteger()
  val pubComplete = new AtomicInteger()
  val subArrived = new AtomicInteger()

  var lastTime = start
  var lastSent = 0
  var lastComplete = 0
  var lastArrived = 0

  def sentPublish() = pubSent.incrementAndGet()
  def deliveryComplete(deliveryToken: MqttDeliveryToken) = pubComplete.incrementAndGet()
  def messageArrived(topic: MqttTopic, message: MqttMessage) = subArrived.incrementAndGet()
  def connectionLost(error: Throwable) {error.printStackTrace()}

  var publishers = 0
  var subscribers = 0

  def addPublisher() {publishers += 1}
  def addSubscriber() {subscribers += 1}
  def lostPublisher() {publishers -= 1}
  def lostSubscriber() {subscribers -= 1}

  def run() {
    println("Elapsed (ms),Sent (msgs/s),Published (msgs/s),Consumed (msgs/s),Num Publishers,Num Subscribers")

    while(true) {
      Thread.sleep(1000)

      val now = DateTime.now().millisOfDay().get()
      val sent = pubSent.get()
      val complete = pubComplete.get()
      val arrived = subArrived.get()

      val elapsedMs = now - start
      val sentPs = sent - lastSent
      val completePs = complete - lastComplete
      val arrivedPs = arrived - lastArrived

      println(s"$elapsedMs,$sentPs,$completePs,$arrivedPs,$publishers,$subscribers")

      lastTime = now
      lastSent = sent
      lastComplete = complete
      lastArrived = arrived
    }
  }
}

object LoadTest extends App {

  import Config.config

  new Thread(new Runnable { def run() {launchSubscribers()} }).start()

  new Thread(new Runnable {
    def run() {
      for (i <- 1 to config.publishers) {
        val pub = Publisher(i)
        new Thread(new Runnable { def run() {pub.run()} }).start()
      }
    }
  }).start()

  def launchSubscribers() {
    for (i <- 1 to config.subscribers) {
      Subscriber(i)
      Thread.sleep(config.connectRate)
    }
  }

  Reporter.run()
}
