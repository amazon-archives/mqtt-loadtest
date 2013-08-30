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
    val c = new MqttClient(s"tcp://${config.host}:${config.port}", config.baseClientId + id)
    val opts = new MqttConnectOptions
    if (config.user.isDefined) opts.setUserName(config.user.get)
    if (config.password.isDefined) opts.setPassword(md5(config.password.get).toCharArray)
    c.connect(opts)
    c.setCallback(Reporter)
    c
  }

  private def md5(str: String) =
    MessageDigest.getInstance("MD5").digest(str.getBytes("utf8")).map("%02x" format _).mkString
}

case class Subscriber(id: Int) extends Client(id) {

  client.subscribe("io.m2m/loadtest/+/midwithdsn/<iterator>/65", 1)
}

case class Publisher(id: Int) extends Client(id) {
  import Config.config

  val sleepBetweenPublishes = config.publishRate
  val topic = client.getTopic(config.pubTopic(id))

  def run() {
    var iteration = 0
    while(true) {
      val payload = config.payload.get(id, iteration)
      topic.publish(payload, config.pubQos, config.pubRetain)
      Reporter.sentPublish()
      iteration += 1
      Thread.sleep(sleepBetweenPublishes)
    }
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

  def run() {
    println("Elapsed (ms),Sent (msgs/s),Published (msgs/s),Consumed (msgs/s)")
    while(true) {
      Thread.sleep(1000)

      val now = DateTime.now().millisOfDay().get()
      val sent = pubSent.get()
      val complete = pubComplete.get()
      val arrived = subArrived.get()

      val elapsedMs = now - lastTime
      val sentPs = sent - lastSent
      val completePs = complete - lastComplete
      val arrivedPs = arrived - lastArrived

      println(s"$elapsedMs,$sentPs,$completePs,$arrivedPs")

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

  for (i <- 1 to config.publishers) {
    val pub = Publisher(i)
    new Thread(new Runnable { def run() {pub.run()} }).start()
  }

  def launchSubscribers() {
    for (i <- 1 to config.subscribers) {
      Subscriber(i)
      Thread.sleep(config.connectRate)
    }
  }

  Reporter.run()
}
