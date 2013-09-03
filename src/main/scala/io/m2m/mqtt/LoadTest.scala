package io.m2m.mqtt

import org.eclipse.paho.client.mqttv3._
import org.joda.time.DateTime
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicInteger
import akka.actor._
import scala.concurrent.duration._

abstract sealed class Client(prefix: String, id: Int) {
  import Config.config

  val client = {
    val persistence = new MqttDefaultFilePersistence("/tmp")
    val c = new MqttClient(s"tcp://${config.host}:${config.port}", prefix + id, persistence)
    val opts = new MqttConnectOptions
    if (config.user.isDefined) opts.setUserName(config.user.get)
    if (config.password.isDefined) opts.setPassword(md5(config.password.get).toCharArray)
    opts.setConnectionTimeout(500)
    c.connect(opts)
    c.setCallback(callback)
    c
  }

  protected def callback: MqttCallback

  private def md5(str: String) =
    MessageDigest.getInstance("MD5").digest(str.getBytes("utf8")).map("%02x" format _).mkString
}

case class Subscriber(prefix: String, id: Int) extends Client(prefix, id) {
  import Config.config

  client.subscribe(config.subTopic(id), config.subQos)
  Reporter.addSubscriber()

  protected def callback: MqttCallback = SubHandler
}

case class Publisher(prefix: String, id: Int) extends Client(prefix, id) with Actor {
  import Config.config

  val sleepBetweenPublishes = config.publishRate
  val topic = client.getTopic(config.pubTopic(id))
  var iteration = 0

  Reporter.addPublisher()

  def receive = {
    case "publish" =>
      val payload = config.payload.get(id, iteration)
      topic.publish(payload, config.pubQos, config.pubRetain)
      Reporter.sentPublish()
      iteration += 1
  }

  protected def callback: MqttCallback = PubHandler
}

class PublisherFactory extends Actor {
  import Config.config
  import scala.concurrent.ExecutionContext.Implicits.global

  val prefix = config.publisherPrefix
  var iteration = 0
  context.system.scheduler.schedule(0 millis, config.connectRate millis) {
    iteration += 1
    if (iteration > config.publishers)
      self ! PoisonPill
    else
      self ! iteration
  }

  def receive = {
    case i: Int =>
      val publisher = context.system.actorOf(Props(classOf[Publisher], prefix, i).withDispatcher("publish-dispatcher"))
      context.system.scheduler.schedule(config.publishRate millis, config.publishRate millis) {
        publisher ! "publish"
      }
  }
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

object Reporter {
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
      val publishedPs = complete - lastComplete
      val consumedPs = arrived - lastArrived

      println(s"$elapsedMs,$sentPs,$publishedPs,$consumedPs,$publishers,$subscribers")

      lastTime = now
      lastSent = sent
      lastComplete = complete
      lastArrived = arrived
    }
  }
}

object LoadTest extends App {

  //Don't cache DNS lookups
  java.security.Security.setProperty("networkaddress.cache.ttl" , "0")

  import Config.config

  val system = ActorSystem("LoadTest")
  system.actorOf(Props[PublisherFactory])

  new Thread(new Runnable { def run() {launchSubscribers()} }).start()

  def launchSubscribers(start: Int = 0) {
    for (i <- (start + 1) to config.subscribers) {
      try {
        Subscriber(config.subscriberPrefix, i)
      } catch {
        case e: Throwable => e.printStackTrace()
      }
      Thread.sleep(config.connectRate)
    }
  }

  Reporter.run()
}
