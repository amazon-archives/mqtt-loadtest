package io.m2m.mqtt

import org.joda.time.DateTime
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicInteger
import akka.actor._
import scala.concurrent.duration._
import org.fusesource.mqtt.client._
import scala.concurrent.ExecutionContext
import org.fusesource.hawtbuf.{Buffer, UTF8Buffer}
import org.fusesource.mqtt.codec.MQTTFrame
import scala.util.Try

object Client {
  import Config.config

  private def getClient(prefix: String, id: Int) = {
    val mqtt = new MQTT()
    mqtt.setTracer(new Tracer {
      override def onReceive(frame: MQTTFrame) {
      }
    })
    mqtt.setHost(config.host, config.port)
    mqtt.setClientId(prefix + id)
    mqtt.setCleanSession(true)
    if (config.user.isDefined) mqtt.setUserName(config.user.get)
    if (config.password.isDefined) mqtt.setPassword(md5(config.password.get))
    mqtt.setKeepAlive(30)
    mqtt.callbackConnection()
  }

  private def md5(str: String) =
    MessageDigest.getInstance("MD5").digest(str.getBytes("utf8")).map("%02x" format _).mkString

  def callback[T](success: T => Unit, failure: Throwable => Unit) = new Callback[T] {
    def onSuccess(item: T) = success(item)
    def onFailure(err: Throwable) = failure(err)
  }

  def qos(int: Int) = QoS.values().filter(q => q.ordinal() == int).head

  def subscribe(id: Int) = {
    val client = getClient(config.subscriberPrefix, id).listener(new Listener {
      def onPublish(topic: UTF8Buffer, body: Buffer, ack: Runnable) {
        Reporter.messageArrived(topic.toString, body.getData)
        ack.run()
      }

      def onConnected() {
        Reporter.addSubscriber()
      }

      def onFailure(value: Throwable) { value.printStackTrace() }

      def onDisconnected() {
        Reporter.lostSubscriber()
      }
    })

    client.connect(callback(_ => {
      client.subscribe(Array(new Topic(config.subTopic(id), qos(config.subQos))),
        callback(bytes => {}, _ => {}))
    }, _ => {}))
  }

  def createPublisher(id: Int, actor: ActorRef) = {
    val client = getClient(config.publisherPrefix, id)
    client.listener(new Listener {
      def onPublish(topic: UTF8Buffer, body: Buffer, ack: Runnable) {}

      def onConnected() {
        Reporter.addPublisher()
      }

      def onFailure(value: Throwable) { value.printStackTrace() }

      def onDisconnected() {
        Reporter.lostPublisher()
      }
    })
    client.connect(callback(_ => actor ! Start(client), _ => {}))
  }
}

case class Start(client: CallbackConnection)
case class Publish(client: CallbackConnection)

case class Publisher(id: Int) extends Actor {
  import Config.config
  import ExecutionContext.Implicits.global

  val sleepBetweenPublishes = config.publishRate
  val qos = Client.qos(config.pubQos)
  val publishCallback = Client.callback((_: Void) => Reporter.deliveryComplete(), _ => {println("delivery failed")})
  var iteration = 0

  def receive = {
    case Start(client) =>
      context.system.scheduler.schedule(0 millis, sleepBetweenPublishes millis) {
        Try(self ! Publish(client)).recover {
          case e: Throwable => e.printStackTrace()
        }
      }
    case Publish(client) =>
      val payload = config.payload.get(id, iteration)
      client.publish(config.pubTopic(id), payload, qos, config.pubRetain, publishCallback)
      Reporter.sentPublish()
      iteration += 1
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
  def deliveryComplete() = pubComplete.incrementAndGet()
  def messageArrived(topic: String, message: Array[Byte]) = subArrived.incrementAndGet()
  def connectionLost(error: Throwable) {error.printStackTrace()}

  var publishers = 0
  var subscribers = 0

  def addPublisher() {publishers += 1}
  def addSubscriber() {subscribers += 1}
  def lostPublisher() { publishers -= 1 }
  def lostSubscriber() { subscribers -= 1 }

  def doReport() {
    val now = DateTime.now().millisOfDay().get()
    val sent = pubSent.get()
    val complete = pubComplete.get()
    val arrived = subArrived.get()

    val elapsedMs = now - start
    val sentPs = sent - lastSent
    val publishedPs = complete - lastComplete
    val inFlight = sent - complete
    val consumedPs = arrived - lastArrived

    println(s"$elapsedMs,$sentPs,$publishedPs,$consumedPs,$inFlight,$publishers,$subscribers")

    lastTime = now
    lastSent = sent
    lastComplete = complete
    lastArrived = arrived
  }
}

object Report

class Reporter extends Actor {
  println("Elapsed (ms),Sent (msgs/s),Published (msgs/s),Consumed (msgs/s),In Flight,Num Publishers,Num Subscribers")

  def receive = {
    case Report => Reporter.doReport()
  }
}

object LoadTest extends App {

  //Don't cache DNS lookups
  java.security.Security.setProperty("networkaddress.cache.ttl" , "0")

  import Config.config
  import ExecutionContext.Implicits.global

  val system = ActorSystem("LoadTest")
  val reporter = system.actorOf(Props[Reporter], "reporter")
  system.scheduler.schedule(1 second, 1 second) {
    reporter ! Report
  }

  for (i <- 1 to config.publishers) {
    try {
      val publisher = system.actorOf(Props(classOf[Publisher], i).withDispatcher("publish-dispatcher"), s"publisher-$i")
      Client.createPublisher(i, publisher)
    } catch {
      case e: Throwable => e.printStackTrace()
    }
  }

  for (i <- 1 to config.subscribers) {
    try {
      Client.subscribe(i)
    } catch {
      case e: Throwable => e.printStackTrace()
    }
    Thread.sleep(config.connectRate)
  }
}
