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
    mqtt.setKeepAlive(config.keepAlive)
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
  val publishCallback = Client.callback((_: Void) => Reporter.deliveryComplete(), Reporter.messageErred)
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
  val errors = new AtomicInteger()

  var lastTime = start
  var lastSent = 0
  var lastComplete = 0
  var lastArrived = 0
  var lastErrors = 0

  def sentPublish() = pubSent.incrementAndGet()
  def deliveryComplete() = pubComplete.incrementAndGet()
  def messageArrived(topic: String, message: Array[Byte]) = subArrived.incrementAndGet()
  def connectionLost(error: Throwable) {error.printStackTrace()}
  def messageErred(error: Throwable) {errors.incrementAndGet()}

  var publishers = 0
  var subscribers = 0
  var lastReport: Option[Report] = None

  def addPublisher() {publishers += 1}
  def addSubscriber() {subscribers += 1}
  def lostPublisher() { publishers -= 1 }
  def lostSubscriber() { subscribers -= 1 }

  def getReport(): Report = {
    val now = DateTime.now().millisOfDay().get()
    val sent = pubSent.get()
    val complete = pubComplete.get()
    val arrived = subArrived.get()
    val currentErrors = errors.get()

    val report = Report(
      now - start,
      sent - lastSent,
      complete - lastComplete,
      sent - complete,
      arrived - lastArrived,
      currentErrors - lastErrors,
      publishers,
      subscribers
    )

    lastTime = now
    lastSent = sent
    lastComplete = complete
    lastArrived = arrived
    lastErrors = currentErrors

    report
  }

  def doReport() {
    lastReport = Some(getReport())
    println(lastReport.get.csv)
  }
}

case class Report(elapsedMs: Int, sentPs: Int, publishedPs: Int, consumedPs: Int, inFlight: Int, errorsPs: Int, publishers: Int, subscribers: Int) {
  import org.json4s.NoTypeHints
  import org.json4s.native.Serialization.{write, formats}

  implicit val fmts = formats(NoTypeHints)

  def csv = s"$elapsedMs,$sentPs,$publishedPs,$consumedPs,$inFlight,$errorsPs,$publishers,$subscribers"

  def json = write(this)
}

object Report

class Reporter extends Actor {
  println("Elapsed (ms),Sent (msgs/s),Published (msgs/s),Consumed (msgs/s),In Flight,Errors (msgs/s),Num Publishers,Num Subscribers")

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
  WebServer.start()

  val reporter = system.actorOf(Props[Reporter], "reporter")
  system.scheduler.schedule(1 second, 1 second) {
    reporter ! Report
  }

  for (i <- 1 to config.publishers) {
    try {
      val publisher = system.actorOf(Props(classOf[Publisher], i).withDispatcher("publishers.dispatcher"), s"publisher-$i")
      Client.createPublisher(i, publisher)
    } catch {
      case e: Throwable => e.printStackTrace()
    }
    Thread.sleep(config.connectRate)
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
