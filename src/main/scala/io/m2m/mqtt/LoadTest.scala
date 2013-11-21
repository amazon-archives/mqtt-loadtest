package io.m2m.mqtt

import org.joda.time.DateTime
import java.security.MessageDigest
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import akka.actor._
import scala.concurrent.duration._
import org.fusesource.mqtt.client._
import scala.concurrent.ExecutionContext
import org.fusesource.hawtbuf.{Buffer, UTF8Buffer}
import org.fusesource.mqtt.codec.MQTTFrame
import scala.util.Try

object Client {
  import Config.config

  private def getClient(prefix: String, id: Int, clean: Boolean) = {
    val mqtt = new MQTT()
    mqtt.setTracer(new Tracer {
      override def onReceive(frame: MQTTFrame) {
      }
    })
    mqtt.setHost(config.host, config.port)
    mqtt.setClientId(prefix + id)
    mqtt.setCleanSession(clean)
    if (config.user.isDefined) mqtt.setUserName(config.user.get)
    if (config.password.isDefined) mqtt.setPassword(config.wirePassword.get)
    mqtt.setKeepAlive(30)
    mqtt.callbackConnection()
  }

  def md5(str: String) =
    MessageDigest.getInstance("MD5").digest(str.getBytes("utf8")).map("%02x" format _).mkString

  def callback[T](success: T => Unit, failure: Throwable => Unit) = new Callback[T] {
    def onSuccess(item: T) = success(item)
    def onFailure(err: Throwable) = failure(err)
  }

  def qos(int: Int) = QoS.values().filter(q => q.ordinal() == int).head

  def subscribe(id: Int) = {
    val clean = config.subscribers.clean
    val client = getClient(config.subscribers.clientIdPrefix, id, clean).listener(new Listener {
      def onPublish(topic: UTF8Buffer, body: Buffer, ack: Runnable) {
        //println("Got data on "+ topic.toString)
        Reporter.messageArrived(topic.toString)
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
      client.subscribe(Array(new Topic(config.subTopic(id), qos(config.subscribers.qos))),
        callback(bytes => {}, _ => {}))
      LoadTest.subController ! Register(client)
    }, _ => {}))
  }

  def createPublisher(id: Int, actor: ActorRef) = {
    val client = getClient(config.publishers.idPrefix, id, config.publishers.cleanSession)
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


class SubscriberController extends Actor {
  import Config.config
  import ExecutionContext.Implicits.global
  def receive = {
    case Register(client) =>
      config.subscribers.timeSpan.foreach { case timeStamp =>
        context.system.scheduler.scheduleOnce(timeStamp seconds, self, Stop(client))
      }

    case Stop(client) =>
      client.disconnect(null)
  }
}

case class Register(client: CallbackConnection)
case class Start(client: CallbackConnection)
case class Publish(client: CallbackConnection)
case class Stop(client: CallbackConnection)

case class Publisher(id: Int) extends Actor {
  import Config.config
  import ExecutionContext.Implicits.global

  val sleepBetweenPublishes = config.publishers.rate
  val qos = Client.qos(config.publishers.qos)
  def publishCallback = {
    val startNanos = System.nanoTime()
    Client.callback((_: Void) => Reporter.deliveryComplete(System.nanoTime() - startNanos), Reporter.messageErred)
  }
  var iteration = 0

  def receive = {
    case Start(client) =>
      context.system.scheduler.schedule(0 millis, sleepBetweenPublishes millis) {
        Try(self ! Publish(client)).recover {
          case e: Throwable => e.printStackTrace()
        }
      }
      config.publishers.timeSpan.foreach(ts => {
        context.system.scheduler.scheduleOnce(ts seconds, self, Stop(client))
      })

    case Publish(client) =>
      val payload = config.publishers.payload.get(id, iteration)
      client.publish(config.pubTopic(id), payload, qos, config.publishers.retain, publishCallback)
      Reporter.sentPublish()
      iteration += 1
    case Stop(client) =>
      client.disconnect(null)
      self ! PoisonPill
  }
}

object Reporter {
  val start = DateTime.now().millisOfDay().get()
  val pubSent = new AtomicInteger()
  val pubComplete = new AtomicInteger()
  val pubAckTime = new AtomicLong()
  val subArrived = new AtomicInteger()
  val errors = new AtomicInteger()

  var lastTime = start
  var lastSent = 0
  var lastComplete = 0
  var lastAckTime = 0L
  var lastArrived = 0
  var lastErrors = 0

  def sentPublish() = pubSent.incrementAndGet()
  def deliveryComplete(elapsedNanos: Long) = {
    pubAckTime.addAndGet(elapsedNanos)
    pubComplete.incrementAndGet()
  }
  def messageArrived(topic: String) = subArrived.incrementAndGet()
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
    val ackTime = pubAckTime.get()
    val arrived = subArrived.get()
    val currentErrors = errors.get()

    val report = Report(
      now - start,
      sent - lastSent,
      complete - lastComplete,
      arrived - lastArrived,
      sent - complete,
      ((ackTime - lastAckTime).toDouble / (arrived - lastArrived)) / 1000000,
      currentErrors - lastErrors,
      publishers,
      subscribers
    )

    lastTime = now
    lastSent = sent
    lastComplete = complete
    lastAckTime = ackTime
    lastArrived = arrived
    lastErrors = currentErrors

    report
  }

  def doReport() {
    lastReport = Some(getReport())
    if(lastReport.get.inFlight > 200000) {
      println("Inflight message count over 200K, something is wrong killing test")
      System.exit(1)
    }
    SplunkLogger.report2Splunk(lastReport.get)
    println(lastReport.get.csv)
  }
}

abstract class JsonSerialiazable {
  def json: String
}

case class Report(elapsedMs: Int, sentPs: Int, publishedPs: Int, consumedPs: Int, inFlight: Int, 
  avgAckMillis: Double, errorsPs: Int, publishers: Int, subscribers: Int) extends JsonSerialiazable {
  import org.json4s.NoTypeHints
  import org.json4s.native.Serialization.{write, formats}

  implicit val fmts = formats(NoTypeHints)

  def csv = f"$elapsedMs,$sentPs,$publishedPs,$consumedPs,$inFlight,$avgAckMillis%.3f,$errorsPs,$publishers,$subscribers"

  def json = write(this)
}

object Report

class Reporter extends Actor {
  println("Elapsed (ms),Sent (msgs/s),Published (msgs/s),Consumed (msgs/s),In Flight,Avg Ack (ms),Errors (msgs/s),Num Publishers,Num Subscribers")

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
  val subController = system.actorOf(Props[SubscriberController])

  val reporter = system.actorOf(Props[Reporter], "reporter")
  system.scheduler.schedule(1 second, 1 second) {
    reporter ! Report
  }

  //val queueReporting = system.actorOf(Props[QueueSubscriber].withDispatcher("subscribers.dispatcher"), s"queue-subscriber")
  //queueReporting ! Init

  for (i <- 1 to config.subscribers.count) {
    try {
      config.subscribers.shared match {
        case true => 
          val subscriber = system.actorOf(Props[SharedSubscriber].withDispatcher("subscribers.dispatcher"), s"subscriber-$i")
          subscriber ! Init
        case false => Client.subscribe(i)
      }  
    } catch {
      case e: Throwable => e.printStackTrace()
    }
    Thread.sleep(config.connectRate)
  }

  for (i <- 1 to config.publishers.count) {
    try {
      val publisher = system.actorOf(Props(classOf[Publisher], i).withDispatcher("publishers.dispatcher"), s"publisher-$i")
      Client.createPublisher(i, publisher)
    } catch {
      case e: Throwable => e.printStackTrace()
    }
    Thread.sleep(config.connectRate)
  }

}
