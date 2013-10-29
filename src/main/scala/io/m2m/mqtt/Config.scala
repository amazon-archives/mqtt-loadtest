package io.m2m.mqtt

import com.typesafe.config.{ConfigValue, ConfigFactory, Config => TSConfig}
import scala.reflect.io.Streamable
import java.io.FileInputStream
import scala.util.{Random, Try}
import scala.collection.JavaConversions._

case class SplunkConfig(splunkUser: String, splunkPass: String, splunkUrl: String, splunkProject: String)

object Config {
  val configFactory = ConfigFactory.load()

  val splunkConf = configFactory.getBoolean("splunk.enabled") match {
    case true =>
      val splunkUser = configFactory.getString("splunk.user")
      val splunkPass = configFactory.getString("splunk.password")
      val splunkUrl = configFactory.getString("splunk.url")
      val splunkProject = configFactory.getString("splunk.project")
      Option(SplunkConfig(splunkUser, splunkPass, splunkUrl, splunkProject))
    case false => None
  }
  

  def payload(config: TSConfig): MessageSource = {
    def getFile(cfg: TSConfig) = FileMessage(cfg.getString("file"))
    def getUTF(cfg: TSConfig) = Utf8Message(cfg.getString("text"))
    def getGenerated(cfg: TSConfig) = GeneratedMessage(cfg.getInt("size"))
    def getSamples(cfg: TSConfig) = SampledMessages.fromRaw {
      cfg.getConfigList("samples")
        .map(x => Try(x.getDouble("percent")).toOption.map(_ / 100) -> payload(x))
        .toList
    }

    val fm = Try(getFile(config))
    val utf = fm.orElse(Try(getUTF(config)))
    val gen = utf.orElse(Try(getGenerated(config)))
    val samples = gen.orElse(Try(getSamples(config)))

    samples.getOrElse(GeneratedMessage(1024))
  }

  def getConfig(conf: TSConfig): Config = {
    Config(
      conf.getString("host"),
      conf.getInt("port"),
      if (conf.hasPath("username")) Some(conf.getString("username")) else None,
      if (conf.hasPath("password")) Some(conf.getString("password")) else None,
      conf.getString("publishers.topic"),
      conf.getString("subscribers.topic"),
      conf.getInt("publishers.count"),
      conf.getInt("subscribers.count"),
      conf.getMilliseconds("millis-between-connects"),
      conf.getMilliseconds("publishers.millis-between-publish"),
      payload(conf.getConfig("publishers.payload")),
      conf.getInt("publishers.qos"),
      conf.getInt("subscribers.qos"),
      conf.getBoolean("publishers.retain"),
      conf.getString("publishers.client-id-prefix"),
      conf.getString("subscribers.client-id-prefix"),
      conf.getInt("keep-alive").toShort,
      Try(conf.getBoolean("publishers.clean-session")).getOrElse(true),
      Try(conf.getBoolean("subscribers.clean-session")).getOrElse(true),
      conf.getBoolean("subscribers.shared"),
      conf.getString("queue-monitor.clientid"),
      conf.getString("queue-monitor.topic")
    )
  }
  
  lazy val config = getConfig(configFactory)
}

case class Config(host: String, port: Int, user: Option[String], password: Option[String],
                  pubTopic: String, subTopic: String, publishers: Int, subscribers: Int, connectRate: Long,
                  publishRate: Long, payload: MessageSource, pubQos: Int, subQos: Int, pubRetain: Boolean,
                  publisherPrefix: String, subscriberClientId: String, keepAlive: Short,
                  pubClean: Boolean, subClean: Boolean, subShared: Boolean, queueClientid: String, queueTopic: String) {


  private def templateTopic(topic: String, id: Int) = topic.replaceAll("\\$num", id.toString)

  def pubTopic(id: Int): String = templateTopic(pubTopic, id)
  def subTopic(id: Int): String = templateTopic(subTopic, id)
  def subscriberId(id: Int) = subscriberClientId + id
  def publisherId(id: Int) = publisherPrefix + id
}

sealed abstract class MessageSource {
  def get(clientNum: Int, iteration: Int): Array[Byte]
}

case class Utf8Message(msg: String) extends MessageSource {
  def get(clientNum: Int, iteration: Int) = msg.getBytes("utf8")
}

case class FileMessage(file: String) extends MessageSource {
  lazy val fileBytes = Streamable.bytes(new FileInputStream(file))
  def get(clientNum: Int, iteration: Int) = fileBytes
}

case class GeneratedMessage(size: Int) extends MessageSource {
  lazy val msg = {
    val bytes = new Array[Byte](size)
    Random.nextBytes(bytes)
    bytes
  }

  def get(clientNum: Int, iteration: Int) = msg
}

case class Sample(msg: MessageSource, lower: Double, upper: Double)
case class SampledMessages(samples: List[Sample]) extends MessageSource {
  def getMessage(n: Double) = samples
    .find(m => m.lower <= n && n < m.upper)
    .map(_.msg)
    .getOrElse(GeneratedMessage(1024))

  def get(clientNum: Int, iteration: Int) = {
    val n = Random.nextDouble()
    getMessage(n).get(clientNum, iteration)
  }
}

object SampledMessages {
  def fromRaw(messages: List[(Option[Double], MessageSource)]): SampledMessages = {
    val (base, explicitSamples) = messages.filter(_._1.isDefined).foldLeft(0.0 -> List[Sample]()) {
      case ((low, acc), (percent, msg)) =>
        val hi = low + percent.get
        hi -> (Sample(msg, low, hi) :: acc)
    }

    val remaining = messages.filter(!_._1.isDefined)
    val remainingPercent = 1 - base
    val size = remainingPercent / remaining.size
    
    val remainingSamples = remaining.map(_._2).zipWithIndex
      .map{ case (msg, i) => Sample(msg, base + (size*i), base + (size*i) + size)}

    SampledMessages(explicitSamples ++ remainingSamples)
  }
}

