package io.m2m.mqtt

import com.typesafe.config.{ConfigValue, ConfigFactory, Config => TSConfig}
import scala.reflect.io.Streamable
import java.io.FileInputStream
import scala.util.{Random, Try}
import scala.collection.JavaConversions._

object Config {
  private def getConfig: Config = {
    val conf = ConfigFactory.load()

    val payload: MessageSource with Product with Serializable = {
      def getFile(cfg: TSConfig) = FileMessage(cfg.getString("path"))
      def getUTF(cfg: TSConfig) = Utf8Message(cfg.getString("publishers.payload"))

      val fm = Try(getFile(conf.atPath("publishers.payload")))
      val utf = fm.orElse(Try(getUTF(conf)))
      val gen = utf.orElse(Try(GeneratedMessage(conf.getInt("publishers.payload.size"))))

      gen.getOrElse(GeneratedMessage(1024))
    }

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
      payload,
      conf.getInt("publishers.qos"),
      conf.getInt("subscribers.qos"),
      conf.getBoolean("publishers.retain"),
      conf.getString("publishers.client-id-prefix"),
      conf.getString("subscribers.client-id-prefix"),
      conf.getInt("keep-alive").toShort,
      Try(conf.getBoolean("publishers.clean-session")).getOrElse(true),
      Try(conf.getBoolean("subscribers.clean-session")).getOrElse(true)
    )
  }

  lazy val config = getConfig
}

case class Config(host: String, port: Int, user: Option[String], password: Option[String],
                  pubTopic: String, subTopic: String, publishers: Int, subscribers: Int, connectRate: Long,
                  publishRate: Long, payload: MessageSource, pubQos: Int, subQos: Int, pubRetain: Boolean,
                  publisherPrefix: String, subscriberPrefix: String, keepAlive: Short,
                  pubClean: Boolean, subClean: Boolean) {


  private def templateTopic(topic: String, id: Int) = topic.replaceAll("\\$num", id.toString)

  def pubTopic(id: Int): String = templateTopic(pubTopic, id)
  def subTopic(id: Int): String = templateTopic(subTopic, id)
  def subscriberId(id: Int) = subscriberPrefix + id
  def publisherId(id: Int) = publisherPrefix + id
}

sealed abstract class MessageSource {
  def get(clientNum: Int, iteration: Int): Array[Byte]
}

case class Utf8Message(msg: String) extends MessageSource {
  def get(clientNum: Int, iteration: Int) = msg.getBytes("utf8")
}

case class FileMessage(file: String) extends MessageSource {
  val fileBytes = Streamable.bytes(new FileInputStream(file))
  def get(clientNum: Int, iteration: Int) = fileBytes
}

case class GeneratedMessage(size: Int) extends MessageSource {
  def get(clientNum: Int, iteration: Int) = {
    val bytes = new Array[Byte](size)
    Random.nextBytes(bytes)
    bytes
  }
}

