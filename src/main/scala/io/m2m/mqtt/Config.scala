package io.m2m.mqtt

import com.typesafe.config.ConfigFactory
import scala.reflect.io.Streamable
import java.io.FileInputStream

object Config {
  private def getConfig: Config = {
    val conf = ConfigFactory.load()
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
      if (conf.hasPath("publishers.payload.path")) FileMessage(conf.getString("publishers.payload.path"))
      else Utf8Message(conf.getString("publishers.payload")),
      conf.getInt("publishers.qos"),
      conf.getInt("subscribers.qos"),
      conf.getBoolean("publishers.retain"),
      conf.getString("publishers.client-id-prefix"),
      conf.getString("subscribers.client-id-prefix")
    )
  }

  lazy val config = getConfig
}

case class Config(host: String, port: Int, user: Option[String], password: Option[String],
                  pubTopic: String, subTopic: String, publishers: Int, subscribers: Int, connectRate: Long,
                  publishRate: Long, payload: MessageSource, pubQos: Int, subQos: Int, pubRetain: Boolean,
                  publisherPrefix: String, subscriberPrefix: String) {


  private def templateTopic(topic: String, id: Int) = topic.replaceAll("\\$num", id.toString)

  def pubTopic(id: Int): String = templateTopic(pubTopic, id)
  def subTopic(id: Int): String = templateTopic(subTopic, id)
}

sealed abstract class MessageSource {
  def get(clientNum: Int, iteration: Int): Array[Byte]
}

case class Utf8Message(msg: String) extends MessageSource {
  def get(clientNum: Int, iteration: Int) = msg.getBytes("utf8")
}

case class FileMessage(file: String) extends MessageSource {
  def get(clientNum: Int, iteration: Int) = Streamable.bytes(new FileInputStream(file))
}

