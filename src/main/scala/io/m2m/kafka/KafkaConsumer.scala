/*
 *  Copyright 2015 2lemetry, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package io.m2m.kafka

import akka.actor.{ActorSystem, Actor}
import io.m2m.mqtt.{Reporter, Init, Sub}
import java.util.Properties
import kafka.producer.ProducerConfig
import kafka.consumer.{Consumer, ConsumerConfig}
import java.util.concurrent.Executors
import akka.util.ByteString
import com.typesafe.config.Config
import scala.util.Try

object KafkaConsumer {
  case class KafkaConfig(enabled: Boolean, zkConnect: String, zkTimeout: Int, groupId: String,
                         topic: String, parallelism: Int)
  object KafkaConfig {
    def get(config: Config) = {
      val cfg = Try(KafkaConfig(
        Try(config.getBoolean("kafka.enabled")).getOrElse(false),
        config.getString("kafka.zkConnect"),
        config.getInt("kafka.zkTimeout"),
        config.getString("kafka.groupId"),
        config.getString("kafka.topic"),
        config.getInt("kafka.parallelism")
      ))
      cfg.recover {
        case ex: Throwable => println(ex.getMessage)
      }
      cfg.filter(_.enabled).toOption
    }
  }

  def start(config: KafkaConfig) {
    val client = {
      val props = new Properties
      props.put("zookeeper.connect", config.zkConnect)
      props.put("zookeeper.connectiontimeout.ms", config.zkTimeout.toString)
      props.put("group.id", config.groupId)
      val cfg = new ConsumerConfig(props)
      Consumer.create(cfg)
    }

    val streams = client.createMessageStreams(Map(config.topic -> config.parallelism)).apply(config.topic)
    val executor = Executors.newFixedThreadPool(config.parallelism)
    streams.zipWithIndex.foreach { case (stream, i) =>
      executor.submit(new Runnable {
        def run() {
          try {
            Reporter.addSubscriber()
            for (msg <- stream) {
              val key = ByteString(msg.key).utf8String
              // The assumption is that when we create a rule, we substitute all / for -. The / is a special
              // character in Kafka topics (filesystem character), so we have to use either - or _.
              val mqttTopic = msg.topic.replace('-', '/') + '/' +  key.replace('-', '/')
              Reporter.messageArrived(mqttTopic, msg.message)
            }
          } finally {
            Reporter.lostSubscriber()
          }
        }
      })
    }
  }
}
