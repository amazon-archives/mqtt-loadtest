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



package io.m2m.mqtt

import akka.actor.{Props, Actor}
import java.util.UUID
import java.io._
import java.nio.ByteBuffer
import scala.util.{Failure, Success, Try}
import scala.annotation.tailrec
import java.util.zip.CRC32
import java.nio.channels.FileChannel

object Validation extends Validator {
  import LoadTest.system
  import Config.config

  case class Save(id: UUID, topic: String, payload: Array[Byte])

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run() {
      LoadTest.system.shutdown()
      pubStream.flush()
      pubStream.close()
      subStream.flush()
      subStream.close()
      println()
      println(s"Processing final report...")
      validate(config.publishers.validateFile, config.subscribers.validateFile)
    }
  })

  lazy val pubStream = new FileOutputStream(config.publishers.validateFile, false)
  lazy val publisher = system.actorOf(Props(classOf[Validation], pubStream))
  lazy val subStream = new FileOutputStream(config.subscribers.validateFile, false)
  lazy val subscriber = system.actorOf(Props(classOf[Validation], subStream))
}

class Validation(stream: FileOutputStream) extends Actor {
  import Validation._

  val chan = stream.getChannel

  def receive = {
    case Save(id, topic, payload) =>
      val topicBytes = topic.getBytes
      val length = 16 + 4 + topicBytes.size + 8
      val buffer = ByteBuffer.allocate(length)

      buffer.putLong(id.getMostSignificantBits)
      buffer.putLong(id.getLeastSignificantBits)
      buffer.putInt(topicBytes.length)
      buffer.put(topicBytes)
      buffer.putLong(crc(payload))

      buffer.flip()

      chan.write(buffer)
  }
}

trait Validator {
  def validate(publishFile: String, subscribeFile: String) = {
    val published = read(publishFile).toMap
    val received = read(subscribeFile)
    var receivedIds = Set[UUID]()
    var notSent = 0
    var notValid = 0
    var valid = 0

    for ((id, (topic, crc)) <- received) {
      published.get(id) match {
        case None => notSent += 1
        case Some((pubTopic, pubCrc)) =>
          if (topic != pubTopic || crc != pubCrc)
            notValid += 1
          else
            valid += 1
      }

      receivedIds += id
    }

    val notReceived = published.map(_._1).count(!receivedIds(_))

    println()
    println(s"======== REPORT ============")
    if (notSent > 0) {
      println(s"WARNING: Received but not sent: $notSent")
    }
    println(s"Corrupted: $notValid")
    println(s"Not Received: $notReceived")
    println(s"Valid: $valid")
  }

  def crc(buffer: Array[Byte]) = {
    val crc = new CRC32()
    crc.update(buffer)
    crc.getValue
  }

  def read(file: String) = {
    val stream = new DataInputStream(new FileInputStream(file))

    def readString(count: Int) = {
      val buffer = new Array[Byte](count)
      stream.read(buffer)
      new String(buffer)
    }

    @tailrec
    def loop(acc: List[(UUID, (String, Long))]): List[(UUID, (String, Long))] = Try {
      val id = {
        val msb = stream.readLong()
        val lsb = stream.readLong()
        new UUID(msb, lsb)
      }
      val topicLength = stream.readInt()
      val topic = readString(topicLength)
      val payload = stream.readLong()

      id -> (topic, payload)
    } match {
      case Success(pair) => loop(pair :: acc)
      case Failure(_) => acc
    }

    val map = loop(Nil)
    stream.close()
    map
  }
}

