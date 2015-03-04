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

import org.mashupbots.socko.routes.{Path, HttpRequest, GET, Routes}
import akka.actor.{ActorLogging, Actor, Props}
import org.mashupbots.socko.events.HttpRequestEvent
import org.mashupbots.socko.webserver
import org.mashupbots.socko.webserver.WebServerConfig

object WebServer {
  import LoadTest.system

  val webServer = system.actorOf(Props[WebServer])

  val routes = Routes {
    case HttpRequest(request) => request match {
      case (GET(Path("/current"))) =>
        webServer ! Current(request)
    }
  }

  val akkaConfig = new WebServerConfig(system.settings.config, "http")
  val server = new webserver.WebServer(akkaConfig, routes, system)
  println(akkaConfig.hostname)

  def enabled =
    if (system.settings.config.hasPath("http.enabled"))
      system.settings.config.getBoolean("http.enabled")
    else
      true

  def start() {
    if (!enabled) return

    server.start()

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() { server.stop() }
    })
  }
}

case class Current(request: HttpRequestEvent)

class WebServer extends Actor with ActorLogging {
  def receive = {
    case Current(request) =>
      request.response.write(Reporter.lastReport.map(_.json).getOrElse("{}"))
  }
}
