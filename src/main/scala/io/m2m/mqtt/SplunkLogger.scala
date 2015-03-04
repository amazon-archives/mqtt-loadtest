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

import dispatch._
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global
import scala.util.Try

object SplunkLogger {

	def report2Splunk(report: JsonSerialiazable) = Config.splunkConf match {
		case Some(conf) => Try {
			val params = Map("index" -> conf.splunkProject, "sourcetype" -> "json_no_timestamp")
			val request = url(conf.splunkUrl).as_!(conf.splunkUser, conf.splunkPass) << report.json <<? params OK as.String
			Http(request)
		}
		case None =>
	}

}
