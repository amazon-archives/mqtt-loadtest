package io.m2m.mqtt

import dispatch._
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global
import scala.util.Try

object SplunkLogger {

	def send2Splunk(report: Report) = Config.splunkConf match {
		case Some(conf) => Try {
			val params = Map("index" -> conf.splunkProject, "sourcetype" -> "json_no_timestamp")
			val request = url(conf.splunkUrl).as_!(conf.splunkUser, conf.splunkPass) << report.json <<? params OK as.String
			Http(request)
		}
		case None =>
	}

}