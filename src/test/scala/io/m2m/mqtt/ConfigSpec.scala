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

import com.typesafe.config.{ConfigValue, ConfigFactory, Config => TSConfig}
import org.scalatest.{Inside, FunSpec}

class ConfigSpec extends FunSpec with Inside {

  describe("SampledMessages.fromRaw") {
    it("processes multiple messages") {
      val m = GeneratedMessage(45)
      val result = SampledMessages.fromRaw(List(Some(0.4) -> m, Some(0.6) -> m))

      assert(result.samples.toSet === Set(Sample(m, 0.0, 0.4), Sample(m, 0.4, 1.0)))
    }

    it("defaults None to equal portions") {
      val m = GeneratedMessage(45)
      val result = SampledMessages.fromRaw(List(None -> m, None -> m, None -> m, None -> m))

      assert(result.samples.toSet === Set(Sample(m, 0, 0.25), Sample(m, 0.25, 0.5), Sample(m, 0.5, 0.75), Sample(m, 0.75, 1.0)))
    }

    it("divides the remaining to equal portions") {
      val m = GeneratedMessage(45)
      val result = SampledMessages.fromRaw(List(None -> m, Some(0.25) -> m, None -> m, Some(0.25) -> m))

      assert(result.samples.toSet === Set(Sample(m, 0, 0.25), Sample(m, 0.25, 0.5), Sample(m, 0.5, 0.75), Sample(m, 0.75, 1.0)))
    }
  }

  describe("payload") {
    it("processes multiple messages correctly") {
      val cfg = ConfigFactory.parseString(
        """
          |samples = [
          |  {
          |    percent = 25
          |    file = publish.json
          |  },
          |  {
          |    size = 25
          |    percent = 25
          |  },
          |  {
          |    text = hello world
          |  }
          |]
        """.stripMargin)

      inside(Config.payload(cfg)) {
        case SampledMessages(s) =>
          val samples = s.toSet
          assert(samples ===
            Set(Sample(GeneratedMessage(25), 0.25, 0.5),
                Sample(FileMessage("publish.json"), 0.0, 0.25),
                Sample(Utf8Message("hello world"), 0.5, 1.0)
          ))
      }
    }
  }

}
