A configurable benchmark tool for MQTT to test brokers.


Running
-------

1. Use SBT to compile and `sbt start-script` to generate a start script.

    sbt update compile start-script

2. Edit `src/main/resources/application.conf` to change the settings to what you need.
3. Run via either `sbt run` or `target/start.sh`

## License

Copyright 2015 2lemetry, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.