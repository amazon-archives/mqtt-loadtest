A configurable benchmark tool for MQTT to test brokers.


Running
-------

1. Use SBT to compile and `sbt start-script` to generate a start script.

    sbt update compile start-script

2. Edit `src/main/resources/application.conf` to change the settings to what you need.
3. Run via either `sbt run` or `target/start.sh`
