# kafka-ems-connector
An Apache Kafka sink connector for Celonis EMS 


### Compile, Test and Package

The project uses sbt builds for scala 2.13.
Starting with Kafka 2.6 there is one build supporting 2.13

```shell
sbt compile
sbt test
sbt assembly
```

Last command builds the connector artifact as a single jar. The file can be found:

```shell
--connector
 ├ target
   ├scala-2.13
      └ kafka-ems-sink-assembly-1.1-SNAPSHOT.jar
```
