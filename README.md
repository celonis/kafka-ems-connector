# kafka-ems-connector
An Apache Kafka sink connector for Celonis EMS 


### Compile, Test and Package

The project uses sbt and cross builds for scala 2.12 and scala 2.13.
Starting with Kafka 2.6 there are two builds:one supporting scala 2.12 and one supporting scala 2.13

Using the `+` sign ensures it covers both scala 2.12 and 2.13

```shell
sbt +compile
sbt +test
sbt +pack
```

Last command builds the connector artefacts. The files can be found:

```shell
--target
 ├ pack_2.12
 │  └ lib
 ├ pack_2.13
 │  └ lib
```