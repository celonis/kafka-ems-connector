import sbt._
import Settings._

scalafixDependencies in ThisBuild ++= Dependencies.scalafixDeps
// This line ensures that sources are downloaded for dependencies, when using Bloop
bloopExportJarClassifiers in Global := Some(Set("sources"))

lazy val root = project.root
  .settings(
    settings ++
      Seq(
        name := "kafka-ems-sink",
        description := "Provides a Kafka Connect sink for Celonis EMS",
        libraryDependencies ++= emsSinkDeps,
        publish / skip := true,
        packDir := s"pack_${CrossVersion.binaryScalaVersion(scalaVersion.value)}",
        packGenerateMakefile := false
        //packMain := Map("ems-sink" -> "com.celonis.kafka.connect.Main")
      )
  )
  .configureTestsForProject(itTestsParallel = false)
  .enablePlugins(PackPlugin)

addCommandAlias(
  "validateAll",
  ";headerCheck;test:headerCheck;fun:headerCheck;it:headerCheck;scalafmtCheck;test:scalafmtCheck;it:scalafmtCheck;fun:scalafmtCheck;e2e:scalafmtCheck"
)
addCommandAlias(
  "formatAll",
  ";headerCreate;test:headerCreate;fun:headerCreate;it:headerCreate;scalafmt;test:scalafmt;it:scalafmt;fun:scalafmt;e2e:scalafmt"
)
addCommandAlias("fullTest", ";test;fun:test;it:test;e2e:test")
addCommandAlias("fullCoverageTest", ";coverage;test;fun:test;it:test;e2e:test;coverageReport;coverageAggregate")

dependencyCheckFormats := Seq("XML", "HTML")
dependencyCheckNodeAnalyzerEnabled := Some(false)
dependencyCheckNodeAuditAnalyzerEnabled := Some(false)
dependencyCheckNPMCPEAnalyzerEnabled := Some(false)
dependencyCheckRetireJSAnalyzerEnabled := Some(false)
