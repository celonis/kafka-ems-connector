import Settings._
import sbt._

ThisBuild / scalafixDependencies ++= Dependencies.scalafixDeps
// This line ensures that sources are downloaded for dependencies, when using Bloop
bloopExportJarClassifiers in Global := Some(Set("sources"))

lazy val root = Project("kafka-ems-connector", file("."))
  .settings(rootSettings)
  .settings(
    name := "kafka-ems-connector",
  )
  .aggregate(
    connector,
    testcontainers,
  )
  .dependsOn(testcontainers)
  .dependsOn(connector)
  .configureE2ETests()

lazy val testcontainers = project.in(file("testcontainers"))
  .settings(
    modulesSettings ++
      Seq(
        name := "kafka-ems-testcontainers",
        description := "Provides a testing environment for EMS connector",
        libraryDependencies ++= testcontainersDeps,
        publish / skip := true,
      ),
  )

lazy val connector = project.in(file("connector"))
  .settings(
    modulesSettings ++
      Seq(
        name := "kafka-ems-sink",
        description := "Provides a Kafka Connect sink for Celonis EMS",
        libraryDependencies ++= emsSinkDeps,
        publish / skip := true,
        packDir := s"pack_${CrossVersion.binaryScalaVersion(scalaVersion.value)}",
        packGenerateMakefile := false,
        packExcludeJars := Seq("kafka-clients.*\\.jar", "kafka-clients.*\\.jar", "hadoop-yarn.*\\.jar"),
      ),
  )
  .configureTests()
  .enablePlugins(PackPlugin)

addCommandAlias(
  "validateAll",
  ";headerCheck;test:headerCheck;scalafmtCheckAll;scalafmtSbtCheck",
)

addCommandAlias(
  "formatAll",
  ";headerCreate;test:headerCreate;scalafmtAll;scalafmtSbt",
)

addCommandAlias("fullTest", ";test;fun:test;it:test;e2e:test")
addCommandAlias("fullCoverageTest", ";coverage;test;it:test;coverageReport;coverageAggregate")

dependencyCheckFormats := Seq("XML", "HTML")
dependencyCheckNodeAnalyzerEnabled := Some(false)
dependencyCheckNodeAuditAnalyzerEnabled := Some(false)
dependencyCheckNPMCPEAnalyzerEnabled := Some(false)
dependencyCheckRetireJSAnalyzerEnabled := Some(false)
