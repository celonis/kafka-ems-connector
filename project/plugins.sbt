// Activate the following only when needed to use specific tasks like `whatDependsOn` etc...
//addDependencyTreePlugin

addSbtPlugin("org.scalameta"                     % "sbt-scalafmt"       % "2.3.1")
addSbtPlugin("com.typesafe.sbt"                  % "sbt-git"            % "1.0.0")
addSbtPlugin("io.spray"                          % "sbt-revolver"       % "0.9.1")
addSbtPlugin("org.scoverage"                     % "sbt-scoverage"      % "1.9.3")
addSbtPlugin("de.heikoseeberger"                 % "sbt-header"         % "5.6.0")
addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings"   % "3.0.2")
addSbtPlugin("ch.epfl.scala"                     % "sbt-scalafix"       % "0.9.26")
addSbtPlugin("ch.epfl.scala"                     % "sbt-bloop"          % "1.4.8")
addSbtPlugin("com.eed3si9n"                      % "sbt-buildinfo"      % "0.10.0")
addSbtPlugin("com.typesafe.sbt"                  % "sbt-license-report" % "1.2.0")

addSbtPlugin("net.vonbuchholtz" % "sbt-dependency-check" % "3.1.1")
addDependencyTreePlugin
libraryDependencies += "org.slf4j" % "slf4j-nop" % "1.7.25"
