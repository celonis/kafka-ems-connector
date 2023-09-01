// From https://github.com/sbt/sbt/issues/3618, to solve a problem where dumpLicenseReport
// was not able to download info for https://repo1.maven.org/maven2/javax/ws/rs/javax.ws.rs-api/2.1.1/javax.ws.rs-api-2.1.1.${packaging.type}

import sbt.*

object PackagingTypePlugin extends AutoPlugin {
  override val buildSettings = {
    sys.props += "packaging.type" -> "jar"
    Nil
  }
}
