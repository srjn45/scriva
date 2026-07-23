// ScalaPB code generation wired through sbt-protoc. Generates idiomatic Scala
// message classes + gRPC stubs from the shared proto/scriva.proto at compile time.
addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.7")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.17"

// Signs + publishes to the Sonatype Central Portal, mirroring the Java/Kotlin
// clients' Maven Central setup.
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.2.1")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.11.3")
