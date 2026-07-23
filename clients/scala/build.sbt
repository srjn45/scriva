// ScrivaDB — Scala client (sbt + ScalaPB)
//
// Generates idiomatic Scala message classes and gRPC stubs from the single
// source of truth, proto/scriva.proto, at compile time via ScalaPB.
// Coordinates: io.github.srjn45:scriva-client-scala_2.13:1.2.1

ThisBuild / organization := "io.github.srjn45"
ThisBuild / version := "1.2.1"
ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "scriva-client-scala",

    libraryDependencies ++= Seq(
      // ScalaPB runtime (well-known protos included on the import path via the
      // `protobuf` scope) + gRPC bindings.
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
      "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
      "io.grpc" % "grpc-netty-shaded" % scalapb.compiler.Version.grpcJavaVersion,
      // Supplies google/api/annotations.proto for import resolution (protobuf
      // scope) and its pre-generated Scala classes at runtime.
      "com.thesamet.scalapb.common-protos" %% "proto-google-common-protos-scalapb_0.11" % "2.9.6-0" % "protobuf",
      "com.thesamet.scalapb.common-protos" %% "proto-google-common-protos-scalapb_0.11" % "2.9.6-0",
      // Test: hermetic in-process gRPC server.
      "org.scalatest" %% "scalatest" % "3.2.18" % Test,
      "io.grpc" % "grpc-inprocess" % scalapb.compiler.Version.grpcJavaVersion % Test,
    ),

    // Generate Scala + gRPC stubs from the shared proto directory.
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = true) -> (Compile / sourceManaged).value / "scalapb"
    ),
    Compile / PB.protoSources += baseDirectory.value / ".." / ".." / "proto",

    // -----------------------------------------------------------------------
    // Maven Central (Sonatype Central Portal) publishing — namespace io.github.srjn45
    // -----------------------------------------------------------------------
    // Validate locally (signs into the local repo):  sbt publishLocalSigned
    // Real publish (workflow):                        sbt publishSigned sonatypeBundleRelease
    // Credentials/GPG are injected by publish-clients.yml; nothing is published here.
    publishMavenStyle := true,
    licenses := Seq("MIT" -> url("https://github.com/srjn45/scriva/blob/main/clients/scala/LICENSE")),
    homepage := Some(url("https://github.com/srjn45/scriva")),
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/srjn45/scriva"),
        "scm:git:https://github.com/srjn45/scriva.git",
        "scm:git:ssh://git@github.com/srjn45/scriva.git",
      )
    ),
    developers := List(
      Developer("srjn45", "srjn45", "srajanpathak45@gmail.com", url("https://github.com/srjn45"))
    ),
    description := "Official Scala gRPC client for ScrivaDB — Futures for unary calls, an Iterator/callback streaming abstraction for Find and Watch.",
    sonatypeProfileName := "io.github.srjn45",
    sonatypeCredentialHost := "central.sonatype.com",
    publishTo := sonatypePublishToBundle.value,
    // Lets `publishSigned` sign non-interactively in CI (gpg loopback) using the
    // passphrase injected by publish-clients.yml. Unused by a plain `sbt test`.
    pgpPassphrase := sys.env.get("PGP_PASSPHRASE").map(_.toCharArray),
  )
