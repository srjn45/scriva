import com.google.protobuf.gradle.*
import com.vanniktech.maven.publish.SonatypeHost

plugins {
    `java-library`
    application
    id("com.google.protobuf") version "0.9.4"
    // Publishes to the Sonatype Central Portal (central.sonatype.com), the
    // successor to OSSRH/Nexus. Wraps maven-publish + signing and handles the
    // Portal bundle upload + sources/javadoc jars. See `mavenPublishing {}` below.
    id("com.vanniktech.maven.publish") version "0.29.0"
}

group = "com.srjn45"
version = "1.2.0"

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

repositories {
    mavenCentral()
}

// `gradle run -PmainClass=com.srjn45.scriva.examples.BasicExample`
application {
    mainClass.set(
        (project.findProperty("mainClass") as String?)
            ?: "com.srjn45.scriva.examples.BasicExample"
    )
}

val grpcVersion = "1.64.0"
val protobufVersion = "3.25.3"

dependencies {
    implementation("io.grpc:grpc-netty-shaded:$grpcVersion")
    implementation("io.grpc:grpc-protobuf:$grpcVersion")
    implementation("io.grpc:grpc-stub:$grpcVersion")
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
    compileOnly("javax.annotation:javax.annotation-api:1.3.2")
    implementation("com.google.api.grpc:proto-google-common-protos:2.37.1")

    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testImplementation("io.grpc:grpc-testing:$grpcVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                id("grpc")
            }
        }
    }
}

sourceSets {
    main {
        proto {
            srcDir("../../proto")
        }
    }
}

tasks.test {
    useJUnitPlatform()
}

// The Central Portal requires a javadoc jar. Disable doclint so the build is not
// failed by cosmetic issues in the bundled example sources (e.g. an unescaped
// `&` in a doc comment), and keep the log quiet.
tasks.withType<Javadoc>().configureEach {
    (options as StandardJavadocDocletOptions).apply {
        addStringOption("Xdoclint:none", "-quiet")
        encoding = "UTF-8"
    }
}

// ---------------------------------------------------------------------------
// Maven Central publishing (Sonatype Central Portal, namespace `com.srjn45`)
// ---------------------------------------------------------------------------
// Coordinates: com.srjn45:scriva-client:1.2.0
//
// The publish-clients.yml workflow supplies credentials + the GPG signing key
// as ORG_GRADLE_PROJECT_* environment variables, which the vanniktech plugin
// reads automatically:
//   ORG_GRADLE_PROJECT_mavenCentralUsername      <- secret MAVEN_CENTRAL_USERNAME
//   ORG_GRADLE_PROJECT_mavenCentralPassword      <- secret MAVEN_CENTRAL_PASSWORD
//   ORG_GRADLE_PROJECT_signingInMemoryKey        <- secret MAVEN_GPG_PRIVATE_KEY
//   ORG_GRADLE_PROJECT_signingInMemoryKeyPassword<- secret MAVEN_GPG_PASSPHRASE
//
// Local validation (no network push, no signing):  gradle publishToMavenLocal
// Real publish (workflow):                          gradle publishAndReleaseToMavenCentral
mavenPublishing {
    publishToMavenCentral(SonatypeHost.CENTRAL_PORTAL)
    // Signs every publication (required by Central). Skipped for publishToMavenLocal.
    signAllPublications()

    coordinates("com.srjn45", "scriva-client", "1.2.0")

    // Enables the sources + javadoc jars Central Portal requires.
    configure(
        com.vanniktech.maven.publish.JavaLibrary(
            javadocJar = com.vanniktech.maven.publish.JavadocJar.Javadoc(),
            sourcesJar = true,
        )
    )

    pom {
        name.set("ScrivaDB Java Client")
        description.set("Official Java gRPC client for ScrivaDB — a file-based document database with a gRPC API.")
        url.set("https://github.com/srjn45/scriva")

        licenses {
            license {
                name.set("MIT License")
                url.set("https://github.com/srjn45/scriva/blob/main/clients/java/LICENSE")
                distribution.set("repo")
            }
        }
        developers {
            developer {
                id.set("srjn45")
                name.set("srjn45")
                url.set("https://github.com/srjn45")
            }
        }
        scm {
            url.set("https://github.com/srjn45/scriva")
            connection.set("scm:git:https://github.com/srjn45/scriva.git")
            developerConnection.set("scm:git:ssh://git@github.com/srjn45/scriva.git")
        }
    }
}
