import com.google.protobuf.gradle.*
import com.vanniktech.maven.publish.JavadocJar
import com.vanniktech.maven.publish.KotlinJvm
import com.vanniktech.maven.publish.SonatypeHost

plugins {
    kotlin("jvm") version "1.9.24"
    application
    id("com.google.protobuf") version "0.9.4"
    // Publishes to the Sonatype Central Portal (central.sonatype.com), mirroring
    // the Java client's setup. Wraps maven-publish + signing and handles the
    // Portal bundle upload + sources/javadoc jars. See `mavenPublishing {}` below.
    id("com.vanniktech.maven.publish") version "0.29.0"
}

group = "io.github.srjn45"
version = "1.2.1"

repositories {
    mavenCentral()
}

// `./gradlew run -PmainClass=io.github.srjn45.scriva.examples.BasicExampleKt`
application {
    mainClass.set(
        (project.findProperty("mainClass") as String?)
            ?: "io.github.srjn45.scriva.examples.BasicExampleKt"
    )
}

val grpcVersion = "1.64.0"
val grpcKotlinVersion = "1.4.1"
val protobufVersion = "3.25.3"
val coroutinesVersion = "1.8.1"

dependencies {
    implementation("io.grpc:grpc-netty-shaded:$grpcVersion")
    implementation("io.grpc:grpc-protobuf:$grpcVersion")
    implementation("io.grpc:grpc-stub:$grpcVersion")
    implementation("io.grpc:grpc-kotlin-stub:$grpcKotlinVersion")
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
    implementation("com.google.protobuf:protobuf-kotlin:$protobufVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:$coroutinesVersion")
    // Supplies google/api/annotations.proto (and other common protos) on the
    // import path so the shared proto/scriva.proto resolves — same as the Java client.
    implementation("com.google.api.grpc:proto-google-common-protos:2.37.1")

    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testImplementation("io.grpc:grpc-testing:$grpcVersion")
    testImplementation("io.grpc:grpc-inprocess:$grpcVersion")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:$coroutinesVersion")
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
        id("grpckt") {
            artifact = "io.grpc:protoc-gen-grpc-kotlin:$grpcKotlinVersion:jdk8@jar"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.builtins {
                id("kotlin")
            }
            task.plugins {
                id("grpc")
                id("grpckt")
            }
        }
    }
}

sourceSets {
    main {
        proto {
            // Generate stubs straight from the single source of truth, exactly
            // like the Java client does.
            srcDir("../../proto")
        }
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

// Target JVM 11 bytecode using whatever JDK runs Gradle (no toolchain download),
// exactly like the Java client.
tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().configureEach {
    compilerOptions {
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_11)
    }
}

tasks.test {
    useJUnitPlatform()
}

// ---------------------------------------------------------------------------
// Maven Central publishing (Sonatype Central Portal, namespace `io.github.srjn45`)
// ---------------------------------------------------------------------------
// Coordinates: io.github.srjn45:scriva-client-kotlin:1.2.1
//
// The publish-clients.yml workflow supplies credentials + the GPG signing key
// as ORG_GRADLE_PROJECT_* environment variables, which the vanniktech plugin
// reads automatically (identical to the Java client):
//   ORG_GRADLE_PROJECT_mavenCentralUsername      <- secret MAVEN_CENTRAL_USERNAME
//   ORG_GRADLE_PROJECT_mavenCentralPassword      <- secret MAVEN_CENTRAL_PASSWORD
//   ORG_GRADLE_PROJECT_signingInMemoryKey        <- secret MAVEN_GPG_PRIVATE_KEY
//   ORG_GRADLE_PROJECT_signingInMemoryKeyPassword<- secret MAVEN_GPG_PASSPHRASE
//
// Local validation (no network push, no signing):  ./gradlew publishToMavenLocal
// Real publish (workflow):                          ./gradlew publishAndReleaseToMavenCentral
mavenPublishing {
    publishToMavenCentral(SonatypeHost.CENTRAL_PORTAL)
    signAllPublications()

    coordinates("io.github.srjn45", "scriva-client-kotlin", "1.2.1")

    // Central requires a javadoc jar; an empty one satisfies the gate without
    // pulling in Dokka. Sources jar is enabled.
    configure(
        KotlinJvm(
            javadocJar = JavadocJar.Empty(),
            sourcesJar = true,
        )
    )

    pom {
        name.set("ScrivaDB Kotlin Client")
        description.set("Official Kotlin gRPC client for ScrivaDB — coroutine suspend calls and Flow-based streaming.")
        url.set("https://github.com/srjn45/scriva")

        licenses {
            license {
                name.set("MIT License")
                url.set("https://github.com/srjn45/scriva/blob/main/clients/kotlin/LICENSE")
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
