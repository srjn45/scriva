import com.google.protobuf.gradle.*

plugins {
    `java-library`
    application
    id("com.google.protobuf") version "0.9.4"
}

group = "com.srjn45"
version = "0.1.0"

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

repositories {
    mavenCentral()
}

// `gradle run -PmainClass=com.srjn45.filedbv2.examples.BasicExample`
application {
    mainClass.set(
        (project.findProperty("mainClass") as String?)
            ?: "com.srjn45.filedbv2.examples.BasicExample"
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
