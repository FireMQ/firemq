// FireMQ for Kotlin/JVM: UniFFI bindings (JNA) over the Rust client.
//
// JNA loads libfiremq_client_ffi from `jna.library.path`. Tests take its directory from
// FIREMQ_LIB_DIR (default: the workspace's target/release) and the broker from FIREMQ_URL.
//
//   cargo build -p firemq-client-ffi --release
//   FIREMQ_URL=fire://guest:guest@localhost:5673 gradle test

plugins {
    kotlin("jvm") version "2.3.21"
    `java-library`
}

group = "io.github.firemq"
version = "0.1.0"

repositories {
    mavenCentral()
}

// The conformance driver (spec/conformance.md) lives in its own source set so the library
// does not depend on a JSON parser.
val conformance: SourceSet by sourceSets.creating

dependencies {
    api("net.java.dev.jna:jna:5.19.1")
    testImplementation(kotlin("test"))
    "conformanceImplementation"(sourceSets.main.get().output)
    "conformanceImplementation"("net.java.dev.jna:jna:5.19.1")
    "conformanceImplementation"("org.jetbrains.kotlinx:kotlinx-serialization-json:1.9.0")
}

kotlin {
    compilerOptions {
        // The generated bindings trip a few style warnings; keep the build quiet.
        suppressWarnings.set(true)
    }
}

val libDir: String = System.getenv("FIREMQ_LIB_DIR") ?: "$rootDir/../../target/release"

tasks.test {
    useJUnitPlatform()
    systemProperty("jna.library.path", libDir)
    testLogging {
        events("passed", "failed", "skipped")
        showStandardStreams = true
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}

// Writes build/conformance-driver, a launcher for the conformance runner:
//   gradle conformanceDriver
//   firemq-conformance run --driver bindings/kotlin/build/conformance-driver
tasks.register("conformanceDriver") {
    dependsOn(conformance.classesTaskName)
    val launcher = layout.buildDirectory.file("conformance-driver")
    outputs.file(launcher)
    doLast {
        val classpath = conformance.runtimeClasspath.asPath
        launcher.get().asFile.apply {
            writeText(
                "#!/bin/sh\nexec java -Djna.library.path=\"$libDir\" -cp \"$classpath\" " +
                    "io.github.firemq.conformance.DriverKt \"$@\"\n",
            )
            setExecutable(true)
        }
    }
}
