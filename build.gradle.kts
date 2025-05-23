import com.avast.gradle.dockercompose.ComposeExtension
import java.net.ConnectException
import java.net.URI
import kotlin.math.max
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent
import org.jetbrains.dokka.gradle.DokkaTask

plugins {
    kotlin("jvm")
    kotlin("plugin.serialization")
    id("org.jetbrains.dokka")
    id("com.avast.gradle.docker-compose")
    id("maven-publish")
}

repositories {
    mavenCentral()
    maven("https://maven.tryformation.com/releases") {
        content {
            includeGroup("com.tryformation")
            includeGroup("com.jillesvangurp")
            includeGroup("com.github.jillesvangurp")
        }
    }
    maven(url = "https://jitpack.io") {
        content {
            includeGroup("com.github.jillesvangurp")
        }
    }

}

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

dependencies {
    implementation(KotlinX.coroutines.jdk8)
    implementation(KotlinX.coroutines.slf4j)
    implementation(KotlinX.serialization.json)
    implementation(KotlinX.datetime)
    implementation("io.github.microutils:kotlin-logging:_")

    api("com.jillesvangurp:kotlinx-serialization-extensions:_")

    implementation("io.github.microutils:kotlin-logging:_")

    // jdbc+hikaricp
    api("org.postgresql:postgresql:_")

    // HikariCP Connection Pool
    api("com.zaxxer:HikariCP:_")

    testImplementation(kotlin("test-junit5"))
    testImplementation(Testing.junit.jupiter.api)
    testImplementation(Testing.junit.jupiter.engine)
    testImplementation(Testing.kotest.assertions.core)
    testImplementation("org.slf4j:slf4j-api:_")
    testImplementation("org.slf4j:jcl-over-slf4j:_")
    testImplementation("org.slf4j:log4j-over-slf4j:_")
    testImplementation("org.slf4j:jul-to-slf4j:_")
    testImplementation("org.apache.logging.log4j:log4j-to-slf4j:_") // es seems to insist on log4j2
    testImplementation("ch.qos.logback:logback-classic:_")
    testImplementation("com.github.jillesvangurp:kotlin4example:_")
}

configure<ComposeExtension> {
    buildAdditionalArgs = listOf("--force-rm")
    stopContainers = true
    removeContainers = true
    useComposeFiles = listOf("docker-compose.yml")
    setProjectName("pg-docstore")
    listOf("/usr/bin/docker","/usr/local/bin/docker").firstOrNull {
        File(it).exists()
    }?.let { docker ->
        // works around an issue where the docker
        // command is not found
        // falls back to the default, which may work on
        // some platforms
        dockerExecutable.set(docker)
    }

}

val sourcesJar by tasks.registering(Jar::class) {
    archiveClassifier.set("sources")
    from(sourceSets.main.get().allSource)
}

val dokkaJar = tasks.register<Jar>("dokkaJar") {
    from(tasks["dokkaHtml"])
    dependsOn(tasks["dokkaHtml"])
    archiveClassifier.set("javadoc")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            artifact(sourcesJar.get())
            artifact(dokkaJar.get())
        }
    }
    repositories {
        maven {
            // change to point to your repo, e.g. http://my.org/repo
            url = uri("gcs://mvn-public-tryformation/releases")
            name = "FormationPublic"
        }
    }
}

tasks.withType<Test> {
    failFast = false
    notCompatibleWithConfigurationCache("db isUp check is not compatible")

    val isUp = try {
        URI.create("http://localhost:5432").toURL().openConnection().connect()
        true
    } catch(e: ConnectException) {
        project.logger.lifecycle("CONNECTION REFUSED ${e::class.simpleName} ${e.message}")
        e.message != "Connection refused"
    }

    if(!isUp) {
        project.logger.lifecycle("Compose hooks")
        // if it's already running just use the existing instance
        dependsOn("composeUp")
        finalizedBy("composeDown")
    } else {
        project.logger.lifecycle("DB is already running, no compose hooks")
    }

    useJUnitPlatform {

    }

    systemProperties["junit.jupiter.execution.parallel.enabled"] = "true"
    // executes test classes concurrently
    systemProperties["junit.jupiter.execution.parallel.mode.default"] = "concurrent"
    // executes tests inside a class concurrently
    systemProperties["junit.jupiter.execution.parallel.mode.classes.default"] = "concurrent"
    systemProperties["junit.jupiter.execution.parallel.config.strategy"] = "fixed"

    // make sure we don't starve es and enrichment of cpu cores
    // note, our eventually blocks cause threads to spend a lot of time delaying
    val threads = max(3,Runtime.getRuntime().availableProcessors()-2)
    println("running tests with $threads threads on a machine with ${Runtime.getRuntime().availableProcessors()} CPUs and ${Runtime.getRuntime().maxMemory()/1024/1024} MB memory")
//    val threads = 3
    systemProperties["junit.jupiter.execution.parallel.config.fixed.parallelism"]=threads
    systemProperties["junit.jupiter.execution.parallel.config.fixed.max-pool-size"]=threads

    systemProperties["junit.jupiter.testclass.order.default"] = "org.junit.jupiter.api.ClassOrderer\$ClassName"
//    systemProperties["junit.jupiter.testclass.order.random.seed"] = "42"
    // works around an issue with the ktor client and redis client needing more than 64 threads in our tests.
//    systemProperties["kotlinx.coroutines.io.parallelism"] = "200"

    // junit test runner in gradle ignores @ActiveProfile, go figure
    systemProperty("spring.profiles.active", "test")

    testLogging.exceptionFormat = TestExceptionFormat.FULL
    testLogging.events = setOf(
        TestLogEvent.FAILED,
        TestLogEvent.PASSED,
        TestLogEvent.SKIPPED,
        TestLogEvent.STANDARD_ERROR,
        TestLogEvent.STANDARD_OUT
    )

    addTestListener(object : TestListener {
        val failures = mutableListOf<String>()
        override fun beforeSuite(desc: TestDescriptor) {
        }

        override fun afterSuite(desc: TestDescriptor, result: TestResult) {

        }

        override fun beforeTest(desc: TestDescriptor) {
        }

        override fun afterTest(desc: TestDescriptor, result: TestResult) {
            if (result.resultType == TestResult.ResultType.FAILURE) {
                val report =
                    """
                    TESTFAILURE ${desc.className} - ${desc.name}
                    ${
                        result.exception?.let { e ->
                            """
                            ${e::class.simpleName} ${e.message}
                        """.trimIndent()
                        }
                    }
                    -----------------
                    """.trimIndent()
                failures.add(report)
            }
        }
    })
}

// gradle dokkaGfm
tasks.withType<DokkaTask>().configureEach {
    notCompatibleWithConfigurationCache("https://github.com/Kotlin/dokka/issues/2231")
}




