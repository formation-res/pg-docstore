import com.avast.gradle.dockercompose.ComposeExtension
import kotlin.math.max
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    alias(libs.plugins.dokka)
    alias(libs.plugins.docker.compose)
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
    sourceCompatibility = JavaVersion.VERSION_25
    targetCompatibility = JavaVersion.VERSION_25
    withSourcesJar()
}

kotlin {
    compilerOptions {
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_25)
    }
}

dependencies {
    api(libs.coroutines.core)
    implementation(libs.coroutines.slf4j)
    api(libs.serialization.json)
    implementation(libs.kotlin.logging)
    implementation(libs.kotlin.reflect)
    api(libs.serialization.extensions)
    api(libs.postgresql)
    api(libs.hikaricp)

    testImplementation(kotlin("test-junit5"))
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.launcher)
    testImplementation(libs.kotest.assertions)
    testImplementation(libs.slf4j.api)
    testRuntimeOnly(libs.logback.classic)
    testImplementation(libs.kotlin4example)
}

configure<ComposeExtension> {
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

val dokkaJar = tasks.register<Jar>("dokkaJar") {
    from(tasks.named("dokkaGeneratePublicationHtml"))
    archiveClassifier.set("javadoc")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
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

val useExistingPostgres = providers.gradleProperty("useExistingPostgres")
    .map(String::toBooleanStrict).orElse(false)

tasks.withType<Test>().configureEach {
    if (!useExistingPostgres.get()) {
        dependsOn("composeUp")
        finalizedBy("composeDown")
    }
    systemProperty("pgdocstore.test.jdbcUrl", providers.gradleProperty("testJdbcUrl")
        .getOrElse("jdbc:postgresql://localhost:55432/docstore"))
    useJUnitPlatform()
    systemProperty("junit.jupiter.execution.parallel.enabled", "true")
    systemProperty("junit.jupiter.execution.parallel.mode.default", "concurrent")
    systemProperty("junit.jupiter.execution.parallel.mode.classes.default", "concurrent")
    systemProperty("junit.jupiter.execution.parallel.config.strategy", "fixed")
    val threads = max(3, Runtime.getRuntime().availableProcessors() - 2)
    systemProperty("junit.jupiter.execution.parallel.config.fixed.parallelism", threads)
    systemProperty("junit.jupiter.execution.parallel.config.fixed.max-pool-size", threads)
    testLogging.exceptionFormat = TestExceptionFormat.FULL
    testLogging.events = setOf(TestLogEvent.FAILED, TestLogEvent.SKIPPED)
}
