plugins {
    kotlin("jvm") version "1.9.22"
    id("application")
    id("com.diffplug.spotless") version "6.25.0"

}

allprojects {
    group = "com.spond.data"
    version = "1.0-SNAPSHOT"

    repositories {
        mavenCentral()
    }
}
subprojects {
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "application")

    kotlin {
        jvmToolchain(17)
    }

    dependencies {
        implementation(kotlin("stdlib"))

        // Logging
        implementation("org.apache.logging.log4j:log4j-api:2.20.0")
        implementation("org.apache.logging.log4j:log4j-core:2.20.0")
        implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.20.0")

        // Apache Spark 3.5.1 for Scala 2.13
        implementation("org.apache.spark:spark-core_2.13:3.5.1")
        implementation("org.apache.spark:spark-sql_2.13:3.5.1")
        implementation("org.apache.spark:spark-mllib_2.13:3.5.1")

        // Delta Lake 2.4.0 is the last released version for Scala 2.13 (as of your repo view)
        implementation("io.delta:delta-spark_2.13:3.1.0")

        // Scala standard library – must match 2.13.x
        implementation("org.scala-lang:scala-library:2.13.10")

        implementation("org.apache.hadoop:hadoop-common:3.3.4") // or compatible with your Spark
        implementation("org.apache.hadoop:hadoop-client:3.3.4")


        // Testing
        testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.0")
        testImplementation(kotlin("test"))
        testImplementation("org.junit.jupiter:junit-jupiter:5.10.2") // or latest
        testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

        testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.0")

    }

    tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().configureEach {
        kotlinOptions {
            jvmTarget = "17"
            allWarningsAsErrors = true
            freeCompilerArgs = listOf("-Xjsr305=strict")
        }
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()

        // 🔧 Pass JVM options required for Spark compatibility with Java 17
        jvmArgs("--add-exports=java.base/sun.nio.ch=ALL-UNNAMED")

        testLogging {
            events("passed", "failed", "skipped", "standardOut", "standardError")
            showStandardStreams = true
        }
    }



    allprojects {
        repositories {
            mavenCentral()
        }
    }


}
