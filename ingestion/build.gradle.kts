plugins {
    kotlin("jvm")
    application
}

kotlin {
    jvmToolchain(17)
}

application {
    mainClass.set("com.spond.ingestion.IngestionApp")

    applicationDefaultJvmArgs = listOf(
        "--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.named<JavaExec>("run") {
    jvmArgs(
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}


dependencies {
    implementation(project(":common"))
}
