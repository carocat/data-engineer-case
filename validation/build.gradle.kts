plugins {
    kotlin("jvm")
    application
}

kotlin {
    jvmToolchain(17)
}

application {
    mainClass.set("com.spond.validation.ValidationApp")

    applicationDefaultJvmArgs = listOf(
        "--add-opens", "java.base/javax.security.auth=ALL-UNNAMED",
        "--add-opens", "java.base/java.lang=ALL-UNNAMED",
        "--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
        "--enable-native-access=ALL-UNNAMED"
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
