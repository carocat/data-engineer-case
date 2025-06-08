plugins {
    kotlin("jvm")
}

kotlin {
    jvmToolchain(17)
}

sourceSets {
    main {
        kotlin {
            setSrcDirs(listOf("src/main/kotlin"))
        }
    }
    test {
        kotlin {
            setSrcDirs(listOf("src/test/kotlin"))
        }
    }
}

dependencies {
    implementation(kotlin("stdlib"))

    // Logging, Spark, etc. if needed
    // For example, no application dependencies here except those common libs
}
