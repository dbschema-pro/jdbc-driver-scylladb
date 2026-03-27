plugins {
    alias(libs.plugins.wisecoders.commonGradle.jdbcDriver)
}

group = "com.wisecoders.jdbc-drivers"

jdbcDriver {
    dbId = "ScyllaDB"
}

dependencies {
    implementation(libs.wisecoders.commonLib.commonSlf4j)
    implementation(libs.wisecoders.commonJdbc.commonJdbcJvm)
    implementation(libs.wisecoders.commonJdbc.commonJdbcAws)
    implementation(libs.scylladb.javaDriverCore)
    implementation(libs.cassandraDriverKerberos)
    implementation(libs.slf4j.api)
    implementation(platform(libs.awsSdk2.bom))
    implementation(libs.awsSdk2.secretsManager)
    implementation(platform(libs.jackson.bom))
    implementation(libs.jackson.databind)

    runtimeOnly(libs.logback.classic)

    testImplementation(libs.mockito.junit.jupiter)
}
