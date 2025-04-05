plugins {
    java
    id("org.springframework.boot") version "3.2.4"
    id("io.spring.dependency-management") version "1.1.4"
}

group = "com.dynamodbdemo"
version = "0.0.1-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_21
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    testImplementation("org.springframework.boot:spring-boot-starter-test")

    implementation(platform("software.amazon.awssdk:bom:2.31.15"))
    implementation("software.amazon.awssdk:dynamodb")
    implementation("software.amazon.awssdk:dynamodb-enhanced")
    implementation("software.amazon.awssdk:netty-nio-client")
    implementation("software.amazon.awssdk:aws-crt-client")
    implementation("software.amazon.awssdk:sso")
    implementation("software.amazon.awssdk:ssooidc")

    compileOnly("org.projectlombok:lombok:1.18.32")
    annotationProcessor("org.projectlombok:lombok:1.18.32")
    testImplementation("org.apache.commons:commons-lang3:3.14.0")
    testImplementation("software.amazon.awssdk:s3-transfer-manager")
    testImplementation("software.amazon.awssdk:s3")



}


//Run this task with like
//  ./gradlew bootTestRun -DnumberOfRecordsToCreate=1000 -DnumberOfRecordsPerFile=100 -DddbTableName=MyDynamoDBTable
tasks.bootTestRun {
    systemProperties = System.getProperties().map { it.key.toString() to it.value.toString() }.toMap()
    mainClass.set("com.dynamodbdemo.GenerateLoadTestData")
}


tasks.withType<Test> {
    useJUnitPlatform()
}
