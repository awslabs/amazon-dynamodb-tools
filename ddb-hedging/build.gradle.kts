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

    // AWS SDK v2.x BOM (Bill of Materials)
    implementation(platform("software.amazon.awssdk:bom:2.30.2"))

    // DynamoDB v2.x dependencies
    implementation("software.amazon.awssdk:dynamodb")
    implementation("software.amazon.awssdk:dynamodb-enhanced") // For higher-level DynamoDB operations

    implementation("software.amazon.awssdk:netty-nio-client")

    // CloudWatch v2.x dependencies
//    implementation("software.amazon.awssdk:cloudwatch")
//    implementation("software.amazon.awssdk:cloudwatchmetrics")

    // https://mvnrepository.com/artifact/org.projectlombok/lombok
    compileOnly("org.projectlombok:lombok:1.18.32")
    annotationProcessor("org.projectlombok:lombok:1.18.32")
    dependencies {
    testImplementation("org.apache.commons:commons-lang3:3.14.0")
}

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
