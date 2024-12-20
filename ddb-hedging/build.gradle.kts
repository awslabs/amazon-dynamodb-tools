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

// https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-bom
    implementation("com.amazonaws:aws-java-sdk-bom:1.12.770")

// https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-dynamodb
    implementation("com.amazonaws:aws-java-sdk-dynamodb:1.12.770")

// https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-cloudwatch
    implementation("com.amazonaws:aws-java-sdk-cloudwatch:1.12.770")

// https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-cloudwatchmetrics
    implementation("com.amazonaws:aws-java-sdk-cloudwatchmetrics:1.12.770")

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
