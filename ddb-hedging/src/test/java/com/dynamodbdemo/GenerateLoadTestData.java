package com.dynamodbdemo;

import org.apache.commons.lang3.RandomStringUtils;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.GZIPOutputStream;


public class GenerateLoadTestData {

    public static final int NUMBER_OF_RECORDS_TO_CREATE = 1;

    public static final int NUMBER_OF_RECORDS_PER_FILE = 1;
    public static final String DDB_TABLE_NAME = "hedging-demo-104";
    public static final int ENTITY_COUNT = 50;
    static String RECORD_ID_TAG = "$RECORD_ID$";
    static String ENTITY_NUMBER_TAG = "$ENTITY_NUMBER$";

    static String S3_BUCKET_NAME = "entity-record-data-111";

    static String S3_KEY_PREFIX = "data-to-import/hedging-demo-103/";


    static int numberOfRecordsToCreate = NUMBER_OF_RECORDS_TO_CREATE;
    static int numberOfRecordsPerFile = NUMBER_OF_RECORDS_PER_FILE;

    static String ddbTableName = DDB_TABLE_NAME;

    public static void main(String[] args) throws IOException {
        String numberOfRecordsStr = System.getProperty("numberOfRecordsToCreate");
        String numberOfRecordsPerFileStr = System.getProperty("numberOfRecordsPerFile");
        String ddbTableNameStr = System.getProperty("ddbTableName");

        // Validate that all required properties are provided
        if (numberOfRecordsStr == null || numberOfRecordsPerFileStr == null || ddbTableNameStr == null) {

            System.out.println("Missing required system properties. Please provide:");
            System.out.println("-DnumberOfRecordsToCreate=<number>");
            System.out.println("-DnumberOfRecordsPerFile=<number>");
            System.out.println("-DddbTableName=<tableName>");
            System.out.println("Using Default properties");
        } else {
            try {
                numberOfRecordsToCreate = Integer.parseInt(numberOfRecordsStr);
                numberOfRecordsPerFile = Integer.parseInt(numberOfRecordsPerFileStr);
                ddbTableName = ddbTableNameStr;

                // Your existing code here using these variables
            } catch (NumberFormatException e) {
                System.out.println("Error: numberOfRecordsToCreate and numberOfRecordsPerFile must be valid integers");
                System.exit(1);
            }
        }

        System.out.println("Creating " + numberOfRecordsToCreate + " records");
        System.out.println("Records per file: " + numberOfRecordsPerFile);
        System.out.println("DynamoDB table name: " + ddbTableName);


        Set<String> recordIDSet = new HashSet<>();
        Set<String> entityNumbers = generateEntityNumbers(ENTITY_COUNT);

        Map<String, String> tokenMap;

        // Load the template
        List<String> templateLines = new ArrayList<>();
        Scanner scanner = new Scanner(new File("loadtest/entity_records_load_data_template.txt"));
        while (scanner.hasNextLine()) {
            templateLines.add(scanner.nextLine());
        }

        scanner.close();

        // Create credentials provider using the specified SSO profile
        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.builder()
                .profileName("PowerUserAccess-278322016574")
                .build();

        // Create S3 client with the credentials
        S3AsyncClient s3AsyncClient = S3AsyncClient.crtBuilder()
                .credentialsProvider(credentialsProvider)
                .region(Region.US_EAST_1)
                .build();


        // Example usage
        S3TransferManager transferManager = S3TransferManager.builder()
                .s3Client(s3AsyncClient)
                .build();

        int numberOfIterations = numberOfRecordsToCreate / numberOfRecordsPerFile;

        try (PrintWriter loadGenFileWriter = new PrintWriter("loadtest/data/" + ddbTableName + ".csv", StandardCharsets.UTF_8)) {

            for (int i = 0; i < numberOfIterations; i++) {
                String ddbFileName = "loadtest/data/" + ddbTableName + "-" + i + ".json";

                System.out.println("Creating File : " + ddbFileName);

                try (BufferedWriter ddbDataFileWriter = new BufferedWriter(new OutputStreamWriter(
                        new FileOutputStream(ddbFileName), StandardCharsets.UTF_8))) {


                    for (int j = 0; j < numberOfRecordsPerFile; j++) {

                        tokenMap = generateTestTokens(recordIDSet, entityNumbers);
                        String loadGenDataline = tokenMap.get(RECORD_ID_TAG) + "," + tokenMap.get(ENTITY_NUMBER_TAG);

                        loadGenFileWriter.println(loadGenDataline);


                        // Write the ddb test data records
                        for (String templateLine : templateLines) {
                            for (Map.Entry<String, String> entry : tokenMap.entrySet()) {
                                String key = entry.getKey();
                                templateLine = templateLine.replace(key, entry.getValue());
                            }
                            ddbDataFileWriter.write(templateLine);
                            ddbDataFileWriter.newLine();
                        }
                    }
                    ddbDataFileWriter.flush();

                    String outputFile = ddbFileName + ".gz";

                    String s3Key = S3_KEY_PREFIX + outputFile;
                    compressFileAndUploadToS3(ddbFileName, outputFile, S3_BUCKET_NAME, s3Key, transferManager, true);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            }

            loadGenFileWriter.flush();

            String s3Key = S3_KEY_PREFIX + "key/" + ddbTableName + ".csv.gz";
            compressFileAndUploadToS3("loadtest/data/" + ddbTableName + ".csv", "loadtest/data/" + ddbTableName + ".csv.gz", S3_BUCKET_NAME, s3Key, transferManager, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static void compressFileAndUploadToS3(String inputFile, String outputFile,
                                                 String bucketName, String s3Key,
                                                 S3TransferManager transferManager, boolean deleteInputFile) {
        try (FileInputStream fis = new FileInputStream(inputFile);
             GZIPOutputStream gzipOS = new GZIPOutputStream(new FileOutputStream(outputFile))) {

            // Compress file
            byte[] buffer = new byte[1024];
            int len;
            while ((len = fis.read(buffer)) != -1) {
                gzipOS.write(buffer, 0, len);
            }


            // Close resources before attempting to upload and delete
            gzipOS.close();
            fis.close();

            // Upload compressed file to S3
            UploadFileRequest uploadFileRequest = UploadFileRequest.builder()
                    .putObjectRequest(b -> b.bucket(bucketName).key(s3Key))
                    .source(Paths.get(outputFile))
                    .build();

            FileUpload fileUpload = transferManager.uploadFile(uploadFileRequest);
            CompletedFileUpload uploadResult = fileUpload.completionFuture().join();

            if (uploadResult.response().sdkHttpResponse().isSuccessful()) {
                // Delete the original input file
                File inputFileToDelete = new File(inputFile);

                if (deleteInputFile) {
                    if (!inputFileToDelete.delete()) {
                        System.err.println("Warning: Could not delete input file: " + inputFile);
                    }
                }


                // Optionally, delete the local compressed file after successful upload
                File compressedFileToDelete = new File(outputFile);
                if (!compressedFileToDelete.delete()) {
                    System.err.println("Warning: Could not delete compressed file: " + outputFile);
                }

            } else {
                throw new RuntimeException("Failed to upload file to S3");
            }

        } catch (IOException e) {
            System.err.println("Error processing file: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }


    public static Map<String, String> generateTestTokens(Set<String> recordIDSet,
                                                         Set<String> entityNumbers) {

        Map<String, String> tokenMap = new HashMap<>();

        String generatedString = generateUniqueRandom(recordIDSet, "99997f4f6446691".length(), true, true);
        tokenMap.put(RECORD_ID_TAG, generatedString);


        String entityNumber = pickSentimentalityNumber(entityNumbers);
        tokenMap.put(ENTITY_NUMBER_TAG, entityNumber);

        return tokenMap;
    }

    public static String generateUniqueRandom(Set<String> previousValuesSet, int length, boolean useLetters,
                                              boolean useNumbers) {

        String generatedString = RandomStringUtils.random(length, useLetters, useNumbers);
        while (previousValuesSet.contains(generatedString)) {
            generatedString = RandomStringUtils.random(length, useLetters, useNumbers);
        }
        previousValuesSet.add(generatedString);

        return generatedString;
    }

    public static Set<String> generateEntityNumbers(int count) {
        Set<String> entityNumbers = new HashSet<>();
        for (int i = 0; i < count; i++) {
            generateUniqueRandom(entityNumbers, "2220".length(), false, true);
        }
        return entityNumbers;
    }

    public static String pickSentimentalityNumber(Set<String> entityNumbers) {
        int index = (int) (Math.random() * entityNumbers.size());
        String[] entityNumbersArray = entityNumbers.toArray(new String[0]);
        return entityNumbersArray[index];
    }
}
