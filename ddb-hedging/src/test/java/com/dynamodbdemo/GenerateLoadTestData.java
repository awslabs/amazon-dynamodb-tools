package com.dynamodbdemo;

import org.apache.commons.lang3.RandomStringUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.zip.GZIPOutputStream;


public class GenerateLoadTestData {

    public static final int NUMBER_OF_RECORDS_TO_CREATE = 100;

    public static final int NUMBER_OF_RECORDS_PER_FILE = 100;
    public static final String DDB_TABLE_NAME = "hedging-demo-101";
    public static final int ENTITY_COUNT = 50;
    static String RECORD_ID_TAG = "$RECORD_ID$";
    static String ENTITY_NUMBER_TAG = "$ENTITY_NUMBER$";


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

        int numberOfIterations = numberOfRecordsToCreate / numberOfRecordsPerFile;

        try (PrintWriter loadGenFileWriter = new PrintWriter("loadtest/data/" + ddbTableName + ".csv", StandardCharsets.UTF_8)) {

            for (int i = 0; i < numberOfIterations; i++) {
                String ddbFileName = "loadtest/data/" + ddbTableName +"-" + i+ ".json";

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
                    compressFile(ddbFileName, ddbFileName + ".gz");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    // Method to compress a file using GZIP
    public static void compressFile(String inputFile, String outputFile) {
        try (FileInputStream fis = new FileInputStream(inputFile);
             GZIPOutputStream gzipOS = new GZIPOutputStream(new FileOutputStream(outputFile))) {

            byte[] buffer = new byte[1024];
            int len;
            while ((len = fis.read(buffer)) != -1) {
                gzipOS.write(buffer, 0, len);
            }

        } catch (IOException e) {
            System.err.println("Error compressing file: " + e.getMessage());
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
