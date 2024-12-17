package com.dynamodbdemo;

import org.apache.commons.lang3.RandomStringUtils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.*;


public class GenerateLoadTestData {

    public static final int NUMBER_OF_RECORDS_TO_CREATE = 100000;
    public static final String DDB_TABLE_NAME = "hedging-demo-101";
    public static final int ENTITY_COUNT = 50;
    static String RECORD_ID_TAG = "$RECORD_ID$";
    static String ENTITY_NUMBER_TAG = "$ENTITY_NUMBER$";

    public static void main(String[] args) throws IOException {

        Set<String> recordIDSet = new HashSet<>();
        Set<String> entityNumbers = generateEntityNumbers(ENTITY_COUNT);

        Map<String, String> tokenMap = null;

        // Load the template
        List<String> templateLines = new ArrayList<>();
        Scanner scanner = new Scanner(new File("loadtest/entity_records_load_data_template.txt"));
        while (scanner.hasNextLine()) {
            templateLines.add(scanner.nextLine());
        }

        scanner.close();

        try (PrintWriter ddbDataFileWriter = new PrintWriter("loadtest/data/" + DDB_TABLE_NAME + ".json", StandardCharsets.UTF_8); PrintWriter loadGenFileWriter = new PrintWriter("loadtest/data/" + DDB_TABLE_NAME + ".csv", StandardCharsets.UTF_8)) {
            for (int j = 0; j < NUMBER_OF_RECORDS_TO_CREATE; j++) {

                tokenMap = generateTestTokens(recordIDSet, entityNumbers);
                String loadGenDataline = tokenMap.get(RECORD_ID_TAG) + "," + tokenMap.get(ENTITY_NUMBER_TAG);

                loadGenFileWriter.println(loadGenDataline);


                // Write the ddb test data records
                for (String templateLine : templateLines) {
                    for (Map.Entry<String, String> entry : tokenMap.entrySet()) {
                        String key = entry.getKey();
                        templateLine = templateLine.replace(key, entry.getValue());
                    }
                    ddbDataFileWriter.println(templateLine);
                }
            }
        } catch (Exception e) {
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
