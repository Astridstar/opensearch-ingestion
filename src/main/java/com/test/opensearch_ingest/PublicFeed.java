package com.test.opensearch_ingest;

import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Paths;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

public class PublicFeed {

    public JsonNode parseJsonFile(String filename) throws IOException {

        //read json file data to String
        byte[] jsonData = Files.readAllBytes(Paths.get(filename));

        //create ObjectMapper instance
        ObjectMapper objectMapper = new ObjectMapper();

        //read JSON like DOM Parser
        JsonNode rootNode = objectMapper.readTree(jsonData);
        JsonNode idNode = rootNode.path("id");
        System.out.println("id = "+idNode.asInt());    
        
        //logger.debug("JsonNode {}", rootNode);
        return rootNode;
    }

}
