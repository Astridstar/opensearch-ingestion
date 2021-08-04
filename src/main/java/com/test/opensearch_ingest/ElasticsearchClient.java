package com.test.opensearch_ingest;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.HashMap;

import java.time.*;
import java.time.format.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ElasticsearchClient {

    private static final Logger m_logger = LogManager.getLogger(ElasticsearchClient.class);

    String m_indexSuffix = "";
    String m_indexName = "";

    final CredentialsProvider m_credentialsProvider = new BasicCredentialsProvider();
    
    public boolean initialise(){

        m_indexSuffix =
            ZonedDateTime                       // Represent a moment as perceived in the wall-clock time used by the people of a particular region ( a time zone).
            .now(                               // Capture the current moment.
                ZoneId.of( "Asia/Singapore" )   // Specify the time zone using proper Continent/Region name. Never use 3-4 character pseudo-zones such as PDT, EST, IST. 
            )                                   // Returns a `ZonedDateTime` object. 
            .format(                            // Generate a `String` object containing text representing the value of our date-time object. 
                DateTimeFormatter.ofPattern( "yyyy.MM.dd.HH.mm.ss.SSS" )
            ) ;

        //Point to keystore with appropriate certificates for security.
        System.setProperty("javax.net.ssl.trustStore", "/full/path/to/keystore");
        System.setProperty("javax.net.ssl.trustStorePassword", "password-to-keystore");

        try {
            //Establish credentials to use basic authentication.
            //Only for demo purposes. Do not specify your credentials in code.
            m_credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("admin", "admin"));

            m_indexName = "public_feed_" + m_indexSuffix;
            m_logger.info("Index Name: {}", m_indexName);    
        } catch (Exception e) {
            m_logger.error("Error setting up credential provider: ", e);
            return false;
        }

        return true;
    }
    public void loadData(){
        //Create a client.
        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "https"))
        .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
            return httpClientBuilder.setDefaultCredentialsProvider(m_credentialsProvider);
                }
            });
        RestHighLevelClient client = new RestHighLevelClient(builder);

        //Create a non-default index with custom settings and mappings.
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(m_indexName);

        createIndexRequest.settings(Settings.builder() //Specify in the settings how many shards you want in the index.
        .put("index.number_of_shards", 2)
        .put("index.number_of_replicas", 1)
        );

        //1. Create a set of maps for the index's mappings.
        try{            
            HashMap<String, String> typeMapping = new HashMap<String,String>();
            typeMapping.put("type", "integer");
            HashMap<String, Object> ageMapping = new HashMap<String, Object>();
            ageMapping.put("age", typeMapping);
            HashMap<String, Object> mapping = new HashMap<String, Object>();
            mapping.put("properties", ageMapping);
            createIndexRequest.mapping(mapping);
            CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            m_logger.info("Index creation response: {}", createIndexResponse);
        } catch (IOException e) {
            m_logger.error("Error creating index {}", e);
        }

        //2. Adding data to the index.
        try{
            IndexRequest request = new IndexRequest(m_indexName); //Add a document to the custom-index we created.
            request.id("1"); //Assign an ID to the document.
    
            HashMap<String, String> stringMapping = new HashMap<String, String>();
            stringMapping.put("message:", "Testing Java REST client");
            request.source(stringMapping); //Place your content into the index's source.
            IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
            m_logger.info("Document addition response: {}", indexResponse);
        } catch (IOException e) {
            m_logger.error("Error creating document {}", e);
        }

        //3. Getting back the document
        try{
            GetRequest getRequest = new GetRequest(m_indexName, "1");
            GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
            m_logger.info("Retrieved document: {}", response.getSourceAsString());
        } catch (IOException e) {
            m_logger.error("Error retrieving document {}", e);
        }

        //4. Delete the document
        try{
            DeleteRequest deleteDocumentRequest = new DeleteRequest(m_indexName, "1"); //Index name followed by the ID.
            DeleteResponse deleteResponse = client.delete(deleteDocumentRequest, RequestOptions.DEFAULT);
            m_logger.info("Document deletion response: {}", deleteResponse);
        } catch (IOException e) {
            m_logger.error("Error deleting document {}", e);
        }

        //5. Delete the index
        try{
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(m_indexName); //Index name.
            AcknowledgedResponse deleteIndexResponse = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
            m_logger.info("Index deletion response: {}", deleteIndexResponse);
        } catch (IOException e) {
            m_logger.error("Error deleting index {}", e);
        }

          
        try{
            client.close();      
        } catch (IOException e) {
            m_logger.error("Error closing client {}", e);
        }
    }
}
