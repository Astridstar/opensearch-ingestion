package com.test.opensearch_ingest;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.io.InvalidObjectException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ElasticsearchAdaptor {
 
    private static final Logger m_logger = LogManager.getLogger(ElasticsearchAdaptor.class);
 
    PublicFeed m_hybridAnalysisFeed = new PublicFeed();
    ElasticsearchClient m_client = new ElasticsearchClient();
    JsonNode m_jsonObject = null;

    public ElasticsearchAdaptor() throws InvalidObjectException {
        if(!loadSourceData()) {
            m_logger.error("Error initialising the adaptor.");
            throw new InvalidObjectException("Error initialising the adaptor.");
        }

        if(!configureClients()) {
            m_logger.error("Error configuring clients.");
            throw new InvalidObjectException("Error initialising the adaptor."); 
        }

        try{
            m_client.loadData();
        } catch (Exception e) {
            m_logger.error("Exception caught while trying to ingest data into elasticsearch {}", e);
            throw new InvalidObjectException("Error ingesting data!"); 
        }
    }

    private boolean loadSourceData(){

		try{
			String file = "/home/aranel/Downloads/hyrbid-analysis-daily-feed.json";
			m_logger.debug("Attempting to parse json @ {}", file);
		    m_jsonObject = m_hybridAnalysisFeed.parseJsonFile(file);
		} catch (IOException e) {
			m_logger.error("Exception caught while attempting to parse file @ {}", e);
            return false;
		}

        return true;
    }

    private boolean configureClients(){
        return m_client.initialise();
    }
}
