package com.test.opensearch_ingest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class OpensearchIngestApplication {

	public static void main(String[] args) {

		try {
			ElasticsearchAdaptor adaptor = new ElasticsearchAdaptor();
			SpringApplication.run(OpensearchIngestApplication.class, args);
		}catch (Exception e) {

		}
	}

}
