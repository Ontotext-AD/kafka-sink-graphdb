package com.ontotext.kafka;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;

import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.eclipse.rdf4j.repository.manager.RepositoryManager;
import org.eclipse.rdf4j.repository.sail.config.SailRepositoryConfig;
import org.eclipse.rdf4j.sail.memory.config.MemoryStoreConfig;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.ontotext.graphdb.GraphDBRepositoryManager;
import com.ontotext.test.TemporaryLocalFolder;

class GraphDBSinkTaskTest {

	private RepositoryManager manager;

	@Rule
	public TemporaryLocalFolder tmpFolder = new TemporaryLocalFolder();

	@BeforeEach
	public void init() throws IOException {
		tmpFolder.create();
		manager = new GraphDBRepositoryManager(new File(tmpFolder.getRoot().getAbsolutePath()));
		manager.init();
		addRepoConfig("id");
	}

	@Test
	@DisplayName("Test put RDF data without batching")
	void put() throws MalformedURLException {

	}


	private void addRepoConfig(String repoID) throws MalformedURLException {
		// SharedHttpClientSessionManager sessionManager = new SharedHttpClientSessionManager();
		// HttpClient client = sessionManager.getHttpClient();
		// manager.setHttpClient(client);
		// System.err.println(manager.getLocation());
		RepositoryConfig repoConfig = new RepositoryConfig();
		repoConfig.setID(repoID);
		SailRepositoryConfig sailConfig = new SailRepositoryConfig();
		sailConfig.setSailImplConfig(new MemoryStoreConfig());
		repoConfig.setRepositoryImplConfig(sailConfig);
		manager.addRepositoryConfig(repoConfig);
	}
}