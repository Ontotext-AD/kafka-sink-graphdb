package com.ontotext.kafka.rdf.repository;

import org.eclipse.rdf4j.repository.http.HTTPRepository;

public class MockRepositoryManager {

	public static RepositoryManager createManager(HTTPRepository repository) {
		return new RepositoryManager(repository);
	}

}
