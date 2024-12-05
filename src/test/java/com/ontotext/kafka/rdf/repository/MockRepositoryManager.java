package com.ontotext.kafka.rdf.repository;

import org.eclipse.rdf4j.repository.http.HTTPRepository;

import static org.mockito.Mockito.spy;

public class MockRepositoryManager {

	public static RepositoryManager createManagerSpy(HTTPRepository repository) {
		return spy(new RepositoryManager(repository));
	}

}
