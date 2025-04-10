package com.ontotext.kafka.gdb;

import org.eclipse.rdf4j.repository.http.HTTPRepository;

import static org.mockito.Mockito.spy;

public class MockRepositoryManager {

	public static GDBConnectionManager createManagerSpy(HTTPRepository repository) {
		return spy(new GDBConnectionManager(repository));
	}

}
