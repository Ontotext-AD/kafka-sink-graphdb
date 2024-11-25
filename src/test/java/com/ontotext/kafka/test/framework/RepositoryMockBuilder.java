package com.ontotext.kafka.test.framework;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.mockito.stubbing.Answer;

import java.io.File;

import static org.mockito.Mockito.*;

public class RepositoryMockBuilder {

	private final Repository mocked;

	public RepositoryMockBuilder() {
		this.mocked = mock(Repository.class);
	}

	public RepositoryMockBuilder dataDir(File dataDir) {
		doReturn(dataDir).when(mocked).getDataDir();
		return this;
	}

	public RepositoryMockBuilder initialize() {
		doReturn(true).when(mocked).isInitialized();
		return this;
	}

	public RepositoryMockBuilder init(Answer<Void> answer) {
		doAnswer(answer).when(mocked).init();
		return this;
	}

	public RepositoryMockBuilder shutDown(Answer<Void> answer) {
		doAnswer(answer).when(mocked).shutDown();
		return this;
	}

	public RepositoryMockBuilder writable() {
		doReturn(true).when(mocked).isWritable();
		return this;
	}

	public RepositoryMockBuilder connection(RepositoryConnection connection) {
		doReturn(connection).when(mocked).getConnection();
		return this;
	}

	public RepositoryMockBuilder valueFactory(ValueFactory valueFactory) {
		doReturn(valueFactory).when(mocked).getValueFactory();
		return this;
	}

	public Repository createRepository() {
		return mocked;
	}

	public static Repository createDefaultMockedRepository(RepositoryConnection connection) {
		return new RepositoryMockBuilder()
			.writable()
			.initialize()
			.connection(connection)
			.valueFactory(SimpleValueFactory.getInstance())
			.createRepository();
	}

}
