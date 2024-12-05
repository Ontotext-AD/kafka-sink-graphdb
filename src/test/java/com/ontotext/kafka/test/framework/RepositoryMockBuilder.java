package com.ontotext.kafka.test.framework;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.mockito.stubbing.Answer;

import java.io.File;

import static org.mockito.Mockito.*;

public class RepositoryMockBuilder {

	private final HTTPRepository mocked;

	public RepositoryMockBuilder() {
		this.mocked = mock(HTTPRepository.class);
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

	public RepositoryMockBuilder url(String url) {
		doReturn(url).when(mocked).getRepositoryURL();
		return this;
	}

	public HTTPRepository createRepository() {
		return mocked;
	}

	public static HTTPRepository createDefaultMockedRepository(RepositoryConnection connection) {
		return new RepositoryMockBuilder()
			.writable()
			.initialize()
			.connection(connection)
			.valueFactory(SimpleValueFactory.getInstance())
			.url("TestRepository")
			.createRepository();
	}

	public static HTTPRepository createDefaultMockedRepository() {
		return new RepositoryMockBuilder()
			.writable()
			.initialize()
			.connection(mock(RepositoryConnection.class))
			.valueFactory(SimpleValueFactory.getInstance())
			.url("TestRepository")
			.createRepository();
	}

}
