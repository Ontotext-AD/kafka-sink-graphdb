package com.ontotext.kafka.test.framework;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.http.HTTPUpdate;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.mockito.Mockito.*;

public class ConnectionMockBuilder {

	private final RepositoryConnection mocked;
	private final BiConsumer<String, Reader> addContextConsumer;
	private final BiConsumer<Reader, RDFFormat> addFormatConsumer;
	private final Consumer<String> removeConsumer;

	public ConnectionMockBuilder(BiConsumer<String, Reader> addContextConsumer,
								 BiConsumer<Reader, RDFFormat> addFormatConsumer,
								 Consumer<String> removeConsumer) {
		this.addContextConsumer = addContextConsumer == null ? (s, r) -> {
		} : addContextConsumer;
		this.addFormatConsumer = addFormatConsumer == null ? (s, r) -> {
		} : addFormatConsumer;
		this.removeConsumer = removeConsumer == null ? (s) -> {
		} : removeConsumer;
		this.mocked = mock(RepositoryConnection.class);
	}

	public RepositoryConnection build() {
		try {
			doAnswer(invocation -> {
				Reader reader = invocation.getArgument(0, Reader.class);
				RDFFormat dataFormat = invocation.getArgument(1, RDFFormat.class);
				addFormatConsumer.accept(reader, dataFormat);
				return null;
			}).when(mocked).add(any(Reader.class), any(RDFFormat.class), any(Resource[].class));

			doAnswer(invocation -> {
				Resource[] contexts = invocation.getArgument(0, Resource[].class);
				removeConsumer.accept(Arrays.stream(contexts).map(Value::stringValue).findFirst().orElse(""));
				return null;
			}).when(mocked).clear(any(Resource[].class));

			HTTPUpdate mockedHTTPUpdate = mock(HTTPUpdate.class);
			doNothing().when(mockedHTTPUpdate).execute();
			doReturn(mockedHTTPUpdate).when(mocked).prepareUpdate(anyString());

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return mocked;
	}
}
