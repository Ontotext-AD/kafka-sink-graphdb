package com.ontotext.kafka;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.eclipse.rdf4j.rio.RDFFormat;

import com.ontotext.kafka.util.PropertiesUtil;
import com.ontotext.kafka.util.RDFValueUtil;

public class GraphDBSinkTask extends SinkTask {

	private Map<String, String> properties;
	private Repository repository;
	private GraphDBSinkConfig.TransactionType transactionType;
	private RDFFormat format;
	// private GraphDBSinkConfig.AuthenticationType

	@Override
	public String version() {
		return PropertiesUtil.getVersion();
	}

	@Override
	public void start(Map<String, String> properties) {
		this.properties = properties;
		this.repository = new HTTPRepository(properties.get(GraphDBSinkConfig.SERVER_IRI),
				properties.get(GraphDBSinkConfig.REPOSITORY));
		this.format = RDFValueUtil.getRDFFormat();
	}

	@Override
	public void put(Collection<SinkRecord> collection) {
		switch (transactionType) {
			case ADD:
				addData(collection);
				return;
			case SMART_UPDATE:
			case REPLACE_GRAPH:
			default:
				throw new UnsupportedOperationException("");
		}
	}

	@Override
	public void stop() {}

	private void addData(Collection<SinkRecord> collection) {
		try (RepositoryConnection connection = repository.getConnection()) {
			for (SinkRecord record : collection) {
				connection.add(new ByteArrayInputStream(getDataStream(record.value())), format);
			}
		} catch (Exception e) {
			throw new RetriableException(e.getMessage());
		}
	}

	private byte[] getDataStream(Object obj) {
		try (var bos = new ByteArrayOutputStream();
				var out = new ObjectOutputStream(bos)) {
			out.writeObject(obj);
			out.flush();
			return bos.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException("WELL.... SHIT");
		}
	}
}
