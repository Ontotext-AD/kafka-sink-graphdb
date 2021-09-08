package com.ontotext.kafka.service;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.io.File;
import java.io.InputStream;
import java.io.Reader;
import java.util.function.BiConsumer;

public class DummyRepository implements Repository {

	protected BiConsumer<Reader, RDFFormat> consumer;

	public DummyRepository(BiConsumer<Reader, RDFFormat> consumer) {
		this.consumer = consumer;
	}

	@Override
	public void setDataDir(File dataDir) {}

	@Override
	public File getDataDir() {
		return null;
	}

	@Override
	public void initialize() throws RepositoryException {}

	@Override
	public void init() throws RepositoryException {}

	@Override
	public boolean isInitialized() {
		return true;
	}

	@Override
	public void shutDown() throws RepositoryException {}

	@Override
	public boolean isWritable() throws RepositoryException {
		return true;
	}

	@Override
	public RepositoryConnection getConnection() throws RepositoryException {
		return new DummyRepositoryConnection(consumer);
	}

	@Override
	public ValueFactory getValueFactory() {
		return SimpleValueFactory.getInstance();
	}
}