package com.ontotext.kafka.service;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.io.File;
import java.io.Reader;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class DummyRepository implements Repository {

	private BiConsumer<Reader, RDFFormat> consumer;
	private Consumer<Resource[]> contexts;

	public DummyRepository(BiConsumer<Reader, RDFFormat> consumer) {
		this.consumer = consumer;
		this.contexts = null;
	}

	public DummyRepository(BiConsumer<Reader, RDFFormat> consumer, Consumer<Resource[]> contexts) {
		this.consumer = consumer;
		this.contexts = contexts;
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
		return new DummyRepositoryConnection(consumer, contexts);
	}

	@Override
	public ValueFactory getValueFactory() {
		return SimpleValueFactory.getInstance();
	}
}