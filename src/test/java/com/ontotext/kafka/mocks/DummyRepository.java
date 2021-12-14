package com.ontotext.kafka.mocks;

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


	protected BiConsumer<Reader, RDFFormat> addFormatConsumer;
	private BiConsumer<String, Reader> addContextConsumer;
	private Consumer<String> removeConsumer;

	public DummyRepository(BiConsumer<Reader, RDFFormat> consumer) {
		this.addFormatConsumer = consumer;
	}

	public DummyRepository(BiConsumer<String, Reader> addContextConsumer,
						   BiConsumer<Reader, RDFFormat> addFormatConsumer, Consumer<String> removeConsumer) {
		this.addFormatConsumer = addFormatConsumer;
		this.addContextConsumer = addContextConsumer;
		this.removeConsumer = removeConsumer;
	}

	@Override
	public void setDataDir(File dataDir) {
	}

	@Override
	public File getDataDir() {
		return null;
	}

	@Override
	public void initialize() throws RepositoryException {
	}

	@Override
	public void init() throws RepositoryException {
	}

	@Override
	public boolean isInitialized() {
		return true;
	}

	@Override
	public void shutDown() throws RepositoryException {
	}

	@Override
	public boolean isWritable() throws RepositoryException {
		return true;
	}

	@Override
	public RepositoryConnection getConnection() throws RepositoryException {
		return new DummyRepositoryConnection(addFormatConsumer, addContextConsumer, removeConsumer);
	}

	@Override
	public ValueFactory getValueFactory() {
		return SimpleValueFactory.getInstance();
	}
}
