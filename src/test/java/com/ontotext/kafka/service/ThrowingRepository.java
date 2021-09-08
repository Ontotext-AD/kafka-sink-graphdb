package com.ontotext.kafka.service;

import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.io.Reader;
import java.util.function.BiConsumer;

public class ThrowingRepository extends DummyRepository {

	private final Exception e;
	private final int numberOfThrows;

	private RepositoryConnection connection;

	public ThrowingRepository(BiConsumer<Reader, RDFFormat> consumer, Exception e, int numberOfThrows) {
		super(consumer);
		this.e = e;
		this.numberOfThrows = numberOfThrows;
	}

	@Override
	public RepositoryConnection getConnection() throws RepositoryException {
		if (connection == null)
			connection = new ThrowingExceptionConnection(consumer, e, numberOfThrows);
		return connection;
	}

}