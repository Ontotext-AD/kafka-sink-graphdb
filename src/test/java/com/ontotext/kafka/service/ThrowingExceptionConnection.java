package com.ontotext.kafka.service;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;

import java.io.IOException;
import java.io.Reader;
import java.util.function.BiConsumer;

public class ThrowingExceptionConnection extends DummyRepositoryConnection {

	private int numberOfThrows;
	private boolean doThrow = true;
	private final Exception e;

	public ThrowingExceptionConnection(BiConsumer<Reader, RDFFormat> consumer, Exception e, int numberOfThrows) {
		super(consumer, null, null);
		this.e = e;
		this.numberOfThrows = numberOfThrows;
	}

	@Override
	public void add(Reader reader, String baseURI, RDFFormat dataFormat, Resource... contexts) throws IOException, RDFParseException, RepositoryException {
		if (doThrow) {
			numberOfThrows--;
			if (numberOfThrows <= 0) {
				doThrow = false;
			}

			if (e.getClass().equals(IOException.class))
				throw new IOException("IOException");
			else if (e.getClass().equals(RDFParseException.class))
				throw new RDFParseException("RDFParseException");
			else if (e.getClass().equals(RepositoryException.class))
				throw new RepositoryException("RepositoryException");
		}
		super.add(reader, baseURI, dataFormat, contexts);
	}

}