package com.ontotext.kafka.mocks;

import java.io.ByteArrayOutputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import org.eclipse.rdf4j.common.transaction.IsolationLevel;
import org.eclipse.rdf4j.common.iteration.Iteration;
import org.eclipse.rdf4j.http.client.query.AbstractHTTPUpdate;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.rio.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URL;
import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class DummyRepositoryConnection implements RepositoryConnection {

	protected BiConsumer<Reader, RDFFormat> addFormatConsumer;
	private final Consumer<String> removeConsumer;
	private final BiConsumer<String, Reader> addContextConsumer;

	public DummyRepositoryConnection(BiConsumer<Reader, RDFFormat> addConsumer, BiConsumer<String, Reader> addContextConsumer,
									 Consumer<String> removeContextsConsumer) {
		this.addFormatConsumer = addConsumer;
		this.addContextConsumer = addContextConsumer;
		this.removeConsumer = removeContextsConsumer;
	}

	@Override
	public Repository getRepository() {
		return null;
	}

	@Override
	public void setParserConfig(ParserConfig config) {

	}

	@Override
	public ParserConfig getParserConfig() {
		return null;
	}

	@Override
	public ValueFactory getValueFactory() {
		return null;
	}

	@Override
	public boolean isOpen() throws RepositoryException {
		return false;
	}

	@Override
	public Query prepareQuery(QueryLanguage ql, String query) throws RepositoryException, MalformedQueryException {
		return null;
	}

	@Override
	public Query prepareQuery(QueryLanguage ql, String query, String baseURI) throws RepositoryException, MalformedQueryException {
		return null;
	}

	@Override
	public TupleQuery prepareTupleQuery(QueryLanguage ql, String query) throws RepositoryException, MalformedQueryException {
		return null;
	}

	@Override
	public TupleQuery prepareTupleQuery(QueryLanguage ql, String query, String baseURI) throws RepositoryException, MalformedQueryException {
		return null;
	}

	@Override
	public GraphQuery prepareGraphQuery(QueryLanguage ql, String query) throws RepositoryException, MalformedQueryException {
		return null;
	}

	@Override
	public GraphQuery prepareGraphQuery(QueryLanguage ql, String query, String baseURI) throws RepositoryException, MalformedQueryException {
		return null;
	}

	@Override
	public BooleanQuery prepareBooleanQuery(QueryLanguage ql, String query) throws RepositoryException, MalformedQueryException {
		return null;
	}

	@Override
	public BooleanQuery prepareBooleanQuery(QueryLanguage ql, String query, String baseURI) throws RepositoryException, MalformedQueryException {
		return null;
	}

	@Override
	public Update prepareUpdate(QueryLanguage ql, String update) throws RepositoryException, MalformedQueryException {
		return new AbstractHTTPUpdate(null, null, null, null) {
			@Override
			public void execute() throws UpdateExecutionException {

			}
		};
	}

	@Override
	public Update prepareUpdate(QueryLanguage ql, String update, String baseURI) throws RepositoryException, MalformedQueryException {
		return null;
	}

	@Override
	public RepositoryResult<Resource> getContextIDs() throws RepositoryException {
		return null;
	}

	@Override
	public RepositoryResult<Statement> getStatements(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts) throws RepositoryException {
		return null;
	}

	@Override
	public boolean hasStatement(Resource subj, IRI pred, Value obj, boolean includeInferred, Resource... contexts) throws RepositoryException {
		return false;
	}

	@Override
	public boolean hasStatement(Statement st, boolean includeInferred, Resource... contexts) throws RepositoryException {
		return false;
	}

	@Override
	public void exportStatements(Resource subj, IRI pred, Value obj, boolean includeInferred, RDFHandler handler, Resource... contexts) throws RepositoryException, RDFHandlerException {

	}

	@Override
	public void export(RDFHandler handler, Resource... contexts) throws RepositoryException, RDFHandlerException {

	}

	@Override
	public long size(Resource... contexts) throws RepositoryException {
		return 0;
	}

	@Override
	public boolean isEmpty() throws RepositoryException {
		return false;
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws RepositoryException {

	}

	@Override
	public boolean isAutoCommit() throws RepositoryException {
		return false;
	}

	@Override
	public boolean isActive() throws RepositoryException {
		return false;
	}

	@Override
	public void setIsolationLevel(IsolationLevel level) throws IllegalStateException {

	}

	@Override
	public IsolationLevel getIsolationLevel() {
		return null;
	}

	@Override
	public void begin() throws RepositoryException {

	}

	@Override
	public void begin(IsolationLevel level) throws RepositoryException {

	}

	@Override
	public void commit() throws RepositoryException {

	}

	@Override
	public void rollback() throws RepositoryException {

	}

	@Override
	public void add(InputStream in, String baseURI, RDFFormat dataFormat, Resource... contexts) throws IOException, RDFParseException, RepositoryException {

	}

	@Override
	public void add(Reader reader, String baseURI, RDFFormat dataFormat, Resource... contexts) throws IOException, RDFParseException, RepositoryException {
		if (contexts.length != 0) {
			addContextConsumer.accept(Arrays.stream(contexts).map(Value::stringValue).findFirst().get(), reader);
		}
		addFormatConsumer.accept(reader, dataFormat);
	}

	@Override
	public void add(URL url, String baseURI, RDFFormat dataFormat, Resource... contexts) throws IOException, RDFParseException, RepositoryException {

	}

	@Override
	public void add(File file, String baseURI, RDFFormat dataFormat, Resource... contexts) throws IOException, RDFParseException, RepositoryException {

	}

	@Override
	public void add(Resource subject, IRI predicate, Value object, Resource... contexts) throws RepositoryException {

	}

	@Override
	public void add(Statement st, Resource... contexts) throws RepositoryException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		RDFWriter writer = Rio.createWriter(RDFFormat.NQUADS, out);
		writer.startRDF();
		writer.handleStatement(st);
		writer.endRDF();
		StringReader reader = new StringReader(new String(out.toByteArray(), StandardCharsets.UTF_8));
		if (contexts.length != 0) {
			addContextConsumer.accept(Arrays.stream(contexts).map(Value::stringValue).findFirst().get(), reader);
		}
		addFormatConsumer.accept(reader, RDFFormat.NQUADS);
	}

	@Override
	public void add(Iterable<? extends Statement> statements, Resource... contexts) throws RepositoryException {

	}

	@Override
	public <E extends Exception> void add(Iteration<? extends Statement, E> statements, Resource... contexts) throws RepositoryException, E {

	}

	@Override
	public void remove(Resource subject, IRI predicate, Value object, Resource... contexts) throws RepositoryException {

	}

	@Override
	public void remove(Statement st, Resource... contexts) throws RepositoryException {
		removeConsumer.accept(Arrays.stream(contexts).map(Value::stringValue).findFirst().get());
	}

	@Override
	public void remove(Iterable<? extends Statement> statements, Resource... contexts) throws RepositoryException {

	}

	@Override
	public <E extends Exception> void remove(Iteration<? extends Statement, E> statements, Resource... contexts) throws RepositoryException, E {

	}

	@Override
	public void clear(Resource... contexts) throws RepositoryException {
		removeConsumer.accept(Arrays.stream(contexts).map(Value::stringValue).findFirst().get());
	}

	@Override
	public RepositoryResult<Namespace> getNamespaces() throws RepositoryException {
		return null;
	}

	@Override
	public String getNamespace(String prefix) throws RepositoryException {
		return null;
	}

	@Override
	public void setNamespace(String prefix, String name) throws RepositoryException {

	}

	@Override
	public void removeNamespace(String prefix) throws RepositoryException {

	}

	@Override
	public void clearNamespaces() throws RepositoryException {

	}
}
