package com.ontotext.kafka.service;

import com.ontotext.kafka.error.ErrorHandler;
import com.ontotext.kafka.operation.OperationHandler;
import com.ontotext.kafka.util.ValueUtil;
import io.confluent.connect.avro.AvroData;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InPlaceReplaceProcessor extends SinkRecordsProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(InPlaceReplaceProcessor.class);

	public InPlaceReplaceProcessor(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun,
								   Repository repository,
								   RDFFormat format, int batchSize, long timeoutCommitMs,
								   ErrorHandler errorHandler, OperationHandler operator) {
		super(sinkRecords, shouldRun, repository, format, batchSize, timeoutCommitMs, errorHandler,
			operator);
	}

	@Override
	protected void handleRecord(SinkRecord record, RepositoryConnection connection)
		throws RetriableException {
		long start = System.currentTimeMillis();
		if (LOG.isDebugEnabled()) {
			LOG.debug(record.toString());
		}
		AvroData avroData = new AvroData(100);
      Model incomingModel = null;
      try {
        incomingModel =
            ValueUtil.convertToModel(record.value(), avroData.fromConnectSchema(record.valueSchema()));
      } catch (IOException | URISyntaxException e) {
        throw new RuntimeException(e);
      }
      if (LOG.isDebugEnabled()) {
			LOG.debug("Produced RDF model from AVRO schema in {} ms",
				(System.currentTimeMillis() - start));
		}
		try {
			incomingModel.forEach(statement -> {
				connection.remove(statement.getSubject(), statement.getPredicate(), null,
					statement.getContext());
				connection.add(statement);
			});
		} catch (RepositoryException e) {
			throw new RetriableException("Failed to handle record: " + ValueUtil.recordInfo(record),
				e);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("Processed record in {} ms", (System.currentTimeMillis() - start));
		}

	}
}
