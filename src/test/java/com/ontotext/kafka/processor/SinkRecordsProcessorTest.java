package com.ontotext.kafka.processor;

import com.ontotext.kafka.GraphDBSinkConfig;
import com.ontotext.kafka.rdf.repository.MockRepositoryManager;
import com.ontotext.kafka.rdf.repository.RepositoryManager;
import com.ontotext.kafka.test.framework.ConnectionMockBuilder;
import com.ontotext.kafka.test.framework.RepositoryMockBuilder;
import com.ontotext.kafka.test.framework.TestSinkConfigBuilder;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.io.Reader;
import java.util.Collection;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ontotext.kafka.test.framework.RdfMockDataUtils.generateRDFStatements;
import static com.ontotext.kafka.test.framework.RdfMockDataUtils.generateSinkRecords;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class SinkRecordsProcessorTest {

	private Queue<Reader> streams;
	private Queue<RDFFormat> formats;
	private HTTPRepository repository;
	private AtomicBoolean shouldRun;
	private LinkedBlockingQueue<Collection<SinkRecord>> sinkRecords;
	private RepositoryManager repositoryMgr;

	@BeforeEach
	public void setup() {
		streams = new LinkedBlockingQueue<>();
		formats = new LinkedBlockingQueue<>();
		RepositoryConnection connection = new ConnectionMockBuilder(null, (in, format) -> {
			streams.add(in);
			formats.add(format);
		}, null).build();

		repository = RepositoryMockBuilder.createDefaultMockedRepository(connection);
		repositoryMgr = MockRepositoryManager.createManager(repository);
		shouldRun = mock(AtomicBoolean.class);
		sinkRecords = spy(new LinkedBlockingQueue<>());
		doReturn(CollectionUtils.isNotEmpty(sinkRecords)).when(shouldRun).get();
	}

	@Test
	@DisplayName("Test should skip record with null key")
	@Timeout(5)
	void testShouldSkipInvalidRecord() throws InterruptedException, IOException {
		GraphDBSinkConfig config = new TestSinkConfigBuilder()
			.transactionType(GraphDBSinkConfig.TransactionType.SMART_UPDATE)
			.batchSize(4)
			.timeoutCommitMs(5000)
			.tolerance(ToleranceType.ALL)
			.rdfFormat(RDFFormat.NQUADS.getDefaultFileExtension())
			.build();
		generateSinkRecords(sinkRecords, 3, 15);

		SinkRecord invalidRecord = new SinkRecord("topic", 0, null, null, null,
			generateRDFStatements(3).getBytes(),
			12);
		sinkRecords.add(Collections.singleton(invalidRecord));


		SinkRecordsProcessor processor = spy(new SinkRecordsProcessor(config, sinkRecords, repositoryMgr));
		processor.run(); //Should terminate once all records have been consumed, as per the mocked shouldRun variable (or timeout in case of a bug/failure)
		assertThat(formats).isNotEmpty();
		assertThat(streams).isNotEmpty();
		assertThat(streams).hasSize(3);
		for (Reader reader : streams) {
			assertThat(Rio.parse(reader, RDFFormat.NQUADS)).hasSize(15);
		}
	}

	@Test
	@DisplayName("Test should skip multiple invalid records")
//	@Timeout(5)
	void testShouldSkipMultipleInvalidRecords() throws InterruptedException, IOException {
		GraphDBSinkConfig config = new TestSinkConfigBuilder()
			.transactionType(GraphDBSinkConfig.TransactionType.SMART_UPDATE)
			.batchSize(4)
			.timeoutCommitMs(5000)
			.tolerance(ToleranceType.ALL)
			.rdfFormat(RDFFormat.NQUADS.getDefaultFileExtension())
			.build();
		generateSinkRecords(sinkRecords, 3, 15);
		SinkRecord invalidRecord = new SinkRecord("topic", 0, null, null, null,
			generateRDFStatements(3).getBytes(),
			12);
		sinkRecords.add(Collections.singleton(invalidRecord));
		generateSinkRecords(sinkRecords, 1, 15);
		SinkRecord invalidRecord2 = new SinkRecord("topic", 0, null, null, null,
			generateRDFStatements(3).getBytes(),
			12);
		sinkRecords.add(Collections.singleton(invalidRecord2));


		SinkRecordsProcessor processor = spy(new SinkRecordsProcessor(config, sinkRecords, repositoryMgr));
		doAnswer(invocation -> {
			if (sinkRecords.isEmpty()) {
				throw new InterruptedException("int");
			} else {
				return sinkRecords.poll();
			}
		}).when(processor).pollForMessages();
		processor.run(); //Should terminate once all records have been consumed, as per the mocked shouldRun variable (or timeout in case of a bug/failure)
		assertThat(formats).isNotEmpty();
		assertThat(streams).isNotEmpty();
		assertThat(streams).hasSize(4);
		for (Reader reader : streams) {
			assertThat(Rio.parse(reader, RDFFormat.NQUADS)).hasSize(15);
		}
		ArgumentCaptor<SinkRecord> argument = ArgumentCaptor.forClass(SinkRecord.class);
		assertThat(argument.getAllValues()).contains(invalidRecord, invalidRecord2);
	}
}
