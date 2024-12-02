package com.ontotext.kafka.operation;

import com.ontotext.kafka.GraphDBSinkConfig;
import com.ontotext.kafka.processor.SinkRecordsProcessor;
import com.ontotext.kafka.test.framework.ConnectionMockBuilder;
import com.ontotext.kafka.test.framework.RepositoryMockBuilder;
import com.ontotext.kafka.test.framework.TestSinkConfigBuilder;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.Test;

import java.io.Reader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ontotext.kafka.test.framework.RdfMockDataUtils.generateSinkRecords;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class GraphDBOperatorTest {

	private static final long INFINITE_RETRY = -1;
	private static final long NO_RETRY = 0;
	private static final long NO_DELAY = 0;

	private static final Map<String, Object> property = new HashMap<>();

	private Repository repository;
	private AtomicBoolean shouldRun;
	private LinkedBlockingDeque<Collection<SinkRecord>> sinkRecords;
	private GraphDBOperator op;
	private GraphDBSinkConfig config;
	private SinkRecordsProcessor processor;
	private RepositoryConnection connection;


	@Test
	public void should_run_throwingProcessor() {
		config = new TestSinkConfigBuilder()
			.errorMaxDelayInMillis(100L)
			.errorRetryTimeout(-1L)
			.tolerance(ToleranceType.ALL)
			.build();


		Queue<Reader> streams = new LinkedBlockingQueue<>();
		Queue<RDFFormat> formats = new LinkedBlockingQueue<>();
		connection = new ConnectionMockBuilder(null, (in, format) -> {
			streams.add(in);
			formats.add(format);
		}, null).build();
		repository = RepositoryMockBuilder.createDefaultMockedRepository(connection);
		shouldRun = new AtomicBoolean(true);
		sinkRecords = new LinkedBlockingDeque<>();

		processor = spy(new SinkRecordsProcessor(config, sinkRecords, repository));


		// Fail on first try, succeed on second
		doThrow(new RepositoryException("ERROR")).doReturn(connection).when(repository).getConnection();


		op = new GraphDBOperator(config);

		generateSinkRecords(sinkRecords, 1, 15);

		assertThat(op.execAndHandleError(processor)).isNotNull();
	}


}
