package com.ontotext.kafka.service;

import static com.ontotext.kafka.Utils.awaitEmptyCollection;
import static com.ontotext.kafka.Utils.awaitProcessorShutdown;
import static com.ontotext.kafka.Utils.generateAvroSinkRecords;
import static com.ontotext.kafka.Utils.initRepository;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


import com.ontotext.kafka.mocks.DummyErrorHandler;
import com.ontotext.kafka.mocks.DummyOperator;
import com.ontotext.kafka.operation.GraphDBOperator;
import com.ontotext.kafka.util.PropertiesUtil;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.util.Collection;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.Ignore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

public class InPlaceReplaceProcessorTest {

	private Queue<Reader> streams;
	private Queue<RDFFormat> formats;
	private Repository repository;
	private AtomicBoolean shouldRun;
	private Queue<Collection<SinkRecord>> sinkRecords;
	private DummyErrorHandler errorHandler;
	private GraphDBOperator operator;

	@BeforeEach
	public void setup() {
		streams = new LinkedBlockingQueue<>();
		formats = new LinkedBlockingQueue<>();
		repository = initRepository(streams, formats);
		shouldRun = new AtomicBoolean(true);
		sinkRecords = new LinkedBlockingQueue<>();
		errorHandler = new DummyErrorHandler();
		PropertiesUtil.setProperty(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG,
			"io.confluent.connect.avro.AvroConverter");
		PropertiesUtil.setProperty(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG,
			"io.confluent.connect.avro.AvroConverter");
		operator = new DummyOperator();
	}

	@Test
	@DisplayName("Test simple write")
	void testSimpleWrite()
		throws InterruptedException, IOException, RestClientException {
		int batch = 5;
		generateAvroSinkRecords(sinkRecords, 1, "extracted-order.avro", "encoded.avr");
		Thread recordsProcessor =
			createProcessorThread(sinkRecords, shouldRun, repository, batch, 5000,
				this.getClass().getClassLoader().getResource("walmartContext.json").getPath(),
				this.getClass().getClassLoader().getResource("walmartMapping.json").getPath());
		recordsProcessor.start();
		awaitEmptyCollection(sinkRecords);
		assertTrue(formats.isEmpty());
		assertTrue(streams.isEmpty());
		shouldRun.set(false);
		awaitProcessorShutdown(recordsProcessor);
		assertFalse(recordsProcessor.isAlive());
		assertEquals(48, streams.size());
		Model model = new TreeModel();
		while (!streams.isEmpty()) {
			model.addAll(Rio.parse(streams.poll(), RDFFormat.NQUADS));
		}
		assertEquals(48, model.size());
	}

	@Test
	@DisplayName("Test double write")
	void testDoubleWrite()
		throws InterruptedException, IOException, RestClientException {
		int batch = 5;
		generateAvroSinkRecords(sinkRecords, 1, "extracted-order.avro", "encoded.avr");
		generateAvroSinkRecords(sinkRecords, 1, "extracted-order.avro", "encoded2.avr");
		Thread recordsProcessor =
			createProcessorThread(sinkRecords, shouldRun, repository, batch, 5000,
				this.getClass().getClassLoader().getResource("walmartContext.json").getPath(),
				this.getClass().getClassLoader().getResource("walmartMapping.json").getPath());
		recordsProcessor.start();
		awaitEmptyCollection(sinkRecords);
		assertTrue(formats.isEmpty());
		assertTrue(streams.isEmpty());
		shouldRun.set(false);
		awaitProcessorShutdown(recordsProcessor);
		assertFalse(recordsProcessor.isAlive());
		assertEquals(97, streams.size());
		Model model = new TreeModel();
		while (!streams.isEmpty()) {
			model.addAll(Rio.parse(streams.poll(), RDFFormat.NQUADS));
		}
		// Off-by-9 (7 blanks, 1 nested blank and 1 reference to them) from what you'd get on GDB due to the dummy model not working OK with blanks.
		assertEquals(58, model.size());
		FileOutputStream fos = new FileOutputStream(
			this.getClass().getClassLoader().getResource("processed.nq").getPath());
		Rio.write(model, fos, RDFFormat.NQUADS);
	}

	@Test
	@Ignore
	@Timeout(5)
	void testCanReadAvro() throws IOException {
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(
			this.getClass().getClassLoader().getResourceAsStream("extracted-order.avro"));
		Decoder decoder = DecoderFactory.get()
			.binaryDecoder(this.getClass().getClassLoader().getResourceAsStream("encoded.avr"),
				null);
		Record avroRecord = (Record) new GenericDatumReader<>(schema).read(null, decoder);
		System.out.println(avroRecord);
	}

	@Test
	@Ignore
	void generateEncodedAvro() throws IOException {
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(
			this.getClass().getClassLoader().getResourceAsStream("extracted-order.avro"));
		Decoder decoder = DecoderFactory.get().jsonDecoder(schema,
			this.getClass().getClassLoader().getResourceAsStream("extracted-orders-message2.json"));
		Record avroRecord = (Record) new GenericDatumReader<>(schema).read(null, decoder);
		FileOutputStream fos = new FileOutputStream(new File(URI.create(
			String.valueOf(this.getClass().getClassLoader().getResource("encoded2.avr")))));
		BinaryEncoder encored = EncoderFactory.get().binaryEncoder(fos, null);
		GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
		writer.write(avroRecord, encored);
		encored.flush();
		fos.close();
	}

	@Test
	@Ignore
	void sendEncodedAvro() throws IOException {
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(
			this.getClass().getClassLoader().getResourceAsStream("extracted-order.avro"));
		Decoder decoder = DecoderFactory.get().jsonDecoder(schema,
			this.getClass().getClassLoader().getResourceAsStream("extracted-orders-message.json"));
		Record avroRecord = (Record) new GenericDatumReader<>(schema).read(null, decoder);
		ProducerRecord pr = new ProducerRecord("test", avroRecord);
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:19092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
			io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
			io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		props.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8085");
		KafkaProducer producer = new KafkaProducer<>(props);
		producer.send(pr);
		producer.flush();
	}


	@Test
	@Ignore
	void checkModel() throws IOException {
		Model model = setModel("base.jsonld");
	}

	private Thread createProcessorThread(Queue<Collection<SinkRecord>> sinkRecords,
										 AtomicBoolean shouldRun,
										 Repository repository, int batchSize, long commitTimeout,
										 String context, String mapping)
		throws IOException, RestClientException {
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(
			this.getClass().getClassLoader().getResourceAsStream("extracted-order.avro"));
		SchemaRegistryClient client = Mockito.mock(SchemaRegistryClient.class);
		Mockito.when(client.getByID(1)).thenReturn(schema);
		Thread thread = new Thread(
			new InPlaceReplaceProcessor(sinkRecords, shouldRun, repository, RDFFormat.NQUADS,
				batchSize,
				commitTimeout, errorHandler, operator, context, mapping, client));

		thread.setDaemon(true);
		return thread;
	}

	private Model setModel(String file) throws IOException {
		return Rio.parse(this.getClass().getClassLoader().getResourceAsStream(file), "",
			RDFFormat.JSONLD);
	}
}
