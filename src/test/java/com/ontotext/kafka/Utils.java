package com.ontotext.kafka;

import com.ontotext.kafka.mocks.DummyRepository;
import io.confluent.connect.avro.AvroData;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.io.Reader;
import java.util.Collection;
import java.util.Collections;
import java.util.Queue;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class Utils {

	private Utils() {
	}

	public static void awaitProcessorShutdown(Thread processor) throws InterruptedException {
		processor.join();
	}

	public static Repository initRepository(Queue<Reader> streams, Queue<RDFFormat> formats) {
		return new DummyRepository((in, format) -> {
			streams.add(in);
			formats.add(format);
		});
	}

	public static <E> void awaitCollectionSizeReached(Collection<E> collection, int size) {
		while (collection.size() < size) {
		}
	}

	public static <E> void awaitEmptyCollection(Collection<E> collection) {
		while (!collection.isEmpty()) {
		}
	}

	public static String generateRDFStatements(int quantity) {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < quantity; i++) {
			builder.append("<urn:one")
				.append(i)
				.append("> <urn:two")
				.append(i)
				.append("> <urn:three")
				.append(i)
				.append("> . \n");
		}
		return builder.toString();
	}

	public static org.apache.kafka.connect.data.Schema getAvroSchema(String source)
		throws IOException {
		Schema.Parser parser = new Schema.Parser();
		AvroData avroData = new AvroData(100);
		Schema schema = parser.parse(
			Utils.class.getClassLoader().getResourceAsStream(source));
		return avroData.toConnectSchema(schema);
	}

	public static byte[] getAvroData(String data) throws IOException {
		return Utils.class.getClassLoader().getResourceAsStream(data).readAllBytes();
	}

	public static void verifyForMilliseconds(Supplier<Boolean> supplier, long ms) {
		long timeUntilSchedule = System.currentTimeMillis() + ms;
		while (System.currentTimeMillis() < timeUntilSchedule) {
			assertTrue(supplier.get());
		}
	}

	public static void generateSinkRecords(Queue<Collection<SinkRecord>> sinkRecords, int recordsSize, int statementsSize) {
		for (int i = 0; i < recordsSize; i++) {
			SinkRecord sinkRecord = new SinkRecord("topic", 0, null, null, null,
				generateRDFStatements(statementsSize).getBytes(),
				12);
			sinkRecords.add(Collections.singleton(sinkRecord));
		}
	}

	public static void generateAvroSinkRecords(Queue<Collection<SinkRecord>> sinkRecords, int recordsSize, String schema, String data)
		throws IOException {
		for (int i = 0; i < recordsSize; i++) {
			SinkRecord sinkRecord = new SinkRecord("topic", 0, null, null, getAvroSchema(schema), getAvroData(data), 12);
			sinkRecords.add(Collections.singleton(sinkRecord));
		}
	}
}
