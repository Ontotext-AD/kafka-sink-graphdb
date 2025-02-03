package com.ontotext.kafka.processor.record.handler;


import com.ontotext.kafka.GraphDBSinkConfig;
import com.ontotext.kafka.convert.JsonDataConverter;
import com.ontotext.kafka.graphdb.template.TemplateInput;
import com.ontotext.kafka.graphdb.template.TemplateUtil;
import com.ontotext.kafka.util.ValueUtil;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.repository.RepositoryConnection;

import java.io.IOException;

public interface RecordHandler {


	void handle(SinkRecord record, RepositoryConnection connection, GraphDBSinkConfig config) throws IOException;


	static RecordHandler addHandler() {
		return (record, connection, config) -> {
			connection.add(ValueUtil.convertRDFData(record.value()), config.getRdfFormat());
		};
	}

	static RecordHandler updateHandler(final JsonDataConverter jsonDataConverter) {
		return (record, connection, config) -> {
			byte[] data = ValueUtil.convertRDFData(record.value()).readAllBytes();
			TemplateUtil.executeUpdate(connection, new TemplateInput(config.getTemplateId(), jsonDataConverter.convert(data)));
		};
	}

	static RecordHandler replaceHandler() {
		return (record, connection, config) -> {
			Resource context = ValueUtil.convertIRIKey(record.key());
			connection.clear(context);
			if (record.value() != null) {
				connection.add(ValueUtil.convertRDFData(record.value()), config.getRdfFormat(), context);
			}
		};
	}


	static RecordHandler getRecordHandler(GraphDBSinkConfig config) {
		switch (config.getTransactionType()) {
			case ADD:
				return addHandler();
			case REPLACE_GRAPH:
				return replaceHandler();
			case SMART_UPDATE:
				return updateHandler(new JsonDataConverter(config.getRdfFormat()));
			default:
				throw new IllegalArgumentException(String.format("No handler for transaction type %s", config.getTransactionType()));
		}

	}

}



