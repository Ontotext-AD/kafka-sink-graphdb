package com.ontotext.kafka.processor;


import com.ontotext.kafka.GraphDBSinkConfig;
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

	static RecordHandler updateHandler() {
		return (record, connection, config) -> {
			String query = ValueUtil.createRecordUpdateQuery(record.key(), config.getTemplateId());
			connection.prepareUpdate(query).execute();
			connection.add(ValueUtil.convertRDFData(record.value()), config.getRdfFormat());
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

}



