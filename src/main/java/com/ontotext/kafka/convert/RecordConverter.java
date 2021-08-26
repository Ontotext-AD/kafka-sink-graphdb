package com.ontotext.kafka.convert;

import java.io.Reader;

import org.apache.kafka.connect.data.Schema;

/**
 * Converter API for kafka message transformations i.e. {@link org.apache.kafka.connect.sink.SinkRecord}
 *
 * @author Tomas Kovachev tomas.kovachev@ontotext.com
 */
public interface RecordConverter {

	Reader convert(Object value);

}
