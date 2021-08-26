package com.ontotext.kafka.convert;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.Objects;

import org.apache.kafka.connect.errors.DataException;

public class DirectRDFConverter implements RecordConverter {

	@Override
	public Reader convert(Object obj) {
		Objects.requireNonNull(obj,"Cannot parse null objects");
		if (obj instanceof String) {
			return new StringReader(((String) obj));
		} else if (obj instanceof byte[]) {
			return new BufferedReader(new InputStreamReader(new ByteArrayInputStream((byte[]) obj)));
		}
		else {
			// return new StringReader(obj.toString());
			throw new DataException("error: no converter present due to unexpected object type "
					                        + obj.getClass().getName());
		}
	}
}
