package com.ontotext.kafka.graphdb.template;

import org.apache.commons.lang.StringUtils;
import org.eclipse.rdf4j.common.net.ParsedIRI;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

public class ValueParser {

	private static final SimpleValueFactory FACTORY = SimpleValueFactory.getInstance();
	private static final String FORBIDDEN_SYMBOLS = "%<>\\{}|^` \"";
	public static final String ID_TAG = "@id";
	public static final String VALUE_TAG = "@value";
	public static final String TYPE_TAG = "@type";
	public static final String LANGUAGE_TAG = "@language";

	private enum ValueType {
		IRI, VALUE, TYPED_VALUE, LANGUAGE_VALUE, EMPTY
	}


	public static Value getValue(Object obj) {
		if (obj == null) {
			return null;
		}
		Class<?> klass = obj.getClass();
		if (BigDecimal.class.equals(klass)) {
			return FACTORY.createLiteral((BigDecimal) obj);
		}
		if (BigInteger.class.equals(klass)) {
			return FACTORY.createLiteral((BigInteger) obj);
		}
		if (Long.class.equals(klass)) {
			return FACTORY.createLiteral((Long) obj);
		}
		if (Integer.class.equals(klass)) {
			return FACTORY.createLiteral((Integer) obj);
		}
		if (Short.class.equals(klass)) {
			return FACTORY.createLiteral((Short) obj);
		}
		if (Byte.class.equals(klass)) {
			return FACTORY.createLiteral((Byte) obj);
		}
		if (Double.class.equals(klass)) {
			return FACTORY.createLiteral((Double) obj);
		}
		if (Float.class.equals(klass)) {
			return FACTORY.createLiteral((Float) obj);
		}
		if (Boolean.class.equals(klass)) {
			return FACTORY.createLiteral((Boolean) obj);
		}
		if (obj instanceof List) {
			// only use first element, since we are dealing with single records
			return getValue(((List<?>) obj).get(0));
		}

		if (obj instanceof Map) {
			return new MapParser((Map<String, Object>) obj).parse();
		}

		// continue as String
		String s = obj.toString();
		if (StringUtils.isEmpty(s)) {
			return null;
		}
		try {
			new ParsedIRI(s);
			return FACTORY.createIRI(s);
		} catch (Exception e) {
			// skip and process as simple string
		}
		return FACTORY.createLiteral(s);
	}

	private static class MapParser {
		private final IRI id;
		private final IRI type;
		private final String value;
		private final String language;
		private final ValueType valueType;

		MapParser(Map<String, Object> input) {
			valueType = getValidState(input);
			id = getIri(input.get(ID_TAG));
			type = getIri(input.get(TYPE_TAG));
			value = getString(input.get(VALUE_TAG));
			language = getString(input.get(LANGUAGE_TAG));
		}

		private ValueType getValidState(Map<String, Object> input) {
			int size = input.size();
			if (size == 0) {
				return ValueType.EMPTY;
			} else if (size == 1) {
				if (input.containsKey(ID_TAG)) {
					return ValueType.IRI;
				} else if (input.containsKey(VALUE_TAG)) {
					return ValueType.VALUE;
				}
			} else if (size == 2) {
				if (input.containsKey(VALUE_TAG)) {
					if (input.containsKey(TYPE_TAG)) {
						return ValueType.TYPED_VALUE;
					} else if (input.containsKey(LANGUAGE_TAG)) {
						return ValueType.LANGUAGE_VALUE;
					}
				}
			}
			throw new IllegalArgumentException("Can't parse " + input + " to Value.");
		}

		private String getString(Object val) {
			return (val == null) ? null : "" + val;
		}

		Value parse() {
			switch (valueType) {
				case EMPTY:
					return null;
				case IRI:
					return getIri(id);
				case VALUE:
					return FACTORY.createLiteral(value);
				case TYPED_VALUE:
					return FACTORY.createLiteral(value, type);
				case LANGUAGE_VALUE:
					return FACTORY.createLiteral(value, language);
				default:
					throw new IllegalArgumentException("Should not get to here. Stateless map.");
			}
		}

		private IRI getIri(Object iriObj) {
			if (iriObj == null) {
				return null;
			}
			String sIri = iriObj.toString();
			try {
				new ParsedIRI(sIri);
				return FACTORY.createIRI(sIri);
			} catch (Exception e) {
				throw new IllegalArgumentException("Can't parse {" + iriObj + "} to IRI.", e);
			}
		}
	}

}
