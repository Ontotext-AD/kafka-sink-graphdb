package com.ontotext.kafka.convert;

import com.ontotext.kafka.logging.LoggerFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.slf4j.Logger;

import java.io.*;

public class RdfFormatConverter {

	protected final Logger log;
	private final RDFFormat inputFormat;
	private final RDFFormat outputFormat;

	public RdfFormatConverter(RDFFormat inputFormat, RDFFormat outputFormat) {
		this.inputFormat = inputFormat;
		this.outputFormat = outputFormat;
		this.log = LoggerFactory.getLogger(getClass());
	}


	public byte[] convertData(byte[] inputData) throws IOException {
		if (inputFormat.equals(outputFormat)) {
			log.debug("Input and output formats [{}] are the same, skipping conversion", inputData);
			return inputData;
		}
		log.debug("Converting data from {} to {}", inputFormat, outputFormat);
		RDFParser rdfParser = Rio.createParser(inputFormat);
		try (InputStream is = new ByteArrayInputStream(inputData);
			 ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
			 BufferedOutputStream bos = new BufferedOutputStream(baos)) {
			RDFWriter rdfWriter = Rio.createWriter(outputFormat, bos);
			rdfParser.setRDFHandler(rdfWriter);
			rdfParser.parse(is);
			return baos.toByteArray();
		}
	}


	public RDFFormat getInputFormat() {
		return inputFormat;
	}

	public RDFFormat getOutputFormat() {
		return outputFormat;
	}
}
