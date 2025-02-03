package com.ontotext.kafka.graphdb.template;

import java.util.Map;

public class TemplateInput {

	private final String templateId;
	private final Map<String, Object> data;

	public TemplateInput(String templateId, Map<String, Object> data) {
		this.templateId = templateId;
		this.data = data;
	}

	public String getTemplateId() {
		return templateId;
	}

	public Map<String, Object> getData() {
		return data;
	}
}
