package org.springframework.cloud.stream.config;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.springframework.boot.context.properties.ConfigurationProperties;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@ConfigurationProperties("spring.cloud.stream.function")
@JsonInclude(Include.NON_DEFAULT)
public class FunctionProperties {

	/**
	 * Names of functions to bind (coma delimited)
	 */
	private List<String> names;

	/**
	 * Additional binding properties (see {@link BinderProperties}) per binding name (e.g., 'input`).
	 *
	 * For example; This sets the content-type for the 'input' binding of a Sink application:
	 * 'spring.cloud.stream.bindings.input.contentType=text/plain'
	 */
	private Map<String, FunctionBindingProperties> bindings = new TreeMap<>(
			String.CASE_INSENSITIVE_ORDER);

	public List<String> getNames() {
		return names;
	}

	public void setNames(List<String> names) {
		this.names = names;
	}


	public Map<String, FunctionBindingProperties> getBindings() {
		return bindings;
	}

	public void setBindings(Map<String, FunctionBindingProperties> bindings) {
		this.bindings = bindings;
	}
}
