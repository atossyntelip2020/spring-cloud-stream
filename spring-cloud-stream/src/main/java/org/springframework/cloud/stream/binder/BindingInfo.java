package org.springframework.cloud.stream.binder;

public class BindingInfo {

	private final String functionName;

	private final Object function;

	private String inputDestinationName;

	private String outputDestinationName;

	private String groupName;

	public BindingInfo(Object function, String functionName) {
		this.functionName = functionName;
		this.function = function;
	}

	public String getInputDestinationName() {
		return inputDestinationName;
	}

	public void setInputDestinationName(String inputDestinationName) {
		this.inputDestinationName = inputDestinationName;
	}

	public String getOutputDestinationName() {
		return outputDestinationName;
	}

	public void setOutputDestinationName(String outputDestinationName) {
		this.outputDestinationName = outputDestinationName;
	}

	public String getGroupName() {
		return groupName;
	}

	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	public String getFunctionName() {
		return functionName;
	}

	public Object getFunction() {
		return function;
	}
}
