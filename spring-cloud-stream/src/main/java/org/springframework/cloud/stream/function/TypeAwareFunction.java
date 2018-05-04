package org.springframework.cloud.stream.function;

import java.lang.reflect.Type;
import java.util.function.Function;

/**
 *
 * @author Oleg Zhurakousky
 *
 * @param <I>
 * @param <O>
 */
public class TypeAwareFunction<I,O> implements Function<I,O>, TypeAware {

	private final Function<I,O> function;

	private final Type inputType;

	private final Type outputType;

	public TypeAwareFunction(Function<I,O> function, Type inputType, Type outputType) {
		this.function = function;
		this.inputType = inputType;
		this.outputType = outputType;
	}

	@Override
	public O apply(I t) {
		return this.function.apply(t);
	}

	public Type getInputType() {
		return inputType;
	}

	public Type getOutputType() {
		return outputType;
	}
}
