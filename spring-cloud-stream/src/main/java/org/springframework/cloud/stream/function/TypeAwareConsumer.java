package org.springframework.cloud.stream.function;

import java.lang.reflect.Type;
import java.util.function.Consumer;

/**
 *
 * @author Oleg Zhurakousky
 *
 * @param <I>
 */
public class TypeAwareConsumer<I> implements Consumer<I>, TypeAware {

	private final Consumer<I> consumer;

	private final Type inputType;

	public TypeAwareConsumer(Consumer<I> consumer, Type inputType) {
		this.consumer = consumer;
		this.inputType = inputType;
	}

	@Override
	public void accept(I t) {
		this.consumer.accept(t);
	}

	public Type getInputType() {
		return inputType;
	}
}
