package org.springframework.cloud.stream.function;

import java.lang.reflect.Type;
import java.util.function.Supplier;

/**
 *
 * @author Oleg Zhurakousky
 *
 */
public class TypeAwareSupplier implements Supplier<Object>, TypeAware {

	private final Supplier<Object> supplier;

	private final Type outputType;

	public TypeAwareSupplier(Supplier<Object> supplier, Type outputType) {
		this.supplier = supplier;
		this.outputType = outputType;
	}

	@Override
	public Object get() {
		return supplier.get();
	}

	public Type getOutputType() {
		return outputType;
	}
}
