package org.springframework.cloud.stream.function;

import java.util.Map;
import java.util.function.Consumer;
/**
 *
 * @author Oleg Zhurakousky
 *
 * @param <F> - the type of raw Input of a function. For example Message or Flux etc.
 */
public interface FunctionRegistry<F> {

	@SuppressWarnings("rawtypes")
	Map<String, TypeAwareFunction> getFunctions();

	@SuppressWarnings("rawtypes")
	Map<String, TypeAwareConsumer> getConsumers();

	FunctionInvoker<F> getFunctionInvoker(TypeAware typeAware, Consumer<F> delegate);

	FunctionInvoker<F> getConsumerInvoker(TypeAware typeAware);
}
