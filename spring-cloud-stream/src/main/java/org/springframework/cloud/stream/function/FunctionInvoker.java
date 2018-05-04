package org.springframework.cloud.stream.function;

import java.util.function.Consumer;

/**
 *
 * @author Oleg Zhurakousky
 *
 * @param <F> - the type of raw Input of a function. For example Message or Flux etc.
 * @see MessagingFunctionInvoker
 */
public interface FunctionInvoker<F> extends Consumer<F> {

}
