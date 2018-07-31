package org.springframework.cloud.stream.binder;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.function.Consumer;
import java.util.function.Function;

import org.springframework.cloud.function.context.FunctionType;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.function.core.FluxWrapper;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;

import reactor.core.publisher.Flux;

/**
 *
 * @author Oleg Zhurakousky
 *
 * @param <I>
 * @param <O>
 */
class FunctionInvoker implements Function<Flux<? extends Object>, Flux<Object>>  {

	private final Consumer<Message<?>> producingMessageHandler;

	private final boolean isConsumer;

//	private final boolean isFlux;

//	private final Type inputType;
//
	private final Class<?> inputClass;

	private final Object userFunction;

	private final SmartMessageConverter messageConverter;

	public FunctionInvoker(Object userFunction, FunctionInspector functionInspector, SmartMessageConverter messageConverter) {
		this(userFunction, functionInspector, messageConverter, null);
	}

	@SuppressWarnings("rawtypes")
	public FunctionInvoker(Object userFunction, FunctionInspector functionInspector, SmartMessageConverter messageConverter, Consumer<Message<?>> producingMessageHandler) {
		Assert.notNull(messageConverter, "'messageConverter' must not be null");
		this.messageConverter = messageConverter;
		this.userFunction = userFunction;

		FunctionType functionType = functionInspector.getRegistration(userFunction).getType();
		this.isConsumer = Void.class.isAssignableFrom(functionType.getOutputType());
		this.inputClass = functionType.getInputType();
//
//		if (functionType.getType() instanceof ParameterizedType) {
//			Type[] types = ((ParameterizedType)functionType.getType()).getActualTypeArguments();
//			if (this.isFlux && types[0] instanceof ParameterizedType) {
//				types[0] = ((ParameterizedType)types[0]).getActualTypeArguments()[0];
//				if (!this.isConsumer) {
//					types[1] = ((ParameterizedType)types[1]).getActualTypeArguments()[0];
//				}
//			}
//			this.inputType = types[0];
//		}
//		else {
//			this.inputType = functionType.getInputType();
//		}

		this.producingMessageHandler = producingMessageHandler;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Flux<Object> apply(Flux<? extends Object> input) {
		Message[] last = new Message[1];
		input = input.doOnNext(msg -> {last[0] = (Message) msg;}).map(message -> resolveArgument((Message)message));
		input = input.transform(f -> ((Function<Flux, Flux>)userFunction).apply(f));
		return input.map(result -> toMessage(result, last[0]));
	}

	private Message<?> toMessage(Object value, Message originalMessage) {
		System.out.println("Converting TO MESSAGE: " + originalMessage);
		Message<?> result = this.messageConverter.toMessage(value, originalMessage.getHeaders());
		return result;
	}

	private Object resolveArgument(Message<?> message) {
		System.out.println("Converting FROM MESSAGE: " + message);
		Object result = message;
		if (this.shouldConvertFromMessage(message)) {
			result = this.messageConverter.fromMessage(message, this.inputClass);
		}
		return result;
	}

	private boolean shouldConvertFromMessage(Message<?> message) {
		return !this.inputClass.isAssignableFrom(byte[].class) && !this.inputClass.isAssignableFrom(Object.class);
	}
}
