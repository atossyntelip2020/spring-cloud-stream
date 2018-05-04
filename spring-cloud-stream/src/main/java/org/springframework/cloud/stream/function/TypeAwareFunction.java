package org.springframework.cloud.stream.function;

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

import reactor.core.publisher.Flux;

/**
 *
 * @author Oleg Zhurakousky
 *
 * @param <I>
 * @param <O>
 */
public class TypeAwareFunction implements Function<Flux<Message<?>>, Flux<Message<?>>>  {

	private final Consumer<Message<?>> producingMessageHandler;

	private final boolean isConsumer;

	private final boolean isFlux;

	private final Type inputType;

	private final Class<?> inputClass;

	private final Object userFunction;

	private final SmartMessageConverter messageConverter;

	public TypeAwareFunction(Object userFunction, FunctionInspector functionInspector, SmartMessageConverter messageConverter) {
		this(userFunction, functionInspector, messageConverter, null);
	}

	@SuppressWarnings("rawtypes")
	public TypeAwareFunction(Object userFunction, FunctionInspector functionInspector, SmartMessageConverter messageConverter, Consumer<Message<?>> producingMessageHandler) {
		this.messageConverter = messageConverter;
		this.userFunction = userFunction instanceof FluxWrapper ? ((FluxWrapper)userFunction).getTarget() : userFunction;

		FunctionType functionType = functionInspector.getRegistration(userFunction).getType();
		this.isConsumer = Void.class.isAssignableFrom(functionType.getOutputType());
		this.isFlux = Flux.class.isAssignableFrom(functionType.getInputWrapper());
		this.inputClass = functionType.getInputType();

		if (functionType.getType() instanceof ParameterizedType) {
			Type[] types = ((ParameterizedType)functionType.getType()).getActualTypeArguments();
			if (this.isFlux && types[0] instanceof ParameterizedType) {
				types[0] = ((ParameterizedType)types[0]).getActualTypeArguments()[0];
				if (!this.isConsumer) {
					types[1] = ((ParameterizedType)types[1]).getActualTypeArguments()[0];
				}
			}
			this.inputType = types[0];
		}
		else {
			this.inputType = functionType.getInputType();
		}

		this.producingMessageHandler = producingMessageHandler;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Flux<Message<?>> apply(Flux<Message<?>> input) {
		if (this.isFlux) {
			if (isConsumer) {
				((Consumer)userFunction).accept(this.messageConversionFlux(input));
			}
			else {
				((Flux) ((Function)userFunction).apply(this.messageConversionFlux(input))).subscribe();
			}
			return input;
		}
		else {
			return input.map(message -> {
				System.out.println("About to unvoke user function");

				if (isConsumer) {
					System.out.println("Invoking CONSUMER");
					((Consumer<Object>)userFunction).accept(this.resolveArgument(message));
				}
				else {
					System.out.println("Invoking FUNCTION");
					Object result = ((Function<Object, Object>)userFunction).apply(this.resolveArgument(message));
					// but we will return the original message??? for the purposes of commit/rollback control
					producingMessageHandler.accept(this.toMessage(result, message));
				}
				return (Message<?>) message;
			});
		}
	}

	private Flux<Object> messageConversionFlux(Flux<Message<?>> input) {
		return input.map(message -> {
			return this.resolveArgument(message);
		});
	}

	private Message<?> toMessage(Object value, Message<?> originalMessage) {
		if (this.messageConverter != null) {
			return this.messageConverter.toMessage(value, originalMessage.getHeaders());
		}
		return MessageBuilder.fromMessage(originalMessage).build();
	}

	private Object resolveArgument(Message<?> message) {
		System.out.println("Converting message: ");
		if (this.messageConverter != null) {
			return this.messageConverter.fromMessage(message, this.inputClass, inputType);
		}
		return message;
	}
}
