package org.springframework.cloud.stream.function;

import java.lang.reflect.ParameterizedType;
import java.util.function.Consumer;

import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.converter.MessageConverter;

/**
 *
 * @author Oleg Zhurakousky
 *
 */
public class MessagingFunctionInvoker implements FunctionInvoker<Message<?>> {

	private final Consumer<Message<?>> producingMessageHandler;

	private final TypeAware typeAware;

	private MessageConverter messageConverter;

	public MessagingFunctionInvoker(TypeAware typeAware) {
		this(typeAware, null);
	}

	public MessagingFunctionInvoker(TypeAware typeAware, Consumer<Message<?>> producingMessageHandler) {
		this.producingMessageHandler = producingMessageHandler;
		this.typeAware = typeAware;
		this.messageConverter = new MessageConverter() {

			@Override
			public Message<?> toMessage(Object payload, MessageHeaders headers) {
				return MessageBuilder.withPayload(payload).copyHeaders(headers).build();
			}

			@Override
			public Object fromMessage(Message<?> message, Class<?> targetClass) {
				return message;
			}
		};
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void accept(Message<?> message) throws MessagingException {
		if (this.typeAware instanceof TypeAwareFunction) {
			TypeAwareFunction function = (TypeAwareFunction) this.typeAware;
			Object argument = this.messageConverter.fromMessage(message, (Class<?>)((ParameterizedType)function.getInputType()).getRawType());
			Object result = function.apply(argument);
			if (result != null) {
				Message<?> outputMessage = this.messageConverter.toMessage(result, message.getHeaders());
				this.producingMessageHandler.accept(outputMessage);
			}
		}
		else if (this.typeAware instanceof TypeAwareConsumer) {
			TypeAwareConsumer consumer = (TypeAwareConsumer) this.typeAware;
			Object argument = this.messageConverter.fromMessage(message, (Class<?>)((ParameterizedType)consumer.getInputType()).getRawType());
			consumer.accept(argument);
		}
	}
}
