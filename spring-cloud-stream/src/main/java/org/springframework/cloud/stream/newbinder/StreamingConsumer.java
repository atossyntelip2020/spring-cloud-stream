package org.springframework.cloud.stream.newbinder;

import java.time.Duration;
import java.util.function.Consumer;

import org.springframework.cloud.stream.function.TypeAwareFunction;
import org.springframework.context.Lifecycle;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

import reactor.core.publisher.Flux;

/**
 *
 * @author Oleg Zhurakousky
 *
 */
public class StreamingConsumer implements Consumer<Message<?>> {

	private Consumer<Message<?>> fluxEmmitingConsumer;

	private final Lifecycle listeningContainer;

	private final int batchSize;

	private final TypeAwareFunction messageHandler;

	public StreamingConsumer(TypeAwareFunction messageHandler, Lifecycle listeningContainer) {
		this(messageHandler, listeningContainer, 1);
	}

	public StreamingConsumer(TypeAwareFunction messageHandler, Lifecycle listeningContainer, int batchSize) {
		this.batchSize = batchSize;
		this.listeningContainer = listeningContainer;
		this.messageHandler = messageHandler;
		this.buildFlux();
	}


	@Override
	public void accept(Message<?> message) {
		System.out.println();
		/*
		 * This is where we delegate to Flux Publisher
		 */
//		fluxEmmitingConsumer.accept(message);
	}

	protected void onCancel() {
		System.out.println("STOPPING CONTAINER");
		this.listeningContainer.stop();
	}

	protected void doOnCommit(Message<?> message) {
		// noop
	}

	private void onCommit(Message<?> message) {
		if (!(message instanceof EmptyMessage)) {
			this.doOnCommit(message);
		}
	}

	private void buildFlux() {
		Flux.<Message<?>>create(emitter -> {
			fluxEmmitingConsumer = new Consumer<Message<?>>() {
				@Override
				public void accept(Message<?> t) {
					emitter.next(t);
				}
			};
		}) // Producer Flux
//		.window(this.txWindowSize)
		.windowTimeout(this.batchSize, Duration.ofSeconds(60)) // batch size , rather then transaction
		.<Message<?>>concatMap(window -> messageHandler.apply(window.cache())
				.doOnNext(result -> {
					System.out.println("RESULT: " + result);
//					functionDelegatingWrapper.getProducingMessageHandler().accept(result);
					}) // this is where you send the result downstream
				.doOnCancel(() -> onCancel())
				.log()
				.retryBackoff(6, Duration.ofSeconds(1)) // available since 3.2
				.last(new EmptyMessage()).doOnNext(element -> onCommit((Message<?>) element)))
		.subscribe();
	}

	private static class EmptyMessage implements Message<Object> {
		@Override
		public Object getPayload() {
			throw new UnsupportedOperationException("This is a signal message");
		}

		@Override
		public MessageHeaders getHeaders() {
			throw new UnsupportedOperationException("This is a signal message");
		}
	}
}
