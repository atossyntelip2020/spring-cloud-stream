package org.springframework.cloud.stream.binder;

import org.reactivestreams.Publisher;
import org.springframework.messaging.Message;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;

public interface FunctionalBinding<I, O> {

	Publisher<I> inputPublisher();

	Mono<Void> outputPublisher(Publisher<O> outputPublisher);

	void onError(Throwable t);

	String getName();

	Disposable subscribe();
}
