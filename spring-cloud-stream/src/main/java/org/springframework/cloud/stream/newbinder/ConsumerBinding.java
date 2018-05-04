package org.springframework.cloud.stream.newbinder;

import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.context.Lifecycle;

/**
 *
 * @author Oleg Zhurakousky
 *
 * @param <F> - the type of raw Input of a function. For example Message or Flux etc.
 */
class ConsumerBinding<T> extends AbstractBinding<T> {

	protected ConsumerDestination destination;

	private ProducerBinding<T> producerBinding;

	public ConsumerBinding(ConsumerDestination destination, T boundComponent) {
		super(boundComponent);
		this.destination = destination;
	}

	public String getName() {
		return this.destination.getName() + "_binding";
	}

	public ProducerBinding<T> getProducerBinding() {
		return producerBinding;
	}

	public void setProducerBinding(ProducerBinding<T> producerBinding) {
		this.producerBinding = producerBinding;
	}
}
