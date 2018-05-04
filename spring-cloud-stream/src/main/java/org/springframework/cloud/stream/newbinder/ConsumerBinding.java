package org.springframework.cloud.stream.newbinder;

import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.context.Lifecycle;

/**
 *
 * @author Oleg Zhurakousky
 *
 * @param <F> - the type of raw Input of a function. For example Message or Flux etc.
 */
class ConsumerBinding<F> extends AbstractBinding<Lifecycle> {

	protected ConsumerDestination destination;

	private ProducerBinding<F> producerBinding;

	public ConsumerBinding(ConsumerDestination destination, Lifecycle boundComponent) {
		super(boundComponent);
		this.destination = destination;
	}

	public String getName() {
		return this.destination.getName() + "_binding";
	}

	public ProducerBinding<F> getProducerBinding() {
		return producerBinding;
	}

	public void setProducerBinding(ProducerBinding<F> producerBinding) {
		this.producerBinding = producerBinding;
	}
}
