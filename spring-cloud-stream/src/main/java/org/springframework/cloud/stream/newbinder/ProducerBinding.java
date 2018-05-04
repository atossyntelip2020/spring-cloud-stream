package org.springframework.cloud.stream.newbinder;

import org.springframework.cloud.stream.provisioning.ProducerDestination;

/**
 *
 * @author Oleg Zhurakousky
 *
 * @param <F> - the type of raw Input of a function. For example Message or Flux etc.
 */
class ProducerBinding<T> extends AbstractBinding<T> {

	protected final ProducerDestination destination;

	public ProducerBinding(ProducerDestination destination, T boundComponent) {
		super(boundComponent);
		this.destination = destination;
	}

	public String getName() {
		return this.destination.getName() + "_binding";
	}
}
