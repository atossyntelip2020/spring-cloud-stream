package org.springframework.cloud.stream.newbinder;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.context.Lifecycle;

/**
 *
 * @author Oleg Zhurakousky
 *
 * @param <T> - the type of the root component of this binding (i.e., Lifecycle, Consumer, etc)
 */
abstract class AbstractBinding<T> implements Binding<T>{

	private T boundComponent;

	public AbstractBinding(T boundComponent) {
		this.boundComponent = boundComponent;
	}

	@Override
	public void unbind() {

	}

	@Override
	public void start() {
		if (this.boundComponent instanceof InitializingBean) {
			try {
				((InitializingBean)this.boundComponent).afterPropertiesSet();
			}
			catch (Exception e) {
				throw new IllegalStateException("Failed to initialize " + this.boundComponent, e);
			}
		}
		if (this.boundComponent instanceof Lifecycle) {
			((Lifecycle)this.boundComponent).start();
		}
	}
}
