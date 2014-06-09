package com.flozano.reactorplayground;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;

import org.junit.Before;
import org.junit.Test;

import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.spec.Reactors;
import reactor.event.Event;
import reactor.event.selector.Selectors;
import reactor.function.Consumer;

public class TestOnlyOneReceived {

	static final int N = 10;
	Reactor r;

	@Before
	public void setUp() {
		r = Reactors.reactor().env(new Environment())
				.dispatcher(Environment.EVENT_LOOP).get();
	}

	@Test
	public void ensureAllAdded() throws InterruptedException {
		Set<Integer> expected = new TreeSet<Integer>();
		for (int i = 0; i < N; i++) {
			expected.add(i);
		}

		TestConsumer c = new TestConsumer();
		r.on(Selectors.object("test"), c);

		for (int i = 0; i < N; i++) {
			r.notify("test", Event.wrap(i));
		}
		c.latch.await();
		assertEquals(0, c.repeatedItems.size());
		assertEquals(expected, c.items);
	}

	public static class TestConsumer implements Consumer<Event<Integer>> {
		final Set<Integer> items = new CopyOnWriteArraySet<Integer>();
		final CountDownLatch latch = new CountDownLatch(N);
		final List<Integer> repeatedItems = new CopyOnWriteArrayList<Integer>();

		public void accept(Event<Integer> arg0) {
			if (!items.add(arg0.getData())) {
				System.err.println("Failed to add " + arg0.getData()
						+ " - repeated value!!!!");
				repeatedItems.add(arg0.getData());
			}
			latch.countDown();
			System.err.println(arg0.getData() + " received, "
					+ latch.getCount() + " to go");
		}
	}
}
