/*
 * All source files are released under the MIT license. (See http://opensource.org/licenses/MIT)
 * No third-party libraries or code was used.
 */

package gr.uoa.di.sigmod15;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

public class ParserWorker implements Actions {

	private final int M = 6;

	private BaseRelManagerImpl manager;
	private ValidationStrats validator;

	private LinkedBlockingQueue<VResWr> nextAction; // true -> flush(), false -> done()
	private LinkedBlockingQueue<ValidOpt> flush;
	private BlockingQueue<QueryWrapper> rest;
	private Semaphore sem;
	private Pair workLoad[];
	private int relnum;

	private CyclicBarrier barrier;

	// private long start;

	public ParserWorker() {
		// start = System.currentTimeMillis();
	}

	@Override
	public void defineSchema(short[] columnCounts) {

		manager = new BaseRelManagerImpl();
		manager.defineSchema(columnCounts);
		nextAction = new LinkedBlockingQueue<VResWr>();
		flush = new LinkedBlockingQueue<ValidOpt>();
		rest = new LinkedBlockingQueue<QueryWrapper>();
		validator = new ValidationStrats(columnCounts, manager, rest);
		sem = new Semaphore(0);
		relnum = columnCounts.length;
		barrier = new CyclicBarrier(M + 1);
		workLoad = new Pair[relnum];
		for (int i = 0; i < relnum; i++)
			workLoad[i] = new Pair();
		for (int i = 0; i < M; i++)
			new Thread(new ValWorker(nextAction, flush, sem, validator, barrier, rest)).start();

	}

	@Override
	public void transaction(byte[] b) {
		manager.transaction(b);
	}

	@Override
	public void validation(VResWr vr) {
		validator.validations.add(vr.res);
		nextAction.add(vr);
	}

	@Override
	public void flush(byte[] b) {
		QueryWrapper qr = null;
		
		for (int i = 0; i < M; i++)
			// wake up all ValWorkers
			nextAction.add(new VResWr(null, null));

		while (barrier.getNumberWaiting() != M) {
			for (int l = 0; l < 10 && (qr = rest.poll()) != null; l++)
				qr.execute();
			if (qr == null) {
				while (barrier.getNumberWaiting() != M);
				break;
			}
		}

		int k = 0;
		long avgComplexity = 0;
		for (Pair pair : workLoad) {
			pair.pos = k;
			ValidOpt valopt = validator.valopts[k++];
			avgComplexity += pair.complexity = valopt.high - valopt.low;
		}
		avgComplexity /= (relnum / 2);
		quickSort(0, relnum - 1);
		// System.err.print("Sorted: ");
		for (Pair pair : workLoad) {
			if (pair.complexity > avgComplexity) {
				ValidOpt[] splitted = validator.valopts[pair.pos].split((int) (pair.complexity / avgComplexity));
				if (splitted != null)
					for (ValidOpt sp : splitted)
						flush.add(sp);
			}
			flush.add(validator.valopts[pair.pos]);
		}

		// System.err.println();

		/*
		 * ValidOpt v; while ((v = flush.poll()) != null) { // do some validation work v.flush(); }
		 */

		try {
			barrier.await();
			
			while ((qr = rest.poll()) != null)
				qr.execute();
			
			sem.acquire(M); // wait for all ValWorkers to finish
		} catch (InterruptedException | BrokenBarrierException e) {
			e.printStackTrace();
		}
		barrier.reset();
		// System.err.println("--------------------------------------------------------------------");
		for (ValidationRes res : validator.validations);
			// print results
			//System.out.write(res.conflict == 1 ? '1' : '0');
		System.out.flush();
		validator.validations.clear();

	}

	@Override
	public void forget(int trId) {
		// manager.forget(trId);
	}

	@Override
	public void done() {
		// System.out.println("");
		// System.out.println(System.currentTimeMillis() - start);
		System.exit(0);
	}

	private void quickSort(int low, int high) {
		int i = low, j = high;
		long pivot = workLoad[low + (high - low) / 2].complexity;
		while (i <= j) {
			while (workLoad[i].complexity > pivot)
				i++;
			while (workLoad[j].complexity < pivot)
				j--;
			if (i <= j) {
				Pair temp = workLoad[i];
				workLoad[i++] = workLoad[j];
				workLoad[j--] = temp;
			}
		}
		if (low <= j)
			quickSort(low, j);
		if (i < high)
			quickSort(i, high);
	}

	private class Pair {
		public int pos;
		public long complexity;
	}

}