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

public class ValWorker implements Runnable {

	LinkedBlockingQueue<VResWr> nextAction;
	LinkedBlockingQueue<ValidOpt> flush;
	Semaphore sem;
	private ValidationStrats validator;
	private CyclicBarrier barrier;
	private BlockingQueue<QueryWrapper> rest;

	public ValWorker(LinkedBlockingQueue<VResWr> nextAction, LinkedBlockingQueue<ValidOpt> flush, Semaphore sem,
	        ValidationStrats val, CyclicBarrier barrier, BlockingQueue<QueryWrapper> rest) {
		this.nextAction = nextAction;
		this.flush = flush;
		this.sem = sem;
		this.barrier = barrier;
		validator = val;
		this.rest = rest;
	}

	@Override
	public void run() {
		VResWr vr;
		byte[] b;
		while (true) {
			while ((vr = nextAction.poll()) == null) {// block until next action is given
				QueryWrapper rq = null;
				for (int l = 0; l < 20 && (rq = rest.poll()) != null; l++)
					rq.execute();
				if (rq == null) {
					try {
	                    vr = nextAction.take();
                    } catch (InterruptedException e) {
	                    // TODO Auto-generated catch block
	                    e.printStackTrace();
                    }
					break;
				}
			}
			if (vr.res != null) {
				b = vr.b;
				int offset = 4;
				long validationId = getLong(b, offset);
				long fromTr = getLong(b, offset + 8);
				long toTr = getLong(b, offset + 16);
				long queriesNo = getInt(b, offset + 24);
				offset += 28;
				Query[] queries = new Query[(int) queriesNo];

				for (int i = 0; i != queriesNo; ++i) {
					int relationId = getInt(b, offset);
					int count = getInt(b, offset + 4);
					offset += 8;
					Condition[] conditions = new Condition[count == 0 ? 1 : count];

					if (count == 0)
						conditions[count] = new Condition((short) 0, Condition.empty, 0);
					else {
						boolean possibleconds = true;
						int js = 0;
						int je = count - 1;
						condloop: for (int j = 0; j < count && possibleconds; ++j) {
							int column = getInt(b, offset);
							int operation = getInt(b, offset + 4);
							long condition = getLong(b, offset + 8);
							offset += 16;

							if (operation == 0) {
								for (int c = 0; c < js; c++)
									if (conditions[c].column == column && conditions[c].constant != condition) {
										conditions = new Condition[1];
										conditions[0] = new Condition((short) 0, Condition.noconflict, 0);
										offset += 16 * (count - j - 1);
										break condloop;
									}
								conditions[js++] = new Condition((short) column, Condition.eq, condition);
							} else
								conditions[je--] = new Condition((short) column, operation, condition);
						}
					}
					queries[i] = new Query(relationId, conditions);
				}
				validator.validation(validationId, fromTr, toTr, queries, vr.res);
			} else
				validate(); // validate
		}
	}

	private void validate() {
		ValidOpt v;
		// long stime = System.currentTimeMillis();
		// int count = 0;
		try {
			barrier.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		while ((v = flush.poll()) != null) {
			v.flush();
			// count++;
		}
		// long etime = System.currentTimeMillis();
		// System.err.println(Thread.currentThread().getName() + " running from " + stime + " to " + etime + " (" +
		// (etime-stime) + "), vopts: " + count);
		sem.release();
		
		QueryWrapper rq;
		while ((rq = rest.poll()) != null)
			rq.execute();
	}

	private int getInt(byte[] b, int offset) {
		return b[0 + offset] & 0xFF | (b[1 + offset] & 0xFF) << 8 | (b[2 + offset] & 0xFF) << 16
		        | (b[3 + offset] & 0xFF) << 24;
	}

	private long getLong(byte[] b, int offset) {
		return ((((long) b[7 + offset]) << 56) | (((long) b[6 + offset] & 0xff) << 48)
		        | (((long) b[5 + offset] & 0xff) << 40) | (((long) b[4 + offset] & 0xff) << 32)
		        | (((long) b[3 + offset] & 0xff) << 24) | (((long) b[2 + offset] & 0xff) << 16)
		        | (((long) b[1 + offset] & 0xff) << 8) | (((long) b[0 + offset] & 0xff)));
	}
}
