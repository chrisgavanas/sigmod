/*
 * All source files are released under the MIT license. (See http://opensource.org/licenses/MIT)
 * No third-party libraries or code was used.
 */

package gr.uoa.di.sigmod15;

import gr.uoa.di.sigmod15.BaseRelManagerImpl.BaseRelAndRanges;

import java.util.concurrent.PriorityBlockingQueue;

public class ValidOpt {
	protected IndexColumn[] index;
	// private List<QueryWrapper> rest;
	// private StaticList rest;
	PriorityBlockingQueue<QueryWrapper> valStart;
	long minStart;
	long maxEnd;
	public long low;
	public long high;
	public long valoptQueries;
	protected BaseRelAndRanges base;
	public StaticList[] allocated;
	public int curr;
	public int columns;

	public ValidOpt(int columns, BaseRelAndRanges base, StaticList[] lists,
			int sum, boolean isSplit) {
		minStart = 0x7FFFFFFFFFFFFFFFL;
		valStart = new PriorityBlockingQueue<QueryWrapper>(); // first min heap
		index = new IndexColumn[columns];
		// rest = new StaticList(1500);
		this.base = base;
		low = high = -1;
		this.columns = columns;
		allocated = new StaticList[columns * 10];
		if (isSplit) {
			for (int i = 0; i < columns * 10; i++)
				allocated[i] = new StaticList(50);
		} else {
			for (int i = 0; i < columns * 10; i++)
				allocated[i] = lists[sum + i];
		}
	}

	public void clear() {
		// rest.currLength = 0;

		for (IndexColumn i : index)
			if (i != null)
				i.clear();
	}

	public double getAvgKeys() {
		int keys = 0, total = 0;
		for (IndexColumn i : index)
			if (i != null)
				if (!i.keyList.isEmpty()) {
					keys += i.keyList.size();
					total++;
				}
		return total == 0 ? 0 : (double) keys / total;
	}

	public void insert(QueryWrapper wrapper) {
		Condition[] conditions = wrapper.q.conditions;

		/*
		 * switch (conditions[0].op) { case Condition.eq:
		 */
		int pos = pivotColumn(conditions);
		short columnHashed = conditions[pos].column;
		long valueHashed = conditions[pos].constant;
		IndexColumn hashIndex = index[columnHashed];

		if (hashIndex == null) {
			index[columnHashed] = new IndexColumn(columnHashed, this);
			hashIndex = index[columnHashed];
		}

		hashIndex.insert(valueHashed, wrapper);
		/*
		 * return; case Condition.empty: wrapper.valConflict.conflict = 1; return; case Condition.noconflict: return;
		 * default: rest.add(wrapper); }
		 */
	}

	private int maxColumn(Condition[] conditions) {
		int pos = -1, max = -1;

		for (int i = 0; i < conditions.length; i++) {
			Condition cond = conditions[i];
			if (cond.op != Condition.eq)
				return pos;
			if (max < cond.column) {
				max = cond.column;
				pos = i;
			}
			i++;
		}

		return pos;
	}

	private int pivotColumn(Condition[] conditions) {
		int max = -1;
		int pos = -1;

		for (int i = 0; i < conditions.length; i++) {
			Condition cond = conditions[i];
			if (cond.op != Condition.eq)
				break;
			IndexColumn t1 = index[cond.column];
			if (t1 != null) {
				int t2 = t1.keys;
				if (t2 > max) {
					max = t2;
					pos = i;
				}
			}
		}

		return pos == -1 ? maxColumn(conditions) : pos;
	}

	void validate(long[] records, int offset, long currow) {
		// boolean conflict;

		for (IndexColumn iter : index)
			if (iter != null)
				iter.validate(records, offset, currow);
	}

	public void addQuery(QueryWrapper qw) {

		if (low == -1) { // if this is the first query
			low = qw.row;
			high = qw.endrow;
		} else {
			if (qw.row < low) { // if range.low < low < high
				if (qw.endrow < low) // if range.low < range.high < low < high
					low -= qw.endrow - qw.row + 1;
				else if (qw.endrow < high) // if range.low < low < range.high < high
					low = qw.row;
				else { // if range.low < low < high < range.high (superset)
					low = qw.row;
					high = qw.endrow;
				}
			} else if (qw.row < high) { // if low < range.low < high
				if (qw.endrow <= high) { // if low < range.low < range.high < high (subset)

				} else
					// if low < range.low < high < range.high
					high = qw.endrow;
			} else
				// if low < high < range.low < range.high
				high += qw.endrow - qw.row + 1;
		}
		valoptQueries++;
		if (qw.row < minStart)
			minStart = qw.row;
		if (qw.endrow > maxEnd)
			maxEnd = qw.endrow;
		valStart.add(qw);
	}

	public ValidOpt[] split(int splitnum) {
		if (minStart >= maxEnd)
			return null;
		ValidOpt[] splitted = new ValidOpt[splitnum];
		long splitdiff = (maxEnd - minStart) / (splitnum + 1);
		long cursplit = minStart;
		for (int i = 0; i < splitnum; i++) {
			splitted[i] = new ValidOpt(index.length, base, null, 1, true);
			splitted[i].valStart = new PriorityBlockingQueue<QueryWrapper>(valStart);
			splitted[i].minStart = cursplit;
			cursplit += splitdiff;
			splitted[i].maxEnd = cursplit - 1;
		}
		minStart = cursplit;
		return splitted;
	}

	public void flush() {
		long currow = 0;
		QueryWrapper next = valStart.poll();
		if (next == null) {
			clear();
			return;
		}
		while (next != null && next.row < minStart) {
			if (next.endrow >= minStart) {
				insert(next);
			}
			next = valStart.poll();
		}
		currow = minStart;
		int curblock = (int) (currow >> 12);
		int offset = (((int) (currow) & (BaseRelManagerImpl.BLOCKSIZE - 1)) * base.columns);
		long[] records = base.baseRel.get(curblock).records;
		// TODO improve this loop
		while (currow <= maxEnd) {
			while (next != null && next.row == currow) {
				insert(next);
				next = valStart.poll();
			}
			if (next == null) {
				validate(records, offset, currow);
				while (++currow <= maxEnd) {
					offset += base.columns;
					if (offset == base.blockIndexLimit) {
						offset = 0;
						records = base.baseRel.get(++curblock).records;
					}
					validate(records, offset, currow);
				}
				// break;
			} else {
				while (currow < next.row && currow <= maxEnd) {
					validate(records, offset, currow);
					currow++;
					offset += base.columns;
					if (offset == base.blockIndexLimit) {
						offset = 0;
						records = base.baseRel.get(++curblock).records;
					}
				}
			}
		}
		// TODO find why this makes it work
		low = -1;
		valoptQueries = 0;
		minStart = 0x7FFFFFFFFFFFFFFFL;
		maxEnd = 0;
		valStart.clear();
		clear();
	}

}
