/*
 * All source files are released under the MIT license. (See http://opensource.org/licenses/MIT)
 * No third-party libraries or code was used.
 */

package gr.uoa.di.sigmod15;

import gr.uoa.di.sigmod15.BaseRelManagerImpl.BaseRelAndRanges;

public class QueryWrapper implements Comparable<QueryWrapper> {
	public long row;
	public long endrow;
	public Query q;
	public ValidationRes valConflict;

	public QueryWrapper(long row, long endrow, Query q, ValidationRes v) {
		this.row = row;
		this.endrow = endrow;
		this.q = q;
		valConflict = v;
	}
	
	public void execute() {
		if (valConflict.conflict == 1)
			return;
		int curblock = (int) (row >> 12);
		BaseRelAndRanges base = BaseRelManagerImpl.relRanges[q.relId];
		int offset = (((int) (row) & (BaseRelManagerImpl.BLOCKSIZE - 1)) * base.columns);
		long[] recs = base.baseRel.get(curblock).records;
		
		if (doVal(recs, offset)) {
			valConflict.conflict = 1;
			return;
		}
		while (++row <= endrow) {
			offset += base.columns;
			if (offset == base.blockIndexLimit) {
				offset = 0;
				recs = base.baseRel.get(++curblock).records;
			}
			if (doVal(recs, offset)) {
				valConflict.conflict = 1;
				return;
			}
		}
		
	}
	
	private boolean doVal(long[] recs, int offset) {
		for (Condition c : q.conditions) {
			switch (c.op) {
			case Condition.gt:
				if (Long.compareUnsigned(recs[offset + c.column], c.constant) <= 0)
					return false;
				break;
			case Condition.get:
				if (Long.compareUnsigned(recs[offset + c.column], c.constant) < 0)
					return false;
				break;
			case Condition.lt:
				if (Long.compareUnsigned(recs[offset + c.column], c.constant) >= 0)
					return false;
				break;
			case Condition.let:
				if (Long.compareUnsigned(recs[offset + c.column], c.constant) > 0)
					return false;
				break;
			case Condition.neq:
				if (recs[offset + c.column] == c.constant)
					return false;
				break;
			}
		}
		return true;
	}

	@Override
	public String toString() {
		return q.toString();
	}

	@Override
	public boolean equals(Object obj) {
		return ((QueryWrapper) obj).q == this.q;
	}

	@Override
	public int hashCode() {
		return q.hashCode();
	}

	@Override
	public int compareTo(QueryWrapper o) {
		return (row > o.row) ? 1 : -1;
	}
}