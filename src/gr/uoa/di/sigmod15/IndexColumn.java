/*
 * All source files are released under the MIT license. (See http://opensource.org/licenses/MIT)
 * No third-party libraries or code was used.
 */

package gr.uoa.di.sigmod15;

import java.util.Map;
import java.util.HashMap;

public class IndexColumn {
	public Map<LongWrapper, StaticList> keyList;
	public int columnHashed;
	public ValidOpt parent;
	public int keys;

	public IndexColumn(int columnHashed, ValidOpt parent) {
		keyList = new HashMap<LongWrapper, StaticList>();
		this.columnHashed = columnHashed;
		this.parent = parent;
	}

	public void clear() {
		keyList.clear();
		keys = 0;
		for (StaticList list : parent.allocated)
			list.currLength = 0;

		parent.curr = 0;
	}

	public void insert(Long value, QueryWrapper wrapper) {
		StaticList list = keyList.get(new LongWrapper(value));

		if (list == null) {
			if (parent.curr == parent.columns * 10)
				list = new StaticList(50);
			else
				list = parent.allocated[parent.curr++];

			keyList.put(new LongWrapper(value), list);
			keys++;
		}

		list.add(wrapper);
	}

	public void validate(long[] records, int offset, long currow) {
		boolean conflict;
		StaticList hashed = keyList.get(new LongWrapper(records[offset + columnHashed]));

		if (hashed == null)
			return;

		for (int i = 0; i < hashed.currLength; i++) {
			QueryWrapper wrapper = hashed.list[i];
			if (wrapper == null) {
				continue;
			}
			if (currow > wrapper.endrow || wrapper.valConflict.conflict == 1) {
				hashed.remove(i);
				continue;
			}
			conflict = true;
			icloop:
			for (Condition cond : wrapper.q.conditions) {
				switch (cond.op) {
				case Condition.eq:
					if (cond.constant != records[offset + cond.column]) {
						conflict = false;
						break icloop;
					}
					break;
				case Condition.neq:
					if (cond.constant == records[offset + cond.column]) {
						conflict = false;
						break icloop;
					}
					break;
				case Condition.gt:
					if (Long.compareUnsigned(cond.constant, records[offset + cond.column]) >= 0) {
						conflict = false;
						break icloop;
					}
					break;
				case Condition.get:
					if (Long.compareUnsigned(cond.constant, records[offset + cond.column]) > 0) {
						conflict = false;
						break icloop;
					}
					break;
				case Condition.lt:
					if (Long.compareUnsigned(cond.constant, records[offset + cond.column]) <= 0) {
						conflict = false;
						break icloop;
					}
					break;
				case Condition.let:
					if (Long.compareUnsigned(cond.constant, records[offset + cond.column]) < 0) {
						conflict = false;
						break icloop;
					}
					break;
				default:
					break; // Empty queries are validated as true or false?
				}
			}
			if (conflict == true) { // If all conditions matched
				wrapper.valConflict.conflict = 1;
				hashed.remove(i);
			}
		}
	}
}
