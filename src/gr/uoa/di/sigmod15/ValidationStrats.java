/*
 * All source files are released under the MIT license. (See http://opensource.org/licenses/MIT)
 * No third-party libraries or code was used.
 */

package gr.uoa.di.sigmod15;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class ValidationStrats {
	public ValidOpt[] valopts;
	public List<ValidationRes> validations;
	BaseRelManagerImpl manager;
	int relnum;
	private BlockingQueue<QueryWrapper> rest;

	@SuppressWarnings("static-access")
    public ValidationStrats(short[] columnCounts, BaseRelManagerImpl manager, BlockingQueue<QueryWrapper> rest) {
		int sum = 0;
		for (short c : columnCounts)
			sum += c * 10;

		final StaticList[] lists = new StaticList[sum];
		for (int i = 0; i < sum; i++)
			lists[i] = new StaticList(50);
		valopts = new ValidOpt[columnCounts.length];
		sum = 0;
		for (int i = 0; i < columnCounts.length; i++) {
			valopts[i] = new ValidOpt(columnCounts[i], manager.relRanges[i], lists, sum, false);
			sum += columnCounts[i] * 10;
		}
		validations = new LinkedList<ValidationRes>();
		relnum = columnCounts.length;
		this.manager = manager;
		this.rest = rest;
	}

	public void validation(long id, long fromTr, long toTr, Query[] queries, ValidationRes res) {
		valbreak: for (Query query : queries) {
			long[] rows;
			rows = manager.getTransactionRange(fromTr, query.relId, false);
			if (rows == null)
				continue;
			long fromRow = rows[1];
			rows = manager.getTransactionRange(toTr, query.relId, true);
			if (rows == null)
				continue;
			long toRow = rows[2];
			if (toRow < fromRow)
				continue;
			switch (query.conditions[0].op) {
			case Condition.eq:
				valopts[query.relId].addQuery(new QueryWrapper(fromRow, toRow, query, res));
				break;
			case Condition.noconflict:
				break;
			case Condition.empty:
				res.conflict = 1;
				break valbreak;
			default:
				//new QueryWrapper(fromRow, toRow, query, res).execute();
				rest.add(new QueryWrapper(fromRow, toRow, query, res));
			}
		}
	}
}
