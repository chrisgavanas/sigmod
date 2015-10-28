/*
 * All source files are released under the MIT license. (See http://opensource.org/licenses/MIT)
 * No third-party libraries or code was used.
 */

package gr.uoa.di.sigmod15;

public class Condition {

	public final short column;
	public final int op;
	public final long constant;

	public Condition(short column, int op, long constant) {
		super();
		this.column = column;
		this.op = op;
		this.constant = constant;
	}

	public static final int eq = 0;
	public static final int neq = 1;
	public static final int lt = 2;
	public static final int let = 3;
	public static final int gt = 4;
	public static final int get = 5;
	public static final int empty = 6;
	public static final int noconflict = 7;
	
	@Override
	public String toString() {
		String opn = null;
		switch (op) {
		case empty:
			opn = "empty";
			break;
		case eq:
			opn = "=";
			break;
		case get:
			opn = ">=";
			break;
		case gt:
			opn = ">";
			break;
		case let:
			opn = "<=";
			break;
		case lt:
			opn = "<";
			break;
		case neq:
			opn = "!=";
			break;
		default:
			break;
		
		}
		return "c"+column+opn+constant;
	}
	
}
