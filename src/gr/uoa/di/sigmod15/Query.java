/*
 * All source files are released under the MIT license. (See http://opensource.org/licenses/MIT)
 * No third-party libraries or code was used.
 */

package gr.uoa.di.sigmod15;

public class Query {

	public final int relId;
	public final Condition[] conditions;
	public int columnHashed;

	public Query(int relId, Condition[] conditions) {
		super();
		this.relId = relId;
		this.conditions = conditions;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < conditions.length; i++) {
			sb.append(conditions[i].toString());
			sb.append(' ');
		}
		return sb.toString();
	}
}
