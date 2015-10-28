/*
 * All source files are released under the MIT license. (See http://opensource.org/licenses/MIT)
 * No third-party libraries or code was used.
 */

package gr.uoa.di.sigmod15;

public class LongWrapper {
	public final long val;
	
	LongWrapper(Long val) {
		this.val = val;
	}
	
	LongWrapper(long val) {
		this.val = val;
	}

	@Override
	public int hashCode() {
		//return (int) val;
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (val ^ (val >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		LongWrapper other = (LongWrapper) obj;
		return val == other.val;
	}

}
