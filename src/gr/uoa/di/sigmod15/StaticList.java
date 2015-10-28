/*
 * All source files are released under the MIT license. (See http://opensource.org/licenses/MIT)
 * No third-party libraries or code was used.
 */

package gr.uoa.di.sigmod15;

import java.util.Arrays;

public class StaticList {
	public QueryWrapper[] list;
	public int length;
	public int currLength;

	public StaticList(int length) {
		this.length = length;
		this.list = new QueryWrapper[length];
		this.currLength = 0;
	}

	public void add(QueryWrapper wrapper) {
		if (currLength == length) {
			length <<= 1;
			list = Arrays.copyOf(list, length);
		}
		
		list[currLength++] = wrapper;
	}

	public void remove(int index) {
		list[index] = list[0];
		list[0] = null;
	}
	
	public void clear() {
		currLength = 0;
	}
}
