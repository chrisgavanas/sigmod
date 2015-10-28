/*
 * All source files are released under the MIT license. (See http://opensource.org/licenses/MIT)
 * No third-party libraries or code was used.
 */

package gr.uoa.di.sigmod15;

public interface Actions {

	void defineSchema(short[] columnCounts);
	
	void transaction(byte[] b);
	
	void validation(VResWr vr);
	
	void flush(byte[] b);
	
	void forget(int trId);
	
	void done();
}
