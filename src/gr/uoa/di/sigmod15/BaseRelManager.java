/*
 * All source files are released under the MIT license. (See http://opensource.org/licenses/MIT)
 * No third-party libraries or code was used.
 */

package gr.uoa.di.sigmod15;

public interface BaseRelManager {

	/**
	 * Gets called from Actions implementation
	 * @param columns
	 */
	void defineSchema(short[] columns);
	
	/**
	 * Gets called from Actions implementation
	 * @param id
	 * @param deleteops
	 * @param insertions
	 */
	void transaction(byte[] b);

	/**
	 * Gets called from Actions implementation
	 * @param trId
	 */
	void forget(long trId);
	
	/**
	 * Gets called from Validation Strategy implementation to get a trId's range in relId.
	 * Warning: if range of trId in relId is null, last trId's range in relId is returned.
	 * @param trId
	 * @param relId
	 * @return 
	 * 
	 */
	long[] getTransactionRange(long trId, long relId, boolean checkDownwards); 
	
}

