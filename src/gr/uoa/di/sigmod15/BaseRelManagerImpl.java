/*
 * All source files are released under the MIT license. (See http://opensource.org/licenses/MIT)
 * No third-party libraries or code was used.
 */

package gr.uoa.di.sigmod15;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class BaseRelManagerImpl implements BaseRelManager {

	public static final int BLOCKSIZE = 4096;
	public static BaseRelAndRanges[] relRanges;
	private int offset;
	private long id;
	private byte[] b;

	@Override
	public void defineSchema(short[] columns) {
		relRanges = new BaseRelAndRanges[columns.length];
		for (int i = 0; i < columns.length; i++)
			relRanges[i] = new BaseRelAndRanges(columns[i]);
	}

	@Override
	public void transaction(byte[] b) {
		this.b = b;
		id = (long) ((((long) b[11]) << 56) | (((long) b[10] & 0xff) << 48) | (((long) b[9] & 0xff) << 40)
		        | (((long) b[8] & 0xff) << 32) | (((long) b[7] & 0xff) << 24) | (((long) b[6] & 0xff) << 16)
		        | (((long) b[5] & 0xff) << 8) | (((long) b[4] & 0xff)));
		int deletions = b[12] & 0xFF | (b[13] & 0xFF) << 8 | (b[14] & 0xFF) << 16 | (b[15] & 0xFF) << 24;
		int insertions = b[16] & 0xFF | (b[17] & 0xFF) << 8 | (b[18] & 0xFF) << 16 | (b[19] & 0xFF) << 24;
		offset = 20;

		// do deletions
		for (int i = 0; i < deletions; i++) {
			int a = b[0 + offset] & 0xFF | (b[1 + offset] & 0xFF) << 8 | (b[2 + offset] & 0xFF) << 16
			        | (b[3 + offset] & 0xFF) << 24;
			relRanges[a].deleteRecords();
		}

		// do insertions
		for (int i = 0; i < insertions; i++) {
			int a = b[0 + offset] & 0xFF | (b[1 + offset] & 0xFF) << 8 | (b[2 + offset] & 0xFF) << 16
			        | (b[3 + offset] & 0xFF) << 24;
			relRanges[a].addRecords();
		}
	}

	@Override
	public void forget(long trId) {
		// for (BaseRelAndRanges brar : relRanges)
		// brar.ranges = new TreeMap<Long, long[]>(brar.ranges.tailMap(trId + 1));
	}

	@Override
	public long[] getTransactionRange(long trId, long relId, boolean checkDownwards) {
		List<long[]> rn = relRanges[(int) relId].ranges;
		int ns = 0, ne;
		synchronized (rn) {
			 ne = rn.size() - 1;
			while (ne >= ns) {
				int mid = (ns + ne) / 2;
				long[] val = rn.get(mid);
				if (val[0] < trId)
					ns = mid + 1;
				else if (val[0] > trId)
					ne = mid - 1;
				else
					return val;
			}
			if (checkDownwards) {
				if (ne >= 0)
					return rn.get(ne);
				return null;
			}
			if (ns < rn.size())
				return rn.get(ns);
			return null;
		}
		/*
		 * Entry<Long, long[]> entry; synchronized (relRanges[(int) relId].ranges) { if (checkDownwards) entry =
		 * relRanges[(int) relId].ranges.floorEntry(trId); else entry = relRanges[(int)
		 * relId].ranges.ceilingEntry(trId); } return ((entry == null) ? null : entry.getValue());
		 */
	}

	public class RecordBlock {

		public long[] records;

		public RecordBlock(short columns) {
			records = new long[BLOCKSIZE * columns];
		}
	}

	public class BaseRelAndRanges {

		ArrayList<RecordBlock> baseRel; // rowId -> record
		HashMap<LongWrapper, LongWrapper> primaryKeyIndex; // c0 -> rowId
		List<long[]> ranges; // trId -> range
		long[] lastrange;

		short columns; // number of columns for this relation
		long currentRowId; // rowId counter
		long trStartRowId; // transaction starting rowId
		long lastTrId; // last transaction's id that touched this relation
		long[] currentBlock; // points to current Block's records
		int currentBlockIndex; // current Block's index
		int blockIndexLimit; // block's max index

		public BaseRelAndRanges(short columns) {
			baseRel = new ArrayList<RecordBlock>();
			primaryKeyIndex = new HashMap<LongWrapper, LongWrapper>();
			ranges = new ArrayList<long[]>(100);
			currentRowId = 0;
			this.columns = columns;
			currentBlockIndex = blockIndexLimit = columns << 12;
			lastTrId = -1;
		}

		public void addRecords() {
			offset += 4;
			int rows = b[0 + offset] & 0xFF | (b[1 + offset] & 0xFF) << 8 | (b[2 + offset] & 0xFF) << 16
			        | (b[3 + offset] & 0xFF) << 24; // get number of records to be inserted
			offset += 4;
			if (lastTrId != id)
				trStartRowId = currentRowId;
			for (int k = 0; k < rows; ++k) { // for every record
				if (currentBlockIndex == blockIndexLimit) { // if block size is exceeded
					RecordBlock newBlock = new RecordBlock(columns); // create new block
					baseRel.add(newBlock); // append it
					currentBlock = newBlock.records; // point to new block's records
					currentBlockIndex = 0; // reset block index counter
				}
				long c0 = ((((long) b[7 + offset]) << 56) | (((long) b[6 + offset] & 0xff) << 48)
				        | (((long) b[5 + offset] & 0xff) << 40) | (((long) b[4 + offset] & 0xff) << 32)
				        | (((long) b[3 + offset] & 0xff) << 24) | (((long) b[2 + offset] & 0xff) << 16)
				        | (((long) b[1 + offset] & 0xff) << 8) | (((long) b[0 + offset] & 0xff)));
				offset += 8;
				LongWrapper c0L = new LongWrapper(c0);
				primaryKeyIndex.put(c0L, new LongWrapper(currentRowId)); // renew c0 index
				currentBlock[currentBlockIndex++] = c0; // add c0 to block
				for (int j = 1; j < columns; j++) { // for every other column of this record
					currentBlock[currentBlockIndex++] = ((((long) b[7 + offset]) << 56)
					        | (((long) b[6 + offset] & 0xff) << 48) | (((long) b[5 + offset] & 0xff) << 40)
					        | (((long) b[4 + offset] & 0xff) << 32) | (((long) b[3 + offset] & 0xff) << 24)
					        | (((long) b[2 + offset] & 0xff) << 16) | (((long) b[1 + offset] & 0xff) << 8) | (((long) b[0 + offset] & 0xff)));
					offset += 8;
				}
				currentRowId++;
			}
			synchronized (ranges) {
				if (lastrange != null && lastrange[0] == id) {
					lastrange[2] = currentRowId - 1;
				} else {
					lastrange = new long[] { id, trStartRowId, currentRowId - 1 };
					ranges.add(lastrange); // insert transaction's range
				}
			}
			lastTrId = id; // flag last trId
		}

		public void deleteRecords() {
			offset += 4;
			int rows = b[0 + offset] & 0xFF | (b[1 + offset] & 0xFF) << 8 | (b[2 + offset] & 0xFF) << 16
			        | (b[3 + offset] & 0xFF) << 24;
			offset += 4;
			boolean deleted = false; // mark deletion as failure
			if (lastTrId != id)
				trStartRowId = currentRowId;

			for (int i = 0; i < rows; ++i) {
				LongWrapper key = new LongWrapper(((((long) b[7 + offset]) << 56) | (((long) b[6 + offset] & 0xff) << 48)
				        | (((long) b[5 + offset] & 0xff) << 40) | (((long) b[4 + offset] & 0xff) << 32)
				        | (((long) b[3 + offset] & 0xff) << 24) | (((long) b[2 + offset] & 0xff) << 16)
				        | (((long) b[1 + offset] & 0xff) << 8) | (((long) b[0 + offset] & 0xff))));
				offset += 8;
				LongWrapper target = primaryKeyIndex.get(key); // get rowId of record
				if (target != null) { // if it exists
					deleted = true; // mark deletion as a success
					addDeletedRecord(target.val); // append record to base relation
					primaryKeyIndex.remove(key);
				}
			}
			if (deleted) {
				synchronized (ranges) {// if at least one deletion was a success
					if (lastrange != null && lastrange[0] == id) {
						lastrange[1] = trStartRowId;
						lastrange[2] = currentRowId - 1;
					} else {
						lastrange = new long[] { id, trStartRowId, currentRowId - 1 };
						ranges.add(lastrange); // insert transaction's range
					}
				}
				lastTrId = id; // flag last trId
			}
		}

		public void addDeletedRecord(long rowId) {
			if (currentBlockIndex == blockIndexLimit) { // if block size is exceeded
				RecordBlock newBlock = new RecordBlock(columns); // create new block
				baseRel.add(newBlock); // append it
				currentBlock = newBlock.records; // point to new block's records
				currentBlockIndex = 0; // reset block index counter
			}
			long[] from = baseRel.get(((int) rowId) >> 12).records; // get corresponding block's records
			int offset = (int) ((rowId) % BLOCKSIZE) * columns; // get starting offset
			for (int i = 0; i < columns; i++)
				// copy all columns
				currentBlock[currentBlockIndex++] = from[offset++];
			currentRowId++;
		}
	}
}