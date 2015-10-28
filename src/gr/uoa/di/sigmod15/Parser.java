/*
 * All source files are released under the MIT license. (See http://opensource.org/licenses/MIT)
 * No third-party libraries or code was used.
 */

package gr.uoa.di.sigmod15;

import java.io.DataInputStream;
import java.io.IOException;

public class Parser {
	Actions actions;
	DataInputStream stream;
	short[] columns;
	byte[] word;
	byte[] long_word;
	int offset;

	public Parser(Actions actions, DataInputStream stream) {
		this.stream = stream;
		this.actions = actions;
		this.word = new byte[4];
		this.long_word = new byte[8];
		this.offset = 0;
	}

	public void readNext() throws IOException {
		byte[] b, rlenb = new byte[4];
		int rlen;

		stream.read(rlenb);
		rlen = (rlenb[0] & 0xFF | (rlenb[1] & 0xFF) << 8 | (rlenb[2] & 0xFF) << 16 | (rlenb[3] & 0xFF) << 24) + 4;

		b = new byte[rlen];

		int totalRead = stream.read(b);

		while (totalRead < rlen)
			totalRead += stream.read(b, totalRead, rlen - totalRead);

		int count = b[4 + offset] & 0xFF | (b[5 + offset] & 0xFF) << 8 | (b[6 + offset] & 0xFF) << 16
		        | (b[7 + offset] & 0xFF) << 24;
		int offset = 8;
		columns = new short[count];

		for (int i = 0; i < count; i++) {
			columns[i] = (short) (b[offset] & 0xFF | (b[1 + offset] & 0xFF) << 8 | (b[2 + offset] & 0xFF) << 16
			        | (b[3 + offset] & 0xFF) << 24);
			offset += 4;
		}
		actions.defineSchema(columns);
		
		while (true) {
			stream.read(rlenb);
			rlen = (rlenb[0] & 0xFF | (rlenb[1] & 0xFF) << 8 | (rlenb[2] & 0xFF) << 16 | (rlenb[3] & 0xFF) << 24) + 4;

			b = new byte[rlen];

			totalRead = stream.read(b);

			while (totalRead < rlen)
				totalRead += stream.read(b, totalRead, rlen - totalRead);
			
			switch (b[0]) {
			case 0:
				System.exit(0);
			case 1:
				count = b[4] & 0xFF | (b[5] & 0xFF) << 8 | (b[6] & 0xFF) << 16
				        | (b[7] & 0xFF) << 24;
				offset = 8;
				columns = new short[count];

				for (int i = 0; i < count; i++) {
					columns[i] = (short) (b[offset] & 0xFF | (b[1 + offset] & 0xFF) << 8 | (b[2 + offset] & 0xFF) << 16
					        | (b[3 + offset] & 0xFF) << 24);
					offset += 4;
				}
				actions.defineSchema(columns);
				break;
			case 2:
				actions.transaction(b);
				break;
			case 3:
				ValidationRes vres = new ValidationRes();
				actions.validation(new VResWr(b, vres));
				break;
			case 4:
				actions.flush(b);
				break;
			case 5:
				//actions.forget((int) getLong(b, 4));
				break;
			}
		}
	}
}