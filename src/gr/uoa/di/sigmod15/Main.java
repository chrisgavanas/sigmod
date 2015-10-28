/*
 * All source files are released under the MIT license. (See http://opensource.org/licenses/MIT)
 * No third-party libraries or code was used.
 */

package gr.uoa.di.sigmod15;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;

public class Main {

	public static void main(String[] args) {
		try {
			Parser p;
			p = new Parser(new ParserWorker(), new DataInputStream(new FileInputStream("medium2.test")));
			p.readNext();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}