import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ReducerQuery4 {

	public static Logger logger = Logger.getLogger(ReducerQuery4.class.getName());

	static {
		logger.setLevel(Level.OFF);
	}

	public void read(String file1, String file2) throws FileNotFoundException, IOException {

		BufferedReader br = null;
		BufferedWriter bw = null;

		try {
			br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
			bw = new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8));

			if (!file1.isEmpty() && !file2.isEmpty()) {
				br = new BufferedReader(new InputStreamReader(new FileInputStream(file1), StandardCharsets.UTF_8));
				bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file2), StandardCharsets.UTF_8));
			}

			String currentLine;
			while ((currentLine = br.readLine()) != null) {
				if (!currentLine.isEmpty()) {
					bw.write(currentLine);
					bw.newLine();
					bw.flush();
				}
			}
		} catch (Exception e) {
			logger.log(Level.SEVERE, null, e);
		} finally {
			bw.close();
			br.close();
		}
	}

	public static void main(String[] args) {
		ReducerQuery4 reduce = new ReducerQuery4();
		try {
			reduce.read("", "");
			// reduce.read("src/tmp/map", "src/tmp/out");
		} catch (IOException e) {
			logger.log(Level.SEVERE, null, e);
		}
	}

}
