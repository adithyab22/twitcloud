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

public class ReducerQuery5 {

	public static Logger logger = Logger.getLogger(ReducerQuery5.class.getName());

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

			Long currentId = 0L;
			int currentSum = 0;
			String currentLine;
			Long id;
			int count;
			while ((currentLine = br.readLine()) != null) {
				if (!currentLine.isEmpty()) {

					String[] splits = currentLine.split("\t", 2);
					id = Long.parseLong(splits[0]);
					count = Integer.parseInt(splits[1]);

					if (!currentId.equals(id)) {
						if (currentId != 0L) {
							bw.write(currentId + "\t" + currentSum);
							bw.newLine();
						}
						currentId = id;
						currentSum = count;
					} else {
						currentSum += count;
					}
				}
			}
			
			bw.write(currentId + "\t" + currentSum);
			bw.newLine();
			bw.flush();

		} catch (Exception e) {
			logger.log(Level.SEVERE, null, e);
		} finally {
			bw.close();
			br.close();
		}
	}

	public static void main(String[] args) {
		ReducerQuery5 reduce = new ReducerQuery5();
		try {
			reduce.read("", "");
			// reduce.read("src/tmp/map", "src/tmp/out");
		} catch (IOException e) {
			logger.log(Level.SEVERE, null, e);
		}
	}

}
