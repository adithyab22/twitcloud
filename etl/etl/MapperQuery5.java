import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * 
 * @author Nitish
 *
 */
public class MapperQuery5 {

	public static Logger logger = Logger.getLogger(MapperQuery5.class.getName());

	private static SimpleDateFormat utcDf = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
	private static SimpleDateFormat mySQLDf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private static final String KEY_USER = "user";
	private static final String KEY_USER_ID = "id";
	private static final Integer ONE = 1;

	private static final char OUTPUT_DELIM = '\t';

	static {

		logger.setLevel(Level.OFF);

		mySQLDf.setTimeZone(TimeZone.getTimeZone("UTC"));
		utcDf.setTimeZone(TimeZone.getTimeZone("UTC"));
	}

	/**
	 * reads the file and filters it according to the required format
	 *
	 * @param filename
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
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

			String currentLine = "";
			JSONParser parser = new JSONParser();
			JSONObject jsonObject = null;
			JSONObject userObject = null;
			Long userId = null;
			StringBuilder finalText = new StringBuilder();
			while ((currentLine = br.readLine()) != null) {
				try {
					jsonObject = (JSONObject) parser.parse(currentLine);
					userObject = (JSONObject) jsonObject.get(KEY_USER);
					userId = null;

					if (userObject != null && userObject.get(KEY_USER_ID) != null) {
						userId = (Long) userObject.get(KEY_USER_ID);
					}

					// Ignore tweets with null fields required for us
					if (userId != null) {

						// Write tsv output
						finalText.append(userId).append(OUTPUT_DELIM).append(ONE);
						bw.write(finalText.toString());
						finalText = finalText.delete(0, finalText.length());
						bw.newLine();
					}
				} catch (ParseException e) {
					logger.log(Level.SEVERE, null, e);
				}

				bw.flush();
			}

		} catch (Exception e) {
			logger.log(Level.SEVERE, null, e);
		} finally {
			br.close();
			bw.close();
		}
	}

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			MapperQuery5 map = new MapperQuery5();

			map.read("", "");
			// map.read("../part-100k", "src/map");

		} catch (IOException ex) {
		}
	}

}
