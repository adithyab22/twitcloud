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
import java.util.Date;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringEscapeUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * 
 * @author Nitish
 *
 */
public class MapperQuery3 {

	public static Logger logger = Logger.getLogger(MapperQuery3.class.getName());

	private static SimpleDateFormat utcDf = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
	private static SimpleDateFormat mySQLDf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private static final String KEY_ID_STR = "id_str";
	private static final String KEY_CREATED_AT = "created_at";
	private static final String KEY_TEXT = "text";
	private static final String KEY_USER = "user";
	private static final String KEY_USER_ID = "id";
	private static final String KEY_USER_FOLLOWERS = "followers_count";

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
			String id = "";
			String createdAt = "";
			String text = "";
			JSONObject jsonObject = null;
			JSONObject userObject = null;
			Long userId = null;
			Long followerCount = null;
			StringBuilder finalText = new StringBuilder();
			while ((currentLine = br.readLine()) != null) {
				try {
					jsonObject = (JSONObject) parser.parse(currentLine);
					id = (String) jsonObject.get(KEY_ID_STR);
					createdAt = (String) jsonObject.get(KEY_CREATED_AT);
					text = (String) jsonObject.get(KEY_TEXT);
					userObject = (JSONObject) jsonObject.get(KEY_USER);
					userId = null;

					if (userObject != null && userObject.get(KEY_USER_ID) != null) {
						userId = (Long) userObject.get(KEY_USER_ID);
					}

					followerCount = null;

					if (userObject != null && userObject.get(KEY_USER_FOLLOWERS) != null) {
						followerCount = (Long) userObject.get(KEY_USER_FOLLOWERS);
					}

					// Ignore tweets with null fields required for us
					if (id != null && createdAt != null && text != null && userId != null) {

						id = id.trim();
						createdAt = createdAt.trim();
						String date = getDateForMySQL(createdAt);

						// Escape special characters to write them as text
						// Escaping twice, because SQL doesn't recognize "\\u"
						// as special character
						// and hence ignores the first '/'
						text = StringEscapeUtils.escapeJava(text);
						text = StringEscapeUtils.escapeJava(text);

						// Check for tweet date condition
						// Check if ids are valid numbers
						if (date != null && isNumber(id) && isValidText(text)) {

							// calculate sentiment score
							int score = ScoreCalculatorAndCensor.calculateSentimentScore(text);

							// censor bad words in text
							text = ScoreCalculatorAndCensor.censorText(text);

							if (score != 0) {
								// Write tsv output
								finalText
										.append(id).append(OUTPUT_DELIM)
										.append(userId).append(OUTPUT_DELIM)
										.append(date).append(OUTPUT_DELIM)
										.append(score).append(OUTPUT_DELIM)
										.append(text).append(OUTPUT_DELIM)
										.append(followerCount);

								bw.write(finalText.toString());
								finalText = finalText.delete(0, finalText.length());
								bw.newLine();
							}
						}
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
	 * Check if string represents a valid long number
	 * 
	 * @param text
	 * @return
	 */
	private static boolean isNumber(String text) {

		if (text.isEmpty())
			return false;

		try {
			Long num = Long.parseLong(text);
			return true;
		} catch (NumberFormatException e) {
			logger.log(Level.SEVERE, null, e);
		}
		return false;
	}

	private static String getDateForMySQL(String date) {

		String newDate = null;

		if (date.isEmpty())
			return null;
		try {
			Date dateObj = utcDf.parse(date);
			newDate = mySQLDf.format(dateObj);
		} catch (java.text.ParseException e) {
			logger.log(Level.SEVERE, null, e);
		}

		return newDate;
	}

	/**
	 * Empty text is invalid
	 * 
	 * @param text
	 * @return
	 */
	private static boolean isValidText(String text) {

		if (!text.isEmpty()) {
			return true;
		}

		return false;
	}

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			MapperQuery3 map = new MapperQuery3();

			map.read("", "");
			// map.read("../part-100k", "src/map");

		} catch (IOException ex) {
		}
	}

}
