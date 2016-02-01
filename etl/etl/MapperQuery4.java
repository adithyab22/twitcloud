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
import java.util.Iterator;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringEscapeUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * 
 * @author Nitish
 *
 */
public class MapperQuery4 {

	public static Logger logger = Logger.getLogger(MapperQuery4.class.getName());

	private static SimpleDateFormat utcDf = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");

	private static final String KEY_ID_STR = "id_str";
	private static final String KEY_CREATED_AT = "created_at";
	private static final String KEY_TEXT = "text";
	private static final String KEY_USER = "user";
	private static final String KEY_USER_ID = "id";
	private static final String KEY_ENTITIES = "entities";
	private static final String KEY_ENTITIES_HASHTAGS = "hashtags";

	private static final char OUTPUT_DELIM = '\t';

	static {

		logger.setLevel(Level.OFF);

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
			StringBuilder hashTagText = new StringBuilder();
			String text = "";
			String createdAt = "";
			JSONObject jsonObject = null;
			JSONObject userObject = null;
			Long userId = null;
			JSONObject entitiesObject = null;
			JSONArray hashTags = null;
			StringBuilder finalText = new StringBuilder();
			
			while ((currentLine = br.readLine()) != null) {
				try {
					jsonObject = (JSONObject) parser.parse(currentLine);
					createdAt = (String) jsonObject.get(KEY_CREATED_AT);
					userObject = (JSONObject) jsonObject.get(KEY_USER);
					text = (String) jsonObject.get(KEY_TEXT);
					userId = null;

					if (userObject != null && userObject.get(KEY_USER_ID) != null) {
						userId = (Long) userObject.get(KEY_USER_ID);
					}

					entitiesObject = (JSONObject) jsonObject.get(KEY_ENTITIES);

					hashTags = null;
					if (entitiesObject != null && entitiesObject.get(KEY_ENTITIES_HASHTAGS) != null) {
						hashTags = ((JSONArray) entitiesObject.get(KEY_ENTITIES_HASHTAGS));
					}

					// Ignore tweets with null fields required for us
					if (text != null && createdAt != null && userId != null) {

						createdAt = createdAt.trim();
						Long ts = getTimeStamp(createdAt);
						text = StringEscapeUtils.escapeJava(text);
						text = StringEscapeUtils.escapeJava(text);
						
						if (ts != null) {

							if (hashTags != null && !hashTags.isEmpty()) {
								if (hashTagText.length() > 0) {
									hashTagText.delete(0, hashTagText.length());
								}
								hashTagText.append(",");
								Iterator<JSONObject> itr = hashTags.iterator();
								while (itr.hasNext()) {
									JSONObject hashTag = itr.next();
									if (hashTag.get(KEY_TEXT) != null && !((String) hashTag.get(KEY_TEXT)).isEmpty()) {

										finalText
										.append(text).append(OUTPUT_DELIM)
										.append(userId).append(OUTPUT_DELIM)
										.append(ts).append(OUTPUT_DELIM)
										.append(hashTag.get(KEY_TEXT).toString());

										bw.write(finalText.toString());
										finalText = finalText.delete(0, finalText.length());
										bw.newLine();
									}
								}
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
	 * Gets epoch time in millisecs from date formatted as EEE MMM dd kk:mm:ss Z
	 * yyyy
	 * 
	 * @param date
	 * @return
	 */
	private static Long getTimeStamp(String date) {

		if (date.isEmpty())
			return null;

		try {
			Long ts = utcDf.parse(date).getTime();
			return ts;
		} catch (java.text.ParseException e) {
			logger.log(Level.SEVERE, null, e);
		}
		return null;
	}

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			MapperQuery4 map = new MapperQuery4();

			map.read("", "");
			// map.read("../part-20k", "src/tmp/map");

		} catch (IOException ex) {
		}
	}

}
