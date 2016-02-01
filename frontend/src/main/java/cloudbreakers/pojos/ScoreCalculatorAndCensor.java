package cloudbreakers.pojos;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 
 * @author Adithya
 *
 */

public class ScoreCalculatorAndCensor {

	public static int calculateSentimentScore(String text) {

		text = text.toLowerCase();
		int score = 0;
		for (String term : SentimentScoreLookUp.getMap().keySet()) {

			String pattern = "([^a-zA-Z0-9]" + term + "[^a-zA-Z0-9])";
			Pattern p = Pattern.compile(pattern);
			Matcher m = p.matcher(text);

			int count = 0;
			while (m.find()) {
				count++;
			}
			score += SentimentScoreLookUp.getMap().get(term) * count;
		}
		return score;
	}

	public static String censorText(String text) {

		String[] splits = text.split("[^a-zA-Z0-9]+");
		for (String word : splits) {
			if (!word.isEmpty()) {
				if (BannedWordsLookUp.getSet().contains(word.toLowerCase())) {
					text = text.replaceFirst(word,
							word.charAt(0) + asteriskbuilder(word) + word.charAt(word.length() - 1));
				}
			}
		}
		return text;
	}

	public static String asteriskbuilder(String s) {

		StringBuilder sb = new StringBuilder();
		for (int i = 1; i < s.length() - 1; i++) {
			sb.append('*');
		}
		return sb.toString();
	}

	public static void main(String[] args) {
		try {
			assert ScoreCalculatorAndCensor.censorText("That was a fucking fuck").equals("That was a f*****g f**k");
			assert (ScoreCalculatorAndCensor.calculateSentimentScore("I am a good boy") == 10);
		} catch (Exception ex) {
		}
	}

}
