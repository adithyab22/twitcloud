
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Program to calculate Sentiment score of tweets and also censor the banned words
 * @author Adithya Balasubramanian
 *
 */

public class ScoreCalculatorAndCensor {

	public static int calculateSentimentScore(String text) {

		int score = 0;
		text = text.toLowerCase();
		text = " " + text + " ";

		String[] splits = text.split("[^a-zA-Z0-9]+");
		for (String word : splits) {
			if (!word.isEmpty()) {
				if (SentimentScoreLookUp.wordToScore.containsKey(word)) {
					score += SentimentScoreLookUp.wordToScore.get(word);
				}
			}
		}

		for (String term : SentimentScoreLookUp.longWordToScore.keySet()) {

			String pattern = "([^a-zA-Z0-9]" + term + "[^a-zA-Z0-9])";
			Pattern p = Pattern.compile(pattern);
			Matcher m = p.matcher(text);

			int count = 0;
			while (m.find()) {
				count++;
			}
			if (count != 0) {
				score += SentimentScoreLookUp.longWordToScore.get(term) * count;
			}
		}

		for (String term : SentimentScoreLookUp.spclWordToScore.keySet()) {

			String pattern = "([^a-zA-Z0-9]" + term + "[^a-zA-Z0-9])";
			Pattern p = Pattern.compile(pattern);
			Matcher m = p.matcher(text);

			int count = 0;
			while (m.find()) {
				count++;
			}
			if (count != 0) {
				score += SentimentScoreLookUp.spclWordToScore.get(term) * count;
			} else {

				String smallWord = SentimentScoreLookUp.spclWordToSmallword.get(term);
				pattern = "([^a-zA-Z0-9]" + smallWord + "[^a-zA-Z0-9])";
				p = Pattern.compile(pattern);
				m = p.matcher(text);

				count = 0;
				while (m.find()) {
					count++;
				}
				if (count != 0) {
					score += SentimentScoreLookUp.spclSmallWordToScore.get(smallWord) * count;
				}
			}
		}

//		 for (String term : SentimentScoreLookUp.wordToScore.keySet()) {
//		
//		 String pattern = "([^a-zA-Z0-9]" + term + "[^a-zA-Z0-9])";
//		 Pattern p = Pattern.compile(pattern);
//		 Matcher m = p.matcher(text);
//		
//		 int count = 0;
//		 while (m.find()) {
//		 count++;
//		 }
//		 if (count != 0) {
//		 score += SentimentScoreLookUp.wordToScore.get(term) * count;
//		 }
//		 }
//		 
//		 for (String term : SentimentScoreLookUp.longWordToScore.keySet()) {
//				
//			 String pattern = "([^a-zA-Z0-9]" + term + "[^a-zA-Z0-9])";
//			 Pattern p = Pattern.compile(pattern);
//			 Matcher m = p.matcher(text);
//			
//			 int count = 0;
//			 while (m.find()) {
//			 count++;
//			 }
//			 if (count != 0) {
//			 score += SentimentScoreLookUp.longWordToScore.get(term) * count;
//			 }
//			 }
//		 
//		 for (String term : SentimentScoreLookUp.spclSmallWordToScore.keySet()) {
//				
//			 String pattern = "([^a-zA-Z0-9]" + term + "[^a-zA-Z0-9])";
//			 Pattern p = Pattern.compile(pattern);
//			 Matcher m = p.matcher(text);
//			
//			 int count = 0;
//			 while (m.find()) {
//			 count++;
//			 }
//			 if (count != 0) {
//			 score += SentimentScoreLookUp.spclSmallWordToScore.get(term) * count;
//			 }
//			 }
//		 

		return score;
	}

	public static String censorText(String text) {

		String[] splits = text.split("[^a-zA-Z0-9]+");
		for (String word : splits) {
			if (!word.isEmpty()) {
				if (BannedWordsLookUp.getSet().contains(word.toLowerCase())) {
					StringBuilder censoredWord = new StringBuilder();
					censoredWord.append(word.charAt(0)).append(asteriskbuilder(word))
							.append(word.charAt(word.length() - 1));
					text = text.replaceFirst(word, censoredWord.toString());
				}
			}
		}
		return text;
	}

	private static String asteriskbuilder(String s) {

		StringBuilder sb = new StringBuilder();
		for (int i = 1; i < s.length() - 1; i++) {
			sb.append('*');
		}
		return sb.toString();
	}

	public static void main(String[] args) {
		
		try {
			Character c = 'a';
			assert Character.isAlphabetic(c);
			assert ScoreCalculatorAndCensor.censorText("That was a fucking fuck").equals("That was a f*****g f**k");

			assert ScoreCalculatorAndCensor.calculateSentimentScore(
					"RT @onedirection: And here's a reminder of the #YouAndIVideo if you need a helping hand to write them! 1DHQ x http://t.co/elRN30pho6") == 2 : ScoreCalculatorAndCensor
							.calculateSentimentScore(
									"RT @onedirection: And here's a reminder of the #YouAndIVideo if you need a helping hand to write them! 1DHQ x http://t.co/elRN30pho6");

			assert ScoreCalculatorAndCensor.calculateSentimentScore(
					"RT @aiinour_sherif: if u dont like hickeys or ass grabbing then don't talk to me") == -6 : ScoreCalculatorAndCensor
							.calculateSentimentScore("RT @aiinour_sherif: if u dont like hickeys or ass grabbing then don't talk to me");

			assert (ScoreCalculatorAndCensor
					.calculateSentimentScore("I am a good boy good") == 6) : ScoreCalculatorAndCensor
							.calculateSentimentScore("I am a good boy good");

		} catch (Exception ex) {
		}
	}

}
