package cloudbreakers.pojos;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.math.BigInteger;

/**
 *
 * @author ankit
 */
public class PDC_Encryption {

	private static final String secret_key = "8271997208960872478735181815578166723519929177896558845922250595511921395049126920528021164569045773";
	private static final BigInteger TWENTY_FIVE = new BigInteger("25");

	private static BigInteger KEY;
	private static BigInteger X_secret_key;

	private static Integer getSecretKey() {
		BigInteger Y = KEY.divide(X_secret_key);
		return 1 + Y.mod(TWENTY_FIVE).intValue();
	}

	public static String decrypt(String key, String message) {

		KEY = new BigInteger(key);
		X_secret_key = new BigInteger(secret_key);

		int z = getSecretKey();
		StringBuilder clearMessage = new StringBuilder();
		int length = message.length();
		int sqrt = (int) Math.sqrt(length);

		char[] msgArr = message.toCharArray();
		char[][] msgMat = new char[sqrt][sqrt];

		for (int i = 0; i < sqrt; i++) {
			for (int j = 0; j < sqrt; j++) {
				msgMat[i][j] = msgArr[i * sqrt + j];
			}
		}

		char[] intermediate = new char[length];
		for(int k=0; k < length;) {
			for(int sum=0; sum <= 2*(sqrt-1); sum++) {
				int limit = sum >= sqrt ? sqrt-1 : sum;
				for(int i= sum-limit, j=limit; i <= limit && j >= sum-limit; i++, j-- ){
					intermediate[k++] = msgMat[i][j];
				}
			}
		}

		for (int i = 0; i < length; i++) {
			int _char_index = intermediate[i] - 65;
			int _new_char;
			if (_char_index - z < 0)
				_new_char = 25 - (z - _char_index - 1);
			else
				_new_char = _char_index - z;

			clearMessage.append((char) ((_new_char + 65)));
		}
		return clearMessage.toString();
	}

//	public static void main(String[] main) {
//		System.out.println(
//				PDC_Encryption.decrypt("21704407273412482203751560948113977427938433839005491529776106565440478144265567324240466858212729328971300379", "LXNSEZHFJVEXVRVYUTUKXPMXHAGLABMORXFTAGADALPKUHZDJ"));
//	}

}
