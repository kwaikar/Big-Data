package org.apache.spark.metrics.source;

class TestAutoCipher {

	public static void main(String[] args) {
		//System.out.println(encrypt("security","kanchan"));
		System.out.println(decrypt("security",encrypt("security","kanchan")));
	}
	private static String decrypt(String key, String cipherText) {
		char[][] matrix = new char[26][26];

		matrix = matrixInit();
		// System.out.println(matrix);

		String decryptedText = "";
		char[] cipherTextArr = cipherText.toUpperCase().toCharArray();

		char[] keyArr = new char[cipherText.length() + key.length() + 2];

		System.out.println(keyArr.length);

		for (int i = 0; i < key.length(); i++) {
			System.out.println(key.toUpperCase().charAt(i) + " i " + i);
			keyArr[i] = key.toUpperCase().charAt(i);
		}

		for (int i = 0; i < cipherTextArr.length; i++) {
			char cipherTextChar = cipherTextArr[i];
			if (cipherTextChar == ' ') {
				decryptedText += ' ';
			} else {
				char keyChar = keyArr[i];

				int j = ((int) cipherTextChar - 65);
				int k = ((int) keyChar - 65);
				System.out.println("cipherText" + cipherTextChar + " Key char" + keyChar);
				System.out.println("cipherText" + (int) cipherTextChar + " Key char" + (int) keyChar);
				System.out.println("J & K" + j + " " + k);
				int q=0;
				for ( ; q< matrix[k].length;q++) {
					Character c = null;
					c=matrix[k][q];
					if(c==cipherTextChar)
					{
					break;	
					}
				}
				decryptedText += matrix[q][0];
				keyArr[key.length() + i] = matrix[q][0];
			}

		}
		System.out.println(decryptedText);
		return decryptedText;
	}

	private static String encrypt(String key, String plainText) {

		char[][] matrix = new char[26][26];

		matrix = matrixInit();
		// System.out.println(matrix);

		String sameLengthKey = key + plainText;

		sameLengthKey = sameLengthKey.substring(0, plainText.length());

		System.out.println("Lengths: " + plainText.length() + " : " + sameLengthKey.length());

		String cipherTextNew = "";
		char[] plainTextArr = plainText.toUpperCase().toCharArray();
		char[] keyArr = sameLengthKey.toUpperCase().toCharArray();

		for (int i = 0; i < plainTextArr.length; i++) {
			char plainTextChar = plainTextArr[i];
			char keyChar = keyArr[i];

			if (plainTextChar != ' ') {
				int j = (int) plainTextChar - 65;
				int k = (int) keyChar - 65;
				cipherTextNew += matrix[j][k];
			} else {
				cipherTextNew += ' ';
			}
System.out.println("=>"+cipherTextNew);

		}
		return cipherTextNew;
	}

	// Initialize the matrix
	private static char[][] matrixInit() {
		char[][] matrix = new char[26][26];
		int i = 0, j = 0, currentLetter = 0;
		// Initialize matrix
		for (i = 0; i < 26; i++) {
			for (j = 0; j < 26; j++) {

				currentLetter = i + j;
				if (currentLetter >= 26) {
					currentLetter = currentLetter - 26;
				}
				matrix[i][j] = (char) (currentLetter + 65);
			}
		}

		for (int k = 0; k < matrix.length; k++) {
			for (int l = 0; l < matrix[0].length; l++) {
				System.out.print(matrix[k][l] + " ");
			}
			System.out.print("\n");
		}
		return matrix;

	}
}
