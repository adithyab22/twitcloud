package cloudbreakers.services;

public class UnicodeUnescaper{
	
	public String unescapeJavaString(String st) {
		 
	    StringBuilder sb = new StringBuilder(st.length());
	 
	    for (int i = 0; i < st.length(); i++) {
	        char ch = st.charAt(i);
	        if (ch == '\\') {
	            char nextChar = (i == st.length() - 1) ? '\\' : st
	                    .charAt(i + 1);
	            // Octal escape?
	            if (nextChar >= '0' && nextChar <= '7') {
	                String code = "" + nextChar;
	                i++;
	                if ((i < st.length() - 1) && st.charAt(i + 1) >= '0'
	                        && st.charAt(i + 1) <= '7') {
	                    code += st.charAt(i + 1);
	                    i++;
	                    if ((i < st.length() - 1) && st.charAt(i + 1) >= '0'
	                            && st.charAt(i + 1) <= '7') {
	                        code += st.charAt(i + 1);
	                        i++;
	                    }
	                }
	                sb.append((char) Integer.parseInt(code, 8));
	                continue;
	            }
	            switch (nextChar) {
	            case '\\':
	                ch = '\\';
	                break;
	            // Hex Unicode: u????
	            case 'u':
					int j = 2;
					while(i + j < st.length()
						&& ((st.charAt(i + j) >= '0' && st.charAt(i +j) <= '9')
						|| (st.charAt(i + j) >= 'A' && st.charAt(i + j) <= 'F')) && j <= 6) {
						j++;
					}

					StringBuilder codePoint = new StringBuilder();
					int k=2;
					while(k < j) {
						codePoint.append(st.charAt(i+k));
						k++;
					}
					try{
						int code = Integer.parseInt(codePoint.toString(), 16);
						sb.append(Character.toChars(code));
					} catch(NumberFormatException e) {
						k=2;
						while((i+k) < st.length()) { sb.append(st.charAt(i+k)); }
					}
					i += j-1;
					continue;
				}
				i++;
			}
	        sb.append(ch);
	    }
	    return sb.toString();
	}

}
