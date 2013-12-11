package idiro.hadoop.pig;

public class PigUtils {
	
	public static String getDelimiter(char character){
		String delimiter = null;
		if( character >= 32 && character <= 126 ){
			delimiter = new String(new char[]{character});
		}else if(character == '\t'){
			delimiter = "\\t";
		}else{
			delimiter = Integer.toOctalString(character);
			if(delimiter.length() == 1){
				delimiter = "\\\\u00"+delimiter;
			}else if(delimiter.length() == 2){
				delimiter = "\\\\u0"+delimiter;
			}else{
				delimiter = "\\\\u"+delimiter;
			}
		}
		return delimiter;
	}

}
