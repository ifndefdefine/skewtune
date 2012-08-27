package skewtune.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;

public class Utils {

	/**
	 * Returns index of maximum element in a given
	 * array of doubles. First maximum is returned.
	 *
	 * @param doubles the array of doubles
	 * @return the index of the maximum element
	 */
	public static /*@pure@*/ int maxIndex(double[] doubles) {

		double maximum = 0;
		int maxIndex = 0;

		for (int i = 0; i < doubles.length; i++) {
			if ((i == 0) || (doubles[i] > maximum)) {
				maxIndex = i;
				maximum = doubles[i];
			}
		}

		return maxIndex;
	}


	/**
	 * Normalizes the doubles in the array by their sum.
	 *
	 * @param doubles the array of double
	 * @exception IllegalArgumentException if sum is Zero or NaN
	 */
	public static void normalize(double[] doubles) {

		double sum = 0;
		for (int i = 0; i < doubles.length; i++) {
			sum += doubles[i];
		}
		normalize(doubles, sum);
	}

	/**
	 * Normalizes the doubles in the array using the given value.
	 *
	 * @param doubles the array of double
	 * @param sum the value by which the doubles are to be normalized
	 * @exception IllegalArgumentException if sum is zero or NaN
	 */
	public static void normalize(double[] doubles, double sum) {

		if (Double.isNaN(sum)) {
			throw new IllegalArgumentException("Can't normalize array. Sum is NaN.");
		}
		if (sum == 0) {
			// Maybe this should just be a return.
			throw new IllegalArgumentException("Can't normalize array. Sum is zero.");
		}
		for (int i = 0; i < doubles.length; i++) {
			doubles[i] /= sum;
		}
	}
	
	/**
	 * Converts an array containing the natural logarithms of
	 * probabilities stored in a vector back into probabilities.
	 * The probabilities are assumed to sum to one.
	 *
	 * @param a an array holding the natural logarithms of the probabilities
	 * @return the converted array 
	 */
	public static double[] logs2probs(double[] a) {

		double max = a[maxIndex(a)];
		double sum = 0.0;

		double[] result = new double[a.length];
		for(int i = 0; i < a.length; i++) {
			result[i] = Math.exp(a[i] - max);
			sum += result[i];
		}

		normalize(result, sum);

		return result;
	} 

	
	  /**
	   * Rounds a double and converts it into String.
	   *
	   * @param value the double value
	   * @param afterDecimalPoint the (maximum) number of digits permitted
	   * after the decimal point
	   * @return the double as a formatted string
	   */
	  public static /*@pure@*/ String doubleToString(double value, int afterDecimalPoint) {
	    
	    StringBuffer stringBuffer;
	    double temp;
	    int dotPosition;
	    long precisionValue;
	    
	    temp = value * Math.pow(10.0, afterDecimalPoint);
	    if (Math.abs(temp) < Long.MAX_VALUE) {
	      precisionValue = 	(temp > 0) ? (long)(temp + 0.5) 
	                                   : -(long)(Math.abs(temp) + 0.5);
	      if (precisionValue == 0) {
		stringBuffer = new StringBuffer(String.valueOf(0));
	      } else {
		stringBuffer = new StringBuffer(String.valueOf(precisionValue));
	      }
	      if (afterDecimalPoint == 0) {
		return stringBuffer.toString();
	      }
	      dotPosition = stringBuffer.length() - afterDecimalPoint;
	      while (((precisionValue < 0) && (dotPosition < 1)) ||
		     (dotPosition < 0)) {
		if (precisionValue < 0) {
		  stringBuffer.insert(1, '0');
		} else {
		  stringBuffer.insert(0, '0');
		}
		dotPosition++;
	      }
	      stringBuffer.insert(dotPosition, '.');
	      if ((precisionValue < 0) && (stringBuffer.charAt(1) == '.')) {
		stringBuffer.insert(1, '0');
	      } else if (stringBuffer.charAt(0) == '.') {
		stringBuffer.insert(0, '0');
	      }
	      int currentPos = stringBuffer.length() - 1;
	      while ((currentPos > dotPosition) &&
		     (stringBuffer.charAt(currentPos) == '0')) {
		stringBuffer.setCharAt(currentPos--, ' ');
	      }
	      if (stringBuffer.charAt(currentPos) == '.') {
		stringBuffer.setCharAt(currentPos, ' ');
	      }
	      
	      return stringBuffer.toString().trim();
	    }
	    return new String("" + value);
	  }

	  /**
	   * Rounds a double and converts it into a formatted decimal-justified String.
	   * Trailing 0's are replaced with spaces.
	   *
	   * @param value the double value
	   * @param width the width of the string
	   * @param afterDecimalPoint the number of digits after the decimal point
	   * @return the double as a formatted string
	   */
	  public static /*@pure@*/ String doubleToString(double value, int width,
					      int afterDecimalPoint) {
	    
	    String tempString = doubleToString(value, afterDecimalPoint);
	    char[] result;
	    int dotPosition;

	    if ((afterDecimalPoint >= width) 
	        || (tempString.indexOf('E') != -1)) { // Protects sci notation
	      return tempString;
	    }

	    // Initialize result
	    result = new char[width];
	    for (int i = 0; i < result.length; i++) {
	      result[i] = ' ';
	    }

	    if (afterDecimalPoint > 0) {
	      // Get position of decimal point and insert decimal point
	      dotPosition = tempString.indexOf('.');
	      if (dotPosition == -1) {
		dotPosition = tempString.length();
	      } else {
		result[width - afterDecimalPoint - 1] = '.';
	      }
	    } else {
	      dotPosition = tempString.length();
	    }
	    

	    int offset = width - afterDecimalPoint - dotPosition;
	    if (afterDecimalPoint > 0) {
	      offset--;
	    }

	    // Not enough room to decimal align within the supplied width
	    if (offset < 0) {
	      return tempString;
	    }

	    // Copy characters before decimal point
	    for (int i = 0; i < dotPosition; i++) {
	      result[offset + i] = tempString.charAt(i);
	    }

	    // Copy characters after decimal point
	    for (int i = dotPosition + 1; i < tempString.length(); i++) {
	      result[offset + i] = tempString.charAt(i);
	    }

	    return new String(result);
	  }
	  
	  public static long parseSize(String s) {
	      long sz = -1;
	      long multiplier = 1;
	      String number = s.trim();
	      char postfix = number.charAt(number.length()-1);
	      if ( "gGkKmMpP".indexOf(postfix) >= 0 ) {
	          number = number.substring(0,number.length()-1);
	          switch ( postfix ) {
	          case 'P': case 'p': multiplier <<= 40; break;
	          case 'G': case 'g': multiplier <<= 30; break; 
	          case 'M': case 'm': multiplier <<= 20; break;
	          case 'K': case 'k': multiplier <<= 10; break;
              default:
	          }
	      }
	      
	      return Long.parseLong(number) * multiplier;
	  }
	  
	  public static String toHex(byte[] b) {
	      if ( b == null ) return "(null)";
	      return toHex(b,0,b.length);
	  }
	  
	  public static String toHex(byte[] b,int off,int len) {
	      if ( b == null ) return "(null)";
	      StringBuilder buf = new StringBuilder(len*2);
	      for ( int i = off; i < off+len; ++i ) {
	          byte v = b[i];
	          final int upper = (v & 0xf0) >>> 4;
	          final int lower = (v & 0x0f);
	          if ( upper < 10 ) {
	              buf.append((char)('0' + upper));
	          } else {
	              buf.append((char)('a' + (upper-10)));
	          }
	          if ( lower < 10 ) {
                  buf.append((char)('0' + lower));
              } else {
                  buf.append((char)('a' + (lower-10)));
              }
	      }
	      return buf.toString();
	  }
	  
	  public static byte[] toBinary(String s) {
	      byte[] content = new byte[s.length()>>1];
	      int i = 0;
	      while ( i < s.length() ) {
	          char ch1 = s.charAt(i++);
	          char ch2 = s.charAt(i++);
	          
	          int b1 = ( ch1 < 'a' ) ? ch1 - '0' : 10 + (ch1 - 'a');
	          int b2 = ( ch2 < 'a' ) ? ch2 - '0' : 10 + (ch2 - 'a');
	          content[(i>>1) - 1] = (byte)((b1 << 4) | b2);
	      }
	      
	      return content;
	  }
	  
	    
    public static void copyConfiguration(Configuration from,Configuration to,String name) {
        String v = from.get(name);
        if ( v != null && v.length() > 0 )
            to.set(name, v);
    }
    public static void copyConfigurationAs(Configuration from,String name,Configuration to,String alias) {
        String v = from.get(name);
        if ( v != null && v.length() > 0 )
            to.set(alias, v);
    }
}
