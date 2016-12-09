package aggregation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

import miscUtils.SC;

public class Aggregation implements Writable {
	private float numVal;
	private String strVal;
	private long count = 0;
	private float mean, median, stdDev, min, max;
	private float entropy, gini, theil;	
	private long unique;
	
	//////////////////////////////////////////////////////////////////////////

	public float getNumericValue() {
		return numVal;
	}

	public void setValue(float val) {
		this.numVal = val;
	}

	public String getStrValue() {
		return strVal;
	}

	public void setValue(String  strVal) {
		this.strVal = strVal;
	}

	
	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public float getMean() {
		return mean;
	}

	public void setMean(float mean) {
		this.mean = mean;
	}

	public float getStdDev() {
		return stdDev;
	}

	public void setStdDev(float stdDev) {
		this.stdDev = stdDev;
	}

	public float getMedian() {
		return median;
	}

	public void setMedian(float median) {
		this.median = median;
	}

	public float getEntropy() {
		return entropy;
	}

	public void setEntropy(float entropy) {
		this.entropy = entropy;
	}

	public float getGini() {
		return gini;
	}

	public void setGini(float gini) {
		this.gini = gini;
	}

	public float getTheil() {
		return theil;
	}

	public void setTheil(float theil) {
		this.theil = theil;
	}
	
	public long getUnique() {
		return unique;
	}

	public void setUnique(long unique) {
		this.unique = unique;
	}
	
	public float getMin() {
		return min;
	}

	public void setMin(float min) {
		this.min = min;
	}

	public float getMax() {
		return max;
	}

	public void setMax(float max) {
		this.max = max;
	}
	
	
	///////////////////////////////////////////////////////////////////////////

	public void readFields(DataInput in) throws IOException {
		strVal = in.readLine();
		/*
		//numVal = in.readFloat();		
		count = in.readLong();
		mean = in.readFloat();
		stdDev = in.readFloat();
		median = in.readFloat();
		entropy = in.readFloat();
		gini = in.readFloat();
		theil = in.readFloat();
		unique = in.readLong();
		min = in.readFloat();
		max = in.readFloat();
		*/
	}	
	
	public void write(DataOutput out) throws IOException {
		out.writeFloat(numVal);
		out.writeChars(strVal);
		out.writeLong(count);
		out.writeFloat(mean);
		out.writeFloat(stdDev);
		out.writeFloat(median);
		out.writeFloat(entropy);
		out.writeFloat(gini);
		out.writeFloat(theil);
		out.writeLong(unique);
		out.writeFloat(min);
		out.writeFloat(max);
	}
	
	@Override
	public String toString() {
		String ret = SC.ZeroSpace;
		ret += SC.TB + count;
		ret += SC.TB + mean;
	/*
		ret += SpecialCharacter.TB + stdDev;
		ret += SpecialCharacter.TB + median;
		ret += SpecialCharacter.TB + entropy;
		ret += SpecialCharacter.TB + gini;		
		ret += SpecialCharacter.TB + theil;		
		ret += SpecialCharacter.TB + unique;
		ret += SpecialCharacter.TB + min;
		ret += SpecialCharacter.TB + max;
		*/
		return ret;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Float.floatToIntBits(mean);
		result += prime * result + Float.floatToIntBits(count);
		result += prime * result + Float.floatToIntBits(stdDev);
		result += prime * result + Float.floatToIntBits(median);
		result += prime * result + Float.floatToIntBits(entropy);
		result += prime * result + Float.floatToIntBits(gini);
		result += prime * result + Float.floatToIntBits(theil);
		result += prime * result + Float.floatToIntBits(unique);
		result += prime * result + Float.floatToIntBits(min);
		result += prime * result + Float.floatToIntBits(max);		
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
	
		Aggregation other = (Aggregation) obj;
		if (Float.floatToIntBits(mean) != Float.floatToIntBits(other.mean))
			return false;
		if (Float.floatToIntBits(count) != Float.floatToIntBits(other.count))
			return false;
		if (Float.floatToIntBits(stdDev) != Float.floatToIntBits(other.stdDev))
			return false;		
		if (Float.floatToIntBits(median) != Float.floatToIntBits(other.median))
			return false;
		if (Float.floatToIntBits(numVal) != Float.floatToIntBits(other.numVal))
			return false;		
		if (Float.floatToIntBits(entropy) != Float.floatToIntBits(other.entropy))
			return false;		
		if (Float.floatToIntBits(gini) != Float.floatToIntBits(other.gini))
			return false;		
		if (Float.floatToIntBits(theil) != Float.floatToIntBits(other.theil))
			return false;		
		if (Float.floatToIntBits(unique) != Float.floatToIntBits(other.unique))
			return false;		
		if (Float.floatToIntBits(min) != Float.floatToIntBits(other.min))
			return false;		
		if (Float.floatToIntBits(max) != Float.floatToIntBits(other.max))
			return false;		
		return true;
	}	
	//////////////////////////////////////////////////////////////////////////
	

}
