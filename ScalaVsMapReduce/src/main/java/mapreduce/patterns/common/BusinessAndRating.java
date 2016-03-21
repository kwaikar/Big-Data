package mapreduce.patterns.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author Kanchan Waikar Date Created : Mar 7, 2016 - 9:02:07 PM
 *
 */
public class BusinessAndRating implements WritableComparable<BusinessAndRating> {
	private Text businessId=new Text();
	private Text fullAddress=new Text();
	private Text categories=new Text();
	private DoubleWritable rating =new DoubleWritable();

	/**
	 * @return the businessId
	 */
	public Text getBusinessId() {
		return businessId;
	}

	/**
	 * @return the fullAddress
	 */
	public Text getFullAddress() {
		return fullAddress;
	}

	/**
	 * @return the categories
	 */
	public Text getCategories() {
		return categories;
	}

	/**
	 * @param businessId
	 *            the businessId to set
	 */
	public void setBusinessId(Text businessId) {
		this.businessId = businessId;
	}

	/**
	 * @param fullAddress
	 *            the fullAddress to set
	 */
	public void setFullAddress(Text fullAddress) {
		this.fullAddress = fullAddress;
	}

	/**
	 * @param categories
	 *            the categories to set
	 */
	public void setCategories(Text categories) {
		this.categories = categories;
	}

	/**
	 * @param rating
	 *            the rating to set
	 */
	public void setRating(DoubleWritable rating) {
		this.rating = rating;
	}

	/**
	 * @return the rating
	 */
	public DoubleWritable getRating() {
		return rating;
	}

	public BusinessAndRating() {
	}

	public BusinessAndRating(Text businessId, Text fullAddress, Text categories, DoubleWritable rating) {
		super();
		this.businessId = businessId;
		this.fullAddress = fullAddress;
		this.categories = categories;
		this.rating = rating;
	}

	public BusinessAndRating(Text businessId, DoubleWritable rating) {
		super();
		this.businessId = businessId;
		this.rating = rating;
		this.fullAddress = new Text("");
		this.categories = new Text("");
	}

	public BusinessAndRating(Text businessId, Text fullAddress, Text categories) {
		super();
		this.businessId = businessId;
		this.fullAddress = fullAddress;
		this.categories = categories;
		this.rating = new DoubleWritable(-10.0);
	}

	public void write(DataOutput out) throws IOException {
		businessId.write(out);
		fullAddress.write(out);
		categories.write(out);
		rating.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		businessId.readFields(in);
		fullAddress.readFields(in);
		categories.readFields(in);
		rating.readFields(in);
	}
  
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((businessId == null) ? 0 : businessId.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BusinessAndRating other = (BusinessAndRating) obj;
		if (businessId == null) {
			if (other.businessId != null)
				return false;
		} else if (!businessId.equals(other.businessId))
			return false;
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return businessId + ",\t" + fullAddress + ", \t" + categories ;
	}

	public int compareTo(BusinessAndRating o) {
		// TODO Auto-generated method stub
		return this.businessId.compareTo(o.businessId);
	}

}
