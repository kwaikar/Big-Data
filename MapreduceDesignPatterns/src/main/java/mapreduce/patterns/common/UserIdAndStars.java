package mapreduce.patterns.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

 
public class UserIdAndStars implements WritableComparable<UserIdAndStars> {
	private Text userId=new Text();
	private DoubleWritable rating =new DoubleWritable();
 

	/**
	 * @return the userId
	 */
	public Text getUserId() {
		return userId;
	}

	/**
	 * @param userId the userId to set
	 */
	public void setUserId(Text userId) {
		this.userId = userId;
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

	public UserIdAndStars() {
	}

	 
	public UserIdAndStars(Text userId, DoubleWritable rating) {
		super();
		this.userId = userId;
		this.rating = rating;
	}
 

	public void write(DataOutput out) throws IOException {
		userId.write(out);
		rating.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		userId.readFields(in);
		rating.readFields(in);
	}
  
	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return userId + ",\t" + rating;
	}

	public int compareTo(UserIdAndStars o) {
		// TODO Auto-generated method stub
		return this.userId.compareTo(o.userId);
	}

}
