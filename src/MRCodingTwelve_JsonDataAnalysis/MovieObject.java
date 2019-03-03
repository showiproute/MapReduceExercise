package MRCodingTwelve_JsonDataAnalysis;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MovieObject implements WritableComparable<MovieObject>{

	/**
	 * @return the movieId
	 */
	public String getMovieId() {
		return movieId;
	}
	/**
	 * @param movieId the movieId to set
	 */
	public void setMovieId(String movieId) {
		this.movieId = movieId;
	}
	/**
	 * @return the rate
	 */
	public int getRate() {
		return rate;
	}
	/**
	 * @param rate the rate to set
	 */
	public void setRate(int rate) {
		this.rate = rate;
	}
	/**
	 * @return the timeStamp
	 */
	public String getTimeStamp() {
		return timeStamp;
	}
	/**
	 * @param timeStamp the timeStamp to set
	 */
	public void setTimeStamp(String timeStamp) {
		this.timeStamp = timeStamp;
	}
	/**
	 * @return the uid
	 */
	public String getUid() {
		return uid;
	}
	/**
	 * @param uid the uid to set
	 */
	public void setUid(String uid) {
		this.uid = uid;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "MovieObject [movieId=" + movieId + ", rate=" + rate + ", timeStamp=" + timeStamp + ", uid=" + uid + "]";
	}
	public void set(String movieId, int rate, String timeStamp, String uid) {
		this.movieId = movieId;
		this.rate = rate;
		this.timeStamp = timeStamp;
		this.uid = uid;
	}
	private String movieId;
	private int rate;
	private String timeStamp;
	private String uid;
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.movieId=in.readUTF();
		this.rate=in.readInt();
		this.timeStamp=in.readUTF();
		this.uid=in.readUTF();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.movieId);
		out.writeInt(this.rate);
		out.writeUTF(this.timeStamp);
		out.writeUTF(this.uid);
	}

	@Override
	public int compareTo(MovieObject o) {
		// TODO Auto-generated method stub
		return this.getUid().compareTo(o.getUid())==0?
				Integer.compare(o.getRate(), this.getRate()):
					this.getUid().compareTo(o.getUid());
	}
	
}
