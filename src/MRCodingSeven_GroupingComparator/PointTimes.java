package MRCodingSeven_GroupingComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PointTimes implements WritableComparable<PointTimes>{

	/**
	 * @return the point
	 */
	public String getPoint() {
		return point;
	}
	/**
	 * @param point the point to set
	 */
	public void setPoint(String point) {
		this.point = point;
	}
	/**
	 * @return the counts
	 */
	public int getCounts() {
		return counts;
	}
	/**
	 * @param counts the counts to set
	 */
	public void setCounts(int counts) {
		this.counts = counts;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "PointTimes [point=" + point + ", counts=" + counts + "]";
	}
	public void set(String point, int counts) {
		this.point = point;
		this.counts = counts;
	}
	private String point;
	private int counts;
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.point=in.readUTF();
		this.counts=in.readInt();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.point);
		out.writeInt(this.counts);
	}
	@Override
	public int compareTo(PointTimes o) {
		// TODO Auto-generated method stub
		return this.getPoint().compareTo(o.getPoint())==0?
			this.getPoint().compareTo(o.getPoint()):o.getCounts()-this.getCounts();
	}
	
}
