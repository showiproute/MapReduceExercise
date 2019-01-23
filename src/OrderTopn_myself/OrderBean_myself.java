package OrderTopn_myself;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;

public class OrderBean_myself implements WritableComparable<OrderBean_myself>,Serializable{
	private String orderID;
	private String username;
	private String brandname;
	private float price;
	private int num;
	private float feeAmount;
	
	public void set(String orderID, String username, String brandname, float price, int num) {
		this.orderID = orderID;
		this.username = username;
		this.brandname = brandname;
		this.price = price;
		this.num = num;
		this.feeAmount=this.price*this.num;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "OrderBean_myself [orderID=" + orderID + ", username=" + username + ", brandname=" + brandname
				+ ", price=" + price + ", num=" + num + ", feeAmount=" + feeAmount + "]";
	}
	/**
	 * @return the orderID
	 */
	public String getOrderID() {
		return orderID;
	}
	/**
	 * @param orderID the orderID to set
	 */
	public void setOrderID(String orderID) {
		this.orderID = orderID;
	}
	/**
	 * @return the username
	 */
	public String getUsername() {
		return username;
	}
	/**
	 * @param username the username to set
	 */
	public void setUsername(String username) {
		this.username = username;
	}
	/**
	 * @return the brandname
	 */
	public String getBrandname() {
		return brandname;
	}
	/**
	 * @param brandname the brandname to set
	 */
	public void setBrandname(String brandname) {
		this.brandname = brandname;
	}
	/**
	 * @return the price
	 */
	public float getPrice() {
		return price;
	}
	/**
	 * @param price the price to set
	 */
	public void setPrice(float price) {
		this.price = price;
	}
	/**
	 * @return the num
	 */
	public int getNum() {
		return num;
	}
	/**
	 * @param num the num to set
	 */
	public void setNum(int num) {
		this.num = num;
	}
	
	public float getFeeAmount() {
		return this.feeAmount;
	}
	
	public void setFeeAmount() {
		this.feeAmount=this.price*this.num;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.orderID = in.readUTF();
		this.username = in.readUTF();
		this.brandname = in.readUTF();
		this.price = in.readFloat();
		this.num = in.readInt();
		this.feeAmount=this.num*this.price;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.orderID);
		out.writeUTF(this.username);
		out.writeUTF(this.brandname);
		out.writeFloat(this.price);
		out.writeInt(this.num);
	}
	@Override
	public int compareTo(OrderBean_myself o) {
		// TODO Auto-generated method stub
		return Float.compare(o.getFeeAmount(), this.getFeeAmount())==0?
				this.brandname.compareTo(o.brandname):Float.compare(o.getFeeAmount(), this.getFeeAmount());
	}
	

}
