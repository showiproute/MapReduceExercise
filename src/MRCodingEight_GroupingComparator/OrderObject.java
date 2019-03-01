package MRCodingEight_GroupingComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class OrderObject implements WritableComparable<OrderObject>{

	/**
	 * @return the orderId
	 */
	public String getOrderId() {
		return orderId;
	}
	/**
	 * @param orderId the orderId to set
	 */
	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}
	/**
	 * @return the userId
	 */
	public String getUserId() {
		return userId;
	}
	/**
	 * @param userId the userId to set
	 */
	public void setUserId(String userId) {
		this.userId = userId;
	}
	/**
	 * @return the pdtName
	 */
	public String getPdtName() {
		return pdtName;
	}
	/**
	 * @param pdtName the pdtName to set
	 */
	public void setPdtName(String pdtName) {
		this.pdtName = pdtName;
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
	 * @return the amount
	 */
	public int getAmount() {
		return amount;
	}
	/**
	 * @param amount the amount to set
	 */
	public void setAmount(int amount) {
		this.amount = amount;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */

	public void set(String orderId, String userId, String pdtName, float price, int amount) {
		this.orderId = orderId;
		this.userId = userId;
		this.pdtName = pdtName;
		this.price = price;
		this.amount = amount;
		this.totalPrice=this.amount*this.price;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "OrderObject [orderId=" + orderId + ", userId=" + userId + ", pdtName=" + pdtName + ", price=" + price
				+ ", amount=" + amount + ", totalPrice=" + totalPrice + "]";
	}
	private String orderId;
	private String userId;
	private String pdtName;
	private float price;
	private int amount;
	private float totalPrice;
	/**
	 * @return the totalPrice
	 */
	public float getTotalPrice() {
		return totalPrice;
	}
	/**
	 * @param totalPrice the totalPrice to set
	 */
	public void setTotalPrice(float totalPrice) {
		this.totalPrice = totalPrice;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.orderId=in.readUTF();
		this.userId=in.readUTF();
		this.pdtName=in.readUTF();
		this.price=in.readFloat();
		this.amount=in.readInt();
		this.totalPrice=in.readFloat();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
		out.writeUTF(this.orderId);
		out.writeUTF(this.userId);
		out.writeUTF(this.pdtName);
		out.writeFloat(this.price);
		out.writeInt(this.amount);
		out.writeFloat(this.totalPrice);
	}
	@Override
	public int compareTo(OrderObject o) {
		// TODO Auto-generated method stub
		return this.orderId.compareTo(o.orderId)==0?
				Float.compare(o.getTotalPrice(), this.getTotalPrice())
				:this.getOrderId().compareTo(o.getOrderId());
	}
	
	
}
