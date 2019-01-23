package OrderTopnGroupingSecond;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class OrderBean implements WritableComparable<OrderBean>{
	
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
	 * @return the number
	 */
	public int getNumber() {
		return number;
	}

	/**
	 * @param number the number to set
	 */
	public void setNumber(int number) {
		this.number = number;
	}

	/**
	 * @return the amountFee
	 */
	public float getAmountFee() {
		return amountFee;
	}

	/**
	 * @param amountFee the amountFee to set
	 */
	public void setAmountFee(float amountFee) {
		this.amountFee = amountFee;
	}

	public void set(String orderId, String userId, String pdtName, float price, int number) {
		
		this.orderId = orderId;
		this.userId = userId;
		this.pdtName = pdtName;
		this.price = price;
		this.number = number;
		this.amountFee = this.price*this.number;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "OrderBean [orderId=" + orderId + ", userId=" + userId + ", pdtName=" + pdtName + ", price=" + price
				+ ", number=" + number + ", amountFee=" + amountFee + "]";
	}

	private String orderId;
	private String userId;
	private String pdtName;
	private float price;
	private int number;
	private float amountFee;
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.orderId = in.readUTF();		
		this.userId=in.readUTF();
		this.pdtName=in.readUTF();
		this.price=in.readFloat();
		this.number=in.readInt();
		this.amountFee=this.price*this.number;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.orderId);
		out.writeUTF(this.userId);
		out.writeUTF(this.pdtName);
		out.writeFloat(this.price);
		out.writeInt(this.number);
		
		
	}

	@Override
	public int compareTo(OrderBean o) {
		// TODO Auto-generated method stub
		
		return 	this.orderId.compareTo(o.getOrderId())==0?
			Float.compare(o.getAmountFee(), this.getAmountFee()):
				this.orderId.compareTo(o.getOrderId());
	
	}

}
