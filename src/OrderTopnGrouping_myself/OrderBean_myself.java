package OrderTopnGrouping_myself;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;

public class OrderBean_myself implements WritableComparable<OrderBean_myself>,Serializable{
	


	private String orderId;
	private String userName;
	private String brandName;
	private float price;
	private int amount;
	private float amountFee;
	
	public void set(String orderId, String userName, String brandName, float price, int amount) {
		this.orderId = orderId;
		this.userName = userName;
		this.brandName = brandName;
		this.price = price;
		this.amount = amount;
		this.amountFee = this.price*this.amount;
	}

	
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
	 * @return the userName
	 */
	public String getUserName() {
		return userName;
	}

	/**
	 * @param userName the userName to set
	 */
	public void setUserName(String userName) {
		this.userName = userName;
	}

	/**
	 * @return the brandName
	 */
	public String getBrandName() {
		return brandName;
	}

	/**
	 * @param brandName the brandName to set
	 */
	public void setBrandName(String brandName) {
		this.brandName = brandName;
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



	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.orderId = in.readUTF();
		this.userName = in.readUTF();
		this.brandName = in.readUTF();
		this.price = in.readFloat();
		this.amount = in.readInt();
		this.amountFee = this.price*this.amount;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.orderId);
		out.writeUTF(this.userName);
		out.writeUTF(this.brandName);
		out.writeFloat(this.price);
		out.writeInt(this.amount);
		
	}

	@Override
	public int compareTo(OrderBean_myself o) {
		// TODO Auto-generated method stub
		return this.getOrderId().compareTo(o.getOrderId())==0?
				Float.compare(o.getAmountFee(), this.getAmountFee()):this.getOrderId().compareTo(o.getOrderId());
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "OrderBean_myself [orderId=" + orderId + ", userName=" + userName + ", brandName=" + brandName
				+ ", price=" + price + ", amount=" + amount + ", amountFee=" + amountFee + "]";
	}

	
}
