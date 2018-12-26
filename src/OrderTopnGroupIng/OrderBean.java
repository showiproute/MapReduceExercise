package OrderTopnGroupIng;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;

public class OrderBean implements WritableComparable<OrderBean>,Serializable{
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	private String orderID;
	private String userID;
	private String pdtName;
	private float price;
	private int number;
	private float amountFee;
	
	
	@Override
	public String toString() {
		return "OrderBean [orderID=" + orderID + ", userID=" + userID + ", pdtName=" + pdtName + ", price=" + price
				+ ", number=" + number + ", amountFee=" + amountFee + "]";
	}

	
	public void set(String orderID, String userID, String pdtName, float price, int number) {
		this.orderID = orderID;
		this.userID = userID;
		this.pdtName = pdtName;
		this.price = price;
		this.number = number;
		this.amountFee = this.price*this.number;
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
	 * @return the userID
	 */
	public String getUserID() {
		return userID;
	}
	/**
	 * @param userID the userID to set
	 */
	public void setUserID(String userID) {
		this.userID = userID;
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


	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.orderID=in.readUTF();
		this.userID=in.readUTF();
		this.pdtName=in.readUTF();
		this.price=in.readFloat();
		this.number=in.readInt();
		this.amountFee=this.price*this.number;
	}


	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.orderID);
		out.writeUTF(this.userID);
		out.writeUTF(this.pdtName);
		out.writeFloat(this.price);
		out.writeInt(this.number);
		
	}

	@Override
	public int compareTo(OrderBean o) {
		// TODO Auto-generated method stub

		return this.orderID.compareTo(o.getOrderID())==0?
				Float.compare(o.getAmountFee(),this.getAmountFee()):this.orderID.compareTo(o.getOrderID());
				
		
//		float temp=o.getAmountFee()-this.getAmountFee();
//		if(temp==0) {
//			return this.pdtName.compareTo(o.getPdtName());
//		}else if(temp>0) {
//			return 1;
//		}else {
//			return -1;
//		}	
	}

}
