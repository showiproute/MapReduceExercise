package MRCodingThree_SelfDesinType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowObject implements Writable{

	/**
	 * @return the phone
	 */
	public String getPhone() {
		return phone;
	}

	/**
	 * @param phone the phone to set
	 */
	public void setPhone(String phone) {
		this.phone = phone;
	}

	/**
	 * @return the uFlow
	 */
	public int getuFlow() {
		return uFlow;
	}

	/**
	 * @param uFlow the uFlow to set
	 */
	public void setuFlow(int uFlow) {
		this.uFlow = uFlow;
	}

	/**
	 * @return the dFlow
	 */
	public int getdFlow() {
		return dFlow;
	}

	/**
	 * @param dFlow the dFlow to set
	 */
	public void setdFlow(int dFlow) {
		this.dFlow = dFlow;
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

	public void set(String phone, int uFlow, int dFlow) {
		
		this.phone = phone;
		this.uFlow = uFlow;
		this.dFlow = dFlow;
		this.amount = this.uFlow+this.dFlow;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "FlowObject [phone=" + phone + ", uFlow=" + uFlow + ", dFlow=" + dFlow + ", amount=" + amount + "]";
	}

	private String phone;
	private int uFlow;
	private int dFlow;
	private int amount;
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.phone=in.readUTF();
		this.uFlow=in.readInt();
		this.dFlow=in.readInt();
		this.amount=in.readInt();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.phone);
		out.writeInt(this.uFlow);
		out.writeInt(this.dFlow);
		out.writeInt(this.amount);
		
	}
	

}
