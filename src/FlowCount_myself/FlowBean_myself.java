package FlowCount_myself;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean_myself implements Writable{
	
	private String phone;
	private int upflow;
	private int dflow;
	private int amount;
	
	public  FlowBean_myself() {
		// TODO Auto-generated constructor stub
	}
	
	public FlowBean_myself(String phone,int upflow,int dflow) {
		this.phone=phone;
		this.upflow=upflow;
		this.dflow=dflow;
		this.amount=this.dflow+this.upflow;
	}
	
	
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
	 * @return the upflow
	 */
	public int getUpflow() {
		return upflow;
	}

	/**
	 * @param upflow the upflow to set
	 */
	public void setUpflow(int upflow) {
		this.upflow = upflow;
	}

	/**
	 * @return the dflow
	 */
	public int getDflow() {
		return dflow;
	}

	/**
	 * @param dflow the dflow to set
	 */
	public void setDflow(int dflow) {
		this.dflow = dflow;
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


	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.phone=in.readUTF();
		this.upflow = in.readInt();
		this.dflow = in.readInt();
		this.amount = in.readInt();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.phone);
		out.writeInt(this.upflow);
		out.writeInt(this.dflow);
		out.writeInt(this.amount);
		
	}

}
