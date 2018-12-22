package FlowCount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/*
 * 本案例的功能：演示自定义数据类型 如何实现Hadoop的序列化接口
 * 1、该类一定要保留空参构造函数
 * 2、write方法中输出字段二进制数据的顺序，要与readFields方法读取数据的顺序一致
 */


public class FlowBean implements Writable{

	private int upFlow;
	private int dFlow;
	private int amountFlow;
	private String phone;
	
	public FlowBean() {
		
	}
	
	public FlowBean(String phone,int upFlow,int dFlow) {
		this.phone=phone;
		this.upFlow = upFlow;
		this.dFlow = dFlow;
		this.amountFlow = this.dFlow+this.upFlow;
	}
	
	/**
	 * @return the upFlow
	 */
	public int getUpFlow() {
		return upFlow;
	}
	/**
	 * @param upFlow the upFlow to set
	 */
	public void setUpFlow(int upFlow) {
		this.upFlow = upFlow;
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
	public int getAmountFlow() {
		return amountFlow;
	}
	public void setAmountFlow(int amountFlow) {
		this.amountFlow = amountFlow;
	}

	/*
	 * Hadoop系统在反序列化该类对象时要调用的方法
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.upFlow=in.readInt();
		this.phone = in.readUTF();
		this.dFlow =in.readInt();
		this.amountFlow=in.readInt();
	}

	/*
	 * Hadoop系统在序列化该类对象时要调用的方法 (调用write变成二进制字节数据)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
		out.writeInt(upFlow);
		out.writeUTF(phone);
		out.writeInt(dFlow);
		out.writeInt(amountFlow);
/*
两种方法
out.write(phone.getBytes());
out.writeUTF(phone);
*/
		
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

		@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.upFlow+","+this.dFlow+","+this.amountFlow;
	}
	
	

}
