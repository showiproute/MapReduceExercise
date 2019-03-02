package MRCodingTen_JoinAlgorithm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class BeanObject implements Writable{

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
	 * @return the age
	 */
	public int getAge() {
		return age;
	}
	/**
	 * @param age the age to set
	 */
	public void setAge(int age) {
		this.age = age;
	}
	/**
	 * @return the lover
	 */
	public String getLover() {
		return lover;
	}
	/**
	 * @param lover the lover to set
	 */
	public void setLover(String lover) {
		this.lover = lover;
	}
	public void set(String orderId, String userId, String userName, int age, String lover,String tableName) {
		this.orderId = orderId;
		this.userId = userId;
		this.userName = userName;
		this.age = age;
		this.lover = lover;
		this.tableName=tableName;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "BeanObject [orderId=" + orderId + ", userId=" + userId + ", userName=" + userName + ", age=" + age
				+ ", lover=" + lover + "]";
	}
	
	private String orderId;
	private String userId;
	private String userName;
	private int age;
	private String lover;
	private String tableName;
	/**
	 * @return the tableName
	 */
	public String getTableName() {
		return tableName;
	}
	/**
	 * @param tableName the tableName to set
	 */
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.orderId=in.readUTF();
		this.userId=in.readUTF();
		this.userName=in.readUTF();
		this.age=in.readInt();
		this.lover=in.readUTF();
		this.tableName=in.readUTF();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.orderId);
		out.writeUTF(this.userId);
		out.writeUTF(this.userName);
		out.writeInt(this.age);
		out.writeUTF(this.lover);
		out.writeUTF(this.tableName);
		
	}
	
}
