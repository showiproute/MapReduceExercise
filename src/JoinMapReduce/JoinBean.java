package JoinMapReduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class JoinBean implements Writable{

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
	 * @return the userAge
	 */
	public int getUserAge() {
		return userAge;
	}
	/**
	 * @param userAge the userAge to set
	 */
	public void setUserAge(int userAge) {
		this.userAge = userAge;
	}
	/**
	 * @return the userFriend
	 */
	public String getUserFriend() {
		return userFriend;
	}
	/**
	 * @param userFriend the userFriend to set
	 */
	public void setUserFriend(String userFriend) {
		this.userFriend = userFriend;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "JoinBean [orderId=" + orderId + ", userId=" + userId + ", userName=" + userName + ", userAge=" + userAge
				+ ", userFriend=" + userFriend + "]";
	}
	
	public void set(String orderId, String userId, String userName, int userAge, String userFriend,String tableName) {
		this.orderId = orderId;
		this.userId = userId;
		this.userName = userName;
		this.userAge = userAge;
		this.userFriend = userFriend;
		this.tableName=tableName;
	}

	private String orderId;
	private String userId;
	private String userName;
	private int userAge;
	private String userFriend;
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
		this.orderId = in.readUTF();
		this.userId=in.readUTF();
		this.userName=in.readUTF();
		this.userAge=in.readInt();
		this.userFriend=in.readUTF();
		this.tableName=in.readUTF();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.orderId);
		out.writeUTF(this.userId);
		out.writeUTF(this.userName);
		out.writeInt(this.userAge);
		out.writeUTF(this.userFriend);
		out.writeUTF(this.tableName);
	}
	
}
