package cm.redis.syndata;

/**
用户app偏好信息
*/
public class UserAppPreferInfo {
	private String user_nbr="0";
	private String latlong="0";
	private String timestamp="0";
	private String appname="0";
	public String getUser_nbr() {
		return user_nbr;
	}
	public void setUser_nbr(String user_nbr) {
		this.user_nbr = user_nbr;
	}
	public String getLatlong() {
		return latlong;
	}
	public void setLatlong(String latlong) {
		this.latlong = latlong;
	}
	public String getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	public String getAppname() {
		return appname;
	}
	public void setAppname(String appname) {
		this.appname = appname;
	}


}
