package cm.sjsn.commons;

/**
 * 用于记录数据枢纽接口返回的数据文件下载相关的详细信息
 * @author chinamobile
 * 2016-09-06
 */
public class Ftpfilebasicinfo {
	private String ftppath="";
	private String hostip="";
	private String filepath="";
	private String filename="";
	private String ftpusername="";
	private String ftppassword="";
	private String filestatus="";
    private String zippassword="";
    private String md5value="";
	public String getFtppath() {
		return ftppath;
	}
	public void setFtppath(String ftppath) {
		this.ftppath = ftppath;
	}
	public String getHostip() {
		return hostip;
	}
	public void setHostip(String hostip) {
		this.hostip = hostip;
	}
	public String getFilepath() {
		return filepath;
	}
	public void setFilepath(String filepath) {
		this.filepath = filepath;
	}
	public String getFilename() {
		return filename;
	}
	public void setFilename(String filename) {
		this.filename = filename;
	}
	public String getFtpusername() {
		return ftpusername;
	}
	public void setFtpusername(String ftpusername) {
		this.ftpusername = ftpusername;
	}
	public String getFtppassword() {
		return ftppassword;
	}
	public void setFtppassword(String ftppassword) {
		this.ftppassword = ftppassword;
	}
	public String getFilestatus() {
		return filestatus;
	}
	public void setFilestatus(String filestatus) {
		this.filestatus = filestatus;
	}
	public String getZippassword() {
		return zippassword;
	}
	public void setZippassword(String zippassword) {
		this.zippassword = zippassword;
	}
	public String getMd5value() {
		return md5value;
	}
	public void setMd5value(String md5value) {
		this.md5value = md5value;
	}
}
