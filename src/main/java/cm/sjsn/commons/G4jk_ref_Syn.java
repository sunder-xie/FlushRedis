package cm.sjsn.commons;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.log4j.Logger;

import cm.redis.commons.ResourcesConfig;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.FileHeader;


/**
 * 20160906 同步数据枢纽的数据接口方法
 * @author chinamobile
 *
 */
public class G4jk_ref_Syn {
		public static Logger logger=Logger.getLogger(G4jk_ref_Syn.class);
		
		private File [] extractedFiles=null;

		//调用数据枢纽获取热点区域维表对应的接口文件信息
		/**
		 * 按照接口格式获取数据文件的对应下载结构信息
		 * @param add_url   数据枢纽http url路径
		 * @param json_params 获取文件信息所需的参数构成的json格式
		 * @return
		 */
		public Ftpfilebasicinfo getfileinfo(String add_url, String json_params)
		{
			logger.info("[getfileinfo method] starts");
			//String add_url = "http://10.245.254.110:8080/etl_platform/rest/service.shtml";
	        //String json_params = "{\"identify\": \"26c068d5-5cf5-4951-9df4-0e597c4f0bbb\", \"userName\":\"ST_BIGDATA\",\"password\":\"Gmcc_345\",\"systemName\":\"STORM\",\"parameter\":{\"tm_intrvl_cd\":\"\",\"flux\":\"100\",\"amt\":\"20\"}}";
	        HttpClient client = new HttpClient();
	        
			PostMethod post = new PostMethod(add_url);
	        StringRequestEntity entity = null;
	        
	        //文件下载相关信息对象
	        Ftpfilebasicinfo fileinfo=new Ftpfilebasicinfo();
	        
	        try
	        {
	        	entity = new StringRequestEntity(json_params,null,"utf-8");
	        }catch(Exception ex)
	        {
	        	logger.info("[getfileinfo method] runing error: "+ex.getMessage());
	        }
	        
	        post.setRequestHeader("Content-Type","application/json");
	        post.setRequestEntity(entity);
	        
	        try{
	        	HostConfiguration hostConfiguration = new  HostConfiguration();
	        	String data = null;
	            StringBuffer bodyBuffer = new StringBuffer("");
	            int statusCode =0;
	            
	            while(StringUtils.contains(fileinfo.getFilestatus(), "running")==true)
	            {
	            	logger.info(" Data Service is running! ");
	            	statusCode=client.executeMethod(hostConfiguration, post);
	                if(statusCode==HttpStatus.SC_OK)
	                {
	                	BufferedReader br = new BufferedReader(new InputStreamReader(post.getResponseBodyAsStream(),"UTF-8"));
	    		    	while((data = br.readLine())!=null)
	    		    	{
	    		    		bodyBuffer.append(data);
	    		    	}
	    		    	br.close(); 
	                }
	                else
	                {
	                	bodyBuffer.append(statusCode);
	                }
	    	        String body = bodyBuffer.toString();
	    		    if(StringUtils.contains(body, "status"))
	    		    {
	    		    	body=StringUtils.substringBetween(body, "{","}");
	        		    body=body.replace("\"", ""); 
	        		    String[] tmplist=body.split(","); 
	        		    for(int i=0;i<tmplist.length;i++)
	        		    {
	        		    	String[] key_value=tmplist[i].split(":");
	        		    	if(StringUtils.equals(key_value[0], "MD5")==true)
	        		    	{
	        		    		if(key_value.length>1)fileinfo.setMd5value(key_value[1]);
	        		    	}
	        		    	else if(StringUtils.equals(key_value[0], "ftpPassword")==true)
	        		    	{
	        		    		if(key_value.length>1)fileinfo.setFtppassword(key_value[1]);
	        		    	}
	        		    	else if(StringUtils.equals(key_value[0], "ftpPath")==true) //ftp文件格式是 ftp://ip/路径/文件名，所以有如下的判断
	        		    	{
	        		    		if(key_value.length>2)
	        		    		{
	        		    			fileinfo.setFtppath(key_value[1]+":"+key_value[2]);
	        		    			key_value[2]=key_value[2].substring(key_value[2].indexOf("//")+2);  //文件路径按照格式是//ip/WEB/GUID/FILE构成
	            		    		fileinfo.setHostip(key_value[2].substring(0,key_value[2].indexOf("/")));
	            		    		fileinfo.setFilepath(key_value[2].substring(key_value[2].indexOf("/")));
	            		    		fileinfo.setFilename(key_value[2].substring(key_value[2].lastIndexOf("/")+1));
	        		    		}
	        		    	}
	        		    	else if(StringUtils.equals(key_value[0], "ftpUserName")==true)
	        		    	{
	        		    		if(key_value.length>1)fileinfo.setFtpusername(key_value[1]);
	        		    	}
	        		    	else if(StringUtils.equals(key_value[0], "status")==true)
	        		    	{
	        		    		if(key_value.length>1)fileinfo.setFilestatus(key_value[1].toLowerCase());//一律转成小写
	        		    	}
	        		    	else if(StringUtils.equals(key_value[0], "zipPassword")==true)
	        		    	{
	        		    		if(key_value.length>1)fileinfo.setZippassword(key_value[1]);
	        		    	}
	        		    	key_value=null;
	        		    }
	        		    tmplist=null;
	        		    /*logger.info(fileinfo.md5value+","+
	        		    		fileinfo.ftppassword+","+
	        		    		fileinfo.ftppath+","+
	        		    		fileinfo.ftpusername+","+
	        		    		fileinfo.filestatus+","+
	        		    		fileinfo.zippassword);*/ //输出测试
	    		    }
	    		    //logger.info(body);  //输出测试
	    	        bodyBuffer.delete(0, bodyBuffer.length()-1); //释放内存与清空缓冲区内容
	    	        if(StringUtils.contains(fileinfo.getFilestatus(), "running")==true)
	    	        {
	    	        	Thread.currentThread();
	    	        	Thread.sleep(10000); //休眠10秒之后重新进行http请求
	    	        }
	            }
	            logger.info(" [getfileinfo method] ends successfully");
	        }
	        catch(Exception ex)
	        {
	        	logger.info(" [getfileinfo method] runing error: "+ex.getMessage());
	        }
	        return fileinfo;
		}

		//文件数据下载到本地文件夹
		/**
		 * 依据数据文件的对应下载结构信息，进行文件下载
		 * @param ftpinfo 数据文件的对应下载结构信息
		 * @param private_folder 下载存放数据的对应文件夹
		 */
		public void downloadfile(Ftpfilebasicinfo ftpinfo, String private_folder)
		{
			logger.info(" [downloadfile method] starts");
			FTPClient ftp=new FTPClient();
			FileOutputStream fos = null;
			try
			{
				ftp.connect(ftpinfo.getHostip());
		  		ftp.login(ftpinfo.getFtpusername(),ftpinfo.getFtppassword());
				ftp.setControlEncoding("utf-8"); // 中文支持控制流
				ftp.setFileType(FTP.BINARY_FILE_TYPE); //以BINARY格式传送文件
				ftp.setBufferSize(1024 * 8);//设置缓冲区
				ftp.setDataTimeout(30 * 1000);//设置连接超时时间长度
				fos = new FileOutputStream(ResourcesConfig.SYN_SERVER_DATAFILE+private_folder+"/"+ftpinfo.getFilename()); //确定下载目录 for 服务器
				ftp.retrieveFile(ftpinfo.getFilepath(), fos);
				fos.close();
				logger.info(" [downloadfile method] downlaods: "+ftpinfo.getFilename()+" successfully");
			}catch(Exception ex)
			{
				logger.info(" [downloadfile method] runing error: "+ex.getMessage());
			}
			try
			{
				if (ftp.isConnected())  
				{    
					ftp.logout();  
				}
				ftp.disconnect(); 
				logger.info(" [downloadfile method] ends successfully");
			}catch(Exception ex)
			{
				logger.info(" [downloadfile method] runing error: "+ex.getMessage());
			}
		}
		
		
		//将文件进行解压缩，获取目录下的所有解压出来的文件
		/**
		 * 解压缩已经下载的数据文件
		 * @param ftpinfo 数据文件的对应下载结构信息
		 * @param private_folder 下载存放数据的对应文件夹
		 * @return true表示解压成功
		 */
		@SuppressWarnings("unchecked")
		public boolean unzipfilewithpassword(Ftpfilebasicinfo ftpinfo, String private_folder)
		{
			boolean isunzipOK=false;
			logger.info(" [unzipfile method] starts");
			extractedFiles=null;
			try
			{
				ZipFile zFile = new ZipFile(ResourcesConfig.SYN_SERVER_DATAFILE+private_folder+"/"+ftpinfo.getFilename());  
		        zFile.setFileNameCharset("UTF-8");  
		        if (!zFile.isValidZipFile()) {  
		            throw new ZipException("Zip file may be broken.");
		        }  
		        File destDir = new File(ResourcesConfig.SYN_SERVER_DATAFILE+private_folder+"/");  
		        if (destDir.isDirectory() && !destDir.exists()) {  
		            destDir.mkdir();  
		        }  
		        if (zFile.isEncrypted()) {  
		            zFile.setPassword(ftpinfo.getZippassword().toCharArray());  
		        }  
		        zFile.extractAll(ResourcesConfig.SYN_SERVER_DATAFILE+private_folder+"/");  
		          
				List<FileHeader> headerList = zFile.getFileHeaders();  
		        List<File> extractedFileList = new ArrayList<File>();  
		        for(FileHeader fileHeader : headerList) {  
		            if (!fileHeader.isDirectory()) {  
		                extractedFileList.add(new File(destDir,fileHeader.getFileName()));  
		            }  
		        }  
		        extractedFiles = new File[extractedFileList.size()];  
		        extractedFileList.toArray(extractedFiles);  
		        logger.info(" [unzipfile method] ends successfully");
		        if(extractedFileList.size()>0)isunzipOK=true;
			}catch(Exception ex)
			{
				logger.info(" [unzipfile method] runing error: "+ex.getMessage());
				return false;
			}
			return isunzipOK;
		}
		
		//处理解压缩后的文件
		/*public void processunzipfile(File [] files)
		{
			//这里假设允许将一个数据库文件拆分成多个数据库文件，所以采用这种File []的方式
			MyString.printLog(MyTime.getNow()+" [processunzipfile method] starts");
			try
			{
				BufferedReader reader = null;
				for(int i=0;i<files.length;i++)
				{
					reader = new BufferedReader(new FileReader(files[i]));
					String tempString = null;
					//int line = 1; 一次读入一行，直到读入null为文件结束
					while ((tempString = reader.readLine()) != null) {
						//
						// 显示文件内容的测试代码
						// MyString.printLog("line " + line + ": " + tempString);
						// line++;
						// if(line==100)break;

						// 以下则是逐行读取数据并判断写入memcache中
						if(StringUtils.contains(tempString, "tb"))continue; //如果读取到列名称行，不做处理
						
					}
					reader.close();
				}
				MyString.printLog(MyTime.getNow()+" [processunzipfile method] ends successfully");
			}
			catch(Exception ex)
			{
				MyString.printLog(MyTime.getNow()+" [processunzipfile method] runing error: "+ex.getMessage());
			}
		}*/
		
		/**
		 * 删除对应数据接口存放数据文件目录下的所有文件
		 * @param filepath 基于ResourcesConfig.SYN_SERVER_DATAFILE之上的数据枢纽数据文件夹名称
		 */
		public void deletefiles(String private_folder)
		{
			logger.info(" [deletefiles method] starts");
			try
			{
				String filepath=ResourcesConfig.SYN_SERVER_DATAFILE+private_folder+"/";
			    File file = new File(filepath);
			    if (!file.exists()) {
			    	logger.info(" [deletefiles method] has no files");
			    	return;
			    }
			    if (!file.isDirectory()) {
			    	logger.info(" [deletefiles method] not a directory");
			    	return;
			    }
			    String[] tempList = file.list();
			    File temp = null;
			    for (int i = 0; i < tempList.length; i++) {
			    	if (filepath.endsWith(File.separator)) {
			    		temp = new File(filepath + tempList[i]);
			    	} else {
			    		temp = new File(filepath + File.separator + tempList[i]);
			    	}
			    	if (temp.isFile()) {
			    		temp.delete();
			    	}
			    	if (temp.isDirectory()) {
			    		deletefiles(private_folder+"/" + tempList[i]);//递归删除文件夹里面的文件
			    	}
			    }
			    logger.info(" [deletefiles method] ends successfully");
			}
			catch(Exception ex)
			{
				logger.info(" [deletefiles method] runing error: "+ex.getMessage());
			}
		}
		
}
