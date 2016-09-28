package cm.sjsn.commons;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
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

import cm.redis.commons.RedisServer;
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
		public File[] getExtractedFiles() 
		{
			return extractedFiles;
		}
		public void setExtractedFiles(File[] extractedFiles)
		{
			this.extractedFiles = extractedFiles;
		}

		/**
		 * 删除对应数据接口存放数据文件目录下的所有文件，也就是先做数据清理，只删除文件，不删除文件夹
		 * @param private_folder 基于ResourcesConfig.SYN_SERVER_DATAFILE之上的数据枢纽数据文件夹名称，最后不需要添加"/"
		 */
		public void deletefiles(String private_folder)
		{
			logger.info(" [deletefiles method] starts");
			try
			{
				String filepath=null;
				if(private_folder==null||private_folder.trim().equals(""))private_folder="default";
				filepath=ResourcesConfig.SYN_SERVER_DATAFILE+private_folder+"/";
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
			    	temp = new File(filepath + tempList[i]);  	//filepath已经带有File.separator
			    	if (temp.isFile()) {
			    		temp.delete();
			    	}
			    	if (temp.isDirectory()) {
			    		deletefiles(private_folder+"/" + tempList[i]);	//递归删除文件夹里面的文件
			    	}
			    }
			    logger.info(" [deletefiles method] ends successfully");
			}
			catch(Exception ex)
			{
				logger.info(" [deletefiles method] runing error: "+ex.getMessage());
			}
		}		
		
		//调用数据枢纽获取热点区域维表对应的接口文件信息
		/**
		 * 按照接口格式获取数据文件的对应下载结构信息
		 * @param add_url   数据枢纽http url路径
		 * @param json_params 获取文件信息所需的参数构成的json格式
		 * @return Ftpfilebasicinfo 返回文件结构内容
		 */
		public Ftpfilebasicinfo getfileinfo(String add_url, String json_params)
		{
			logger.info("[getfileinfo method] starts");
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
		 * @param private_folder 基于ResourcesConfig.SYN_SERVER_DATAFILE之上的数据枢纽数据文件夹名称，最后不需要添加"/"
		 * @return 下载成功返回true，否则返回false
		 */
		public boolean downloadfile(Ftpfilebasicinfo ftpinfo, String private_folder)
		{
			logger.info(" [downloadfile method] starts");
			boolean isdownldOK=false;
			FTPClient ftp=new FTPClient();
			FileOutputStream fos = null;
			String filepath=null;
			if(private_folder==null||private_folder.trim().equals(""))private_folder="default";
			filepath=ResourcesConfig.SYN_SERVER_DATAFILE+private_folder+"/";
			File destDir = new File(filepath); 
			if (!destDir.exists()) {  
	            destDir.mkdir();  
	        }  
			try
			{
				ftp.connect(ftpinfo.getHostip());
		  		ftp.login(ftpinfo.getFtpusername(),ftpinfo.getFtppassword());
				ftp.setControlEncoding("utf-8"); // 中文支持控制流
				ftp.setFileType(FTP.BINARY_FILE_TYPE); //以BINARY格式传送文件
				ftp.setBufferSize(1024 * 8);//设置缓冲区
				ftp.setDataTimeout(30 * 1000);//设置连接超时时间长度
				fos = new FileOutputStream(filepath+ftpinfo.getFilename()); //确定下载目录 for 服务器
				ftp.retrieveFile(ftpinfo.getFilepath(), fos);
				fos.close();
				logger.info(" [downloadfile method] downlaods: "+ftpinfo.getFilename()+" successfully");
				isdownldOK=true;
			}catch(Exception ex)
			{
				logger.info(" [downloadfile method] runing error: "+ex.getMessage());
				return false;
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
				return false;
			}
			return isdownldOK;
		}

		//将文件进行解压缩，获取目录下的所有解压出来的文件
		/**
		 * 解压缩已经下载的数据文件
		 * @param ftpinfo 数据文件的对应下载结构信息
		 * @param private_folder 基于ResourcesConfig.SYN_SERVER_DATAFILE之上的数据枢纽数据文件夹名称，最后不需要添加"/"
		 * @return true表示解压出了实际文件
		 */
		@SuppressWarnings("unchecked")
		public boolean unzipfilewithpassword(Ftpfilebasicinfo ftpinfo, String private_folder)
		{
			boolean isunzipOK=false;
			logger.info(" [unzipfile method] starts");
			extractedFiles=null;
			String filepath=null;
			if(private_folder==null||private_folder.trim().equals(""))private_folder="default";
			filepath=ResourcesConfig.SYN_SERVER_DATAFILE+private_folder+"/";
			File destDir = new File(filepath); 
			if (!destDir.exists()) {  
	            destDir.mkdir();  
	        }
			try
			{
				ZipFile zFile = new ZipFile(filepath+ftpinfo.getFilename());  
		        zFile.setFileNameCharset("UTF-8");  
		        if (!zFile.isValidZipFile()) {  
		            throw new ZipException("Zip file may be broken.");
		        } 
		        if (zFile.isEncrypted()) {  
		            zFile.setPassword(ftpinfo.getZippassword().toCharArray());  
		        }  
		        zFile.extractAll(filepath);  
		          
				List<FileHeader> headerList = zFile.getFileHeaders();  
		        List<File> extractedFileList = new ArrayList<File>();  
		        for(FileHeader fileHeader : headerList) {  
		            if (!fileHeader.isDirectory()) {  
		                extractedFileList.add(new File(destDir,fileHeader.getFileName()));  
		            }  
		        }  
		        if(extractedFileList.size()>0){
		        	extractedFiles = new File[extractedFileList.size()];  
		        	extractedFileList.toArray(extractedFiles);
		        	isunzipOK=true;
		        	logger.info(" [unzipfile method] unziped out " + extractedFileList.size() +" files");
		        }else{
		        	logger.info(" [unzipfile method] no files inside");
		        }
		        logger.info(" [unzipfile method] ends successfully");
			}catch(Exception ex)
			{
				logger.info(" [unzipfile method] runing error: "+ex.getMessage());
				return false;
			}
			return isunzipOK;
		}
		
		/**
		 * 直接获取已经存在的维表数据文件
		 * @param private_folder  private_folder 基于ResourcesConfig.SYN_SERVER_DATAFILE之上的维表数据所在文件夹名称，最后不需要添加"/"
		 * @return true表示存在文件，fasle表示文件不存在或者无效
		 */
		public boolean getreffilesdircetly(String private_folder){
			boolean isgetfilesOK=false;
			logger.info(" [get ref files directly method] starts");
			extractedFiles=null;
			String filepath=null;
			try{
				//仅处理private_folder下的维表文件，不处理文件夹
				if(private_folder==null||private_folder.trim().equals(""))filepath=ResourcesConfig.SYN_SERVER_DATAFILE;
				filepath=ResourcesConfig.SYN_SERVER_DATAFILE+private_folder+"/";
				File file = new File(filepath);
			    if (!file.exists()) {
			    	logger.info(" [getreffilesdircetly method] has no files");
			    	return false;
			    }
			    if (!file.isDirectory()) { 
			    	logger.info(" [getreffilesdircetly method] not a directory");
			    	return false;
			    }
			    String[] tempList = file.list();
			    File temp = null;
			    List<File> extractedFileList = new ArrayList<File>();
			    for (int i = 0; i < tempList.length; i++) {
			    	temp = new File(filepath + tempList[i]);  	//filepath已经带有File.separator
			    	if (temp.isFile()) {
			    		extractedFileList.add(temp);
			    	}
			    	if (temp.isDirectory())continue;
			    }
		        if(extractedFileList.size()>0){
		        	extractedFiles = new File[extractedFileList.size()];  
		        	extractedFileList.toArray(extractedFiles);
		        	isgetfilesOK=true;
		        	logger.info(" [getreffilesdircetly method] get " + extractedFileList.size() +" files");
		        }else{
		        	logger.info(" [unzipfile method] no files inside");
		        }
			    logger.info(" [getreffilesdircetly directly] ends successfully");
			}catch(Exception ex){
				logger.info(" [getreffilesdircetly method] runing error: "+ex.getMessage());
				return false;
			}
			return isgetfilesOK;
		}
		
		/**
		 * 检查解压后的文件是否有实际数据维表信息
		 * @param files 解压缩后的文件内容列表
		 * @return 文件含有维表信息数据，返回true，否则返回false
		 */
		public boolean checkreffilerecords(File [] files)
		{
			logger.info("[checkunzipfile method] starts");
			boolean isfilecontaininfo=true;
			if(files==null)return false;
			try{
				BufferedReader reader = null;
				int line = 0; 
				for(int i=0;i<files.length;i++)
				{
					line = 0;
					reader = new BufferedReader(new FileReader(files[i]));
					//一次读入一行
					while ((reader.readLine()) != null) {
						line=line+1;
						if(line>2)break; //检验存在数据跳出当前循环
					}
					reader.close();
					if(line<2)isfilecontaininfo=false;
				}
				logger.info("[checkunzipfile method] ends successfully");
			}
			catch(Exception ex)
			{
				logger.info("[checkunzipfile method] runing error: "+ex.getMessage());
			}
			return isfilecontaininfo;
		}
		
		
		/**
		 * 录入解压缩后的文件或者已经存在的维表相关文件到redis中
		 * @param files 解压缩后的文件内容列表
		 */
		public void processunzipfile(File [] files)
		{
			//这里假设允许将一个数据库文件拆分成多个数据库文件，所以采用这种File []的方式
			logger.info(" [processunzipfile method] starts");
			try
			{
				RedisServer redisserver=RedisServer.getInstance();
				BufferedReader reader = null;
				int choose=0;
				String key=null;
				String value=null;
				String [] recinfo=null;
				for(int i=0;i<files.length;i++)
				{
					reader = new BufferedReader(new FileReader(files[i]));
					String tempString = reader.readLine();//先读取第一行的数据
					choose=0;
					if(StringUtils.contains(tempString, "imsi"))choose=1; //读取imsi对应标签的维表
					else if(StringUtils.contains(tempString, "hotsid"))choose=2;//读取tac ci对应区域的标签维表
					else if(StringUtils.contains(tempString, "tac_ci"))choose=3; //读取tac ci对应相同经纬度tac ci的翻译维表
					else if(StringUtils.contains(tempString, "subid"))choose=4; //读取已经去重的subid与webtag 大类别的翻译维表
					while ((tempString = reader.readLine()) != null) {
						// 以下则是逐行将数据做转换，录入redis数据库
						recinfo=tempString.split(";"); //按照分号划分获取字段
						switch(choose){
							case 1: //imsi对应维表
								key="ref_tags_"+recinfo[0].trim();
								value=recinfo[1].trim();
								redisserver.set(key, value);
								break;
							case 2: //hotsid tac ci对应维表，接口读取过来的数据 3 hotspot id, 4 tac, 5 ci
								key=recinfo[5].trim();
								if(key.equals("0")==true)recinfo[5]="none";
								key="ref_hsp_"+recinfo[4].trim()+"_"+recinfo[5].trim();
								value=recinfo[3].trim();
								redisserver.sadd(key, value);
								key="ref_hsp_set"; //整理出所有项目，当天最新的全部热点区域
								redisserver.sadd(key, value);
								break;
							case 3: //tac_ci posid对应维表
								key="ref_hpm_"+recinfo[0].trim();
								value=recinfo[1].trim();
								redisserver.set(key, value);
								break;
							case 4: //subid webtag 大类英文维表
								key="ref_wtag_"+recinfo[0].trim();
								value=recinfo[1].trim();
								redisserver.set(key, value);
								break;
							default:
								logger.info(" [processunzipfile method] no import method for current ref file to Redis.");
								break;
						}
					}
					reader.close();
				}
				logger.info(" [processunzipfile method] ends successfully");
			}
			catch(Exception ex)
			{
				logger.info(" [processunzipfile method] runing error: "+ex.getMessage());
			}
		}
		
		/**
		 * 更新4G网分维表信息-对外接口
		 * @param sjsn_id 接口提供的id
		 * @param private_folder 存放数据的对应文件夹名称，由用户自定义 最后不需要添加"/"
		 */
		public void ref_data_syn(String sjsn_id, String private_folder){
			String url=null;
			String json=null;
			Ftpfilebasicinfo ftpinfo=null;
			File[] dlfiles=null;
			boolean check=false;

			if(sjsn_id==null||sjsn_id.trim().equals("")==true){
				check=getreffilesdircetly(private_folder);
				if(check==true)
				{
					dlfiles=getExtractedFiles();
					check=checkreffilerecords(dlfiles);
					if(check==true)processunzipfile(dlfiles);
				}
			}else{
				if(private_folder==null||private_folder.trim().equals(""))private_folder="default";//默认制定一个文件夹
				url="http://10.245.254.110:8080/etl_platform/rest/service.shtml";
				json= "{ \"identify\": \""+sjsn_id+"\", \"userName\": \"STORM\", \"password\": \"Srm_xxy_2016\", \"systemName\": \"STORM\"}";
				deletefiles(private_folder); 
				ftpinfo=getfileinfo(url,json);
				check=downloadfile(ftpinfo,private_folder);
				if(check==true)
				{
					check=unzipfilewithpassword(ftpinfo, private_folder);
					if(check==true){
						dlfiles=getExtractedFiles();
						check=checkreffilerecords(dlfiles);
						if(check==true)processunzipfile(dlfiles); //
					}
				}
			}
		}
		
		/**
		 * 测试主函数
		 * @param args
		 */
		public static void main(String[] args){
//			G4jk_ref_Syn g4jk_ref_Syn=new G4jk_ref_Syn();

			//测试录入redis成功，可以用于手动导入维表信息
//			File [] getFiles=null;
//			File refdata=null;
//			List<File> FileList = new ArrayList<File>();
//			refdata=new File("E:/WorkSpace/tb_mofang_custtag_ref.txt");
//			FileList.add(refdata);
//			refdata=new File("E:/WorkSpace/data.txt");//tb_mofang_hotspot_ref.txt
//			FileList.add(refdata);
//			refdata=new File("E:/WorkSpace/tb_mofang_tcsll_ref.txt");
//			FileList.add(refdata);
//			refdata=new File("E:/WorkSpace/tb_mofang_webtag_ref.txt");
//			FileList.add(refdata);
//			getFiles=new File[FileList.size()];
//			FileList.toArray(getFiles);
//			g4jk_ref_Syn.processunzipfile(getFiles);
			
			//测试接口下载文件，成功
			//g4jk_ref_Syn.ref_data_syn("d243c012-5ef5-4537-ad75-21c4b90fe74f",null);
			//g4jk_ref_Syn.ref_data_syn("d243c012-5ef5-4537-ad75-21c4b90fe74f","custtag");
			//g4jk_ref_Syn.ref_data_syn("c1ed7776-a16b-4472-a1bd-954df3925466","hotspot");
		}
}

//logger.info(fileinfo.getMd5value()+","+
//fileinfo.getFtppassword()+","+
//fileinfo.getFtppath()+","+
//fileinfo.getFtpusername()+","+
//fileinfo.getFilestatus()+","+
//fileinfo.getZippassword()); //输出测试
