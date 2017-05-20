package cm.redis.commons;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import org.apache.log4j.Logger;

/**
 * 专门用于写文件，处理增量数据
 * @author chinamobile
 *
 */
public class FileServer {
	//单例模式实现客户端管理类
	private static FileServer INSTANCE=new FileServer();
	
	public static Logger logger=Logger.getLogger(FileServer.class);

	/**
	 * 获取文件管理器的唯一实例，单例模式
	 * @return
	 */
	public static FileServer getInstance() {
		if(INSTANCE==null){
			synchronized (FileServer.class) {
				if(INSTANCE==null){
					INSTANCE=new FileServer();
				}
			}
		}
		return INSTANCE;
	}
	
	/**
	 * 将新增行写入到文件中
	 * @param contentline：需要新增到文件末尾的内容行
	 */
	public void setWordsToFile(String contentline, String filepath){        
        try {
        	boolean flag=false;
        	String tdate=TimeFormatter.getDate(); //当前日期的时间格式
        	String file_prefix=filepath;
        	String file_postfix=null;
        	String file_name=null;
        	File stormfile=null;
        	RandomAccessFile randomFile =null;
        	FileChannel fcout=null;
        	FileLock flout=null;
        	// 文件长度，字节数     
            long fileLength = 0;          
           
    		fileLength = 0; 
        	flag=false;
        	file_postfix=tdate+"_userbehaviors.txt";
        	file_name=file_prefix+file_postfix;
        	
        	stormfile=new File(file_name);
            if(!stormfile.exists())  
            	stormfile.createNewFile();
                
            //尝试对文件加锁  
            randomFile  = new RandomAccessFile(stormfile, "rw");  
            fcout=randomFile .getChannel();

            try {  
                flout = fcout.tryLock();
                flag=true; 
			} catch (Exception e) {
				flag=false;
			}

    		if(flag==true){
    			fileLength=randomFile.length();
                randomFile.seek(fileLength);
    			randomFile.write(contentline.getBytes("utf-8"));
                flout.release();  
                fcout.close();  
                randomFile.close();
    		}
    		
			flag=false;
			tdate=null;
        	file_prefix=null;
        	file_postfix=null;
        	file_name=null;
        	stormfile=null;
        	randomFile=null;
        	fcout=null;
        	flout=null;
        } catch (Exception e) {  
            //保存文件出错
        }
	}
}
