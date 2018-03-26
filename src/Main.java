
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;


import org.apache.commons.lang.mutable.MutableInt;
import org.apache.log4j.Logger;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

public class Main {
	static HashMap<Long, int[]> threadStatus=new HashMap<Long, int[]>();
	static int start;
	static int end;
	static long  threadHashCode;
	private final static int BATCH_SIZE=100;
	private static  String SQL="";
	private static final String TABLE_NAME="members";
	private static final String DESC_TABLE_NAME="DESCmembers";
	static HashMap<String,HashMap<String,String>> tableMappingDesc=new HashMap<String,HashMap<String,String>>();
	final static Logger logger = Logger.getLogger("Global Logger");
	
	private static HashMap<String,Integer> tableMetaData()
	{
		HashMap<String,Integer> tableMeta=new HashMap<String,Integer>();
		Connection con=C3P0DataSource.getInstance().getConnection();
		try {
			PreparedStatement pStatement=con.prepareStatement("select * from "+TABLE_NAME);
			ResultSetMetaData rsmd=pStatement.getMetaData();
			SQL="insert into "+TABLE_NAME+" values(";
			for(int i=1;i<=rsmd.getColumnCount();i++)
			{	
				if(i>1)
				{
					SQL+=",";
				}
				SQL+="?";
				tableMeta.put(rsmd.getColumnName(i),i);
			}
			
			SQL+=")";
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			logger.error(e1.toString());
			
		}
		return tableMeta;
	}
	

	private static HashMap<String,Integer> csvHeader(String fileName)
	{
		HashMap<String,Integer> header=new HashMap<String,Integer>();
		CSVReader csvReader=null;
		Reader reader;
		try {
			reader = Files.newBufferedReader(Paths.get(fileName));
			csvReader =new CSVReaderBuilder(reader).build();
			String[] data;
			data=csvReader.readNext();
			for(int j=0;j<data.length;j++)
			{
				header.put(data[j], j);
			}
			csvReader.close();
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error(e.toString());
		}		 		    
			
		return header;
	}
	
	
	private static HashMap<String,HashMap<String,String>> tableDescription()
	{
		
		Connection con=C3P0DataSource.getInstance().getConnection();
		try {
			PreparedStatement pStatement=con.prepareStatement("select * from "+DESC_TABLE_NAME);
			ResultSet rs=pStatement.executeQuery();
			HashMap<String,String> dbDesc=null;
			while(rs.next())
			{	
				if(tableMappingDesc.containsKey(rs.getString(2)))
				{
					dbDesc=tableMappingDesc.get(rs.getString(2));
					
				}
				else
				{
					dbDesc=new HashMap<String,String>();
				}
				dbDesc.put(rs.getString(1),rs.getString(3));
				tableMappingDesc.put(rs.getString(2),dbDesc);
			}
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			logger.error(e1.toString());
			
		}
		finally{
			try {
				if(con!=null)
				{
					con.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error(e.toString());
				
			}	
		}
		return tableMappingDesc;
	}

	
	
	private synchronized static void errorinfo(Connection con,Exception e,int rownum)
	{
		PreparedStatement ps=null;
		try {
			ps=con.prepareStatement("insert into errortable values(?,?)");
			ps.setInt(1, rownum);
			ps.setString(2, e.getMessage());
			if(ps.executeUpdate()==1)
			{
				logger.info("Error Msg Inserted");
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			logger.error(e.toString());
		}
		
	}
	
	private synchronized static void serialize()
	{
		FileOutputStream fileOut;
		try {
		
			fileOut = new FileOutputStream("write_record.ser");
			 ObjectOutputStream out = new ObjectOutputStream(fileOut);
			 out.writeObject(threadStatus);
			 out.close();
			 fileOut.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); 	
		}
	}

	
	@SuppressWarnings("unchecked")
	private static HashMap<Long, int[]> deSerialize() {
		HashMap<Long, int[]> prevThreadStatus=null;
		File file=new File("write_record.ser");
		if(file.exists())
		{
			try {
				FileInputStream fileIn = new FileInputStream("write_record.ser");
				ObjectInputStream in = new ObjectInputStream(fileIn);
				prevThreadStatus = (HashMap<Long, int[]>) in.readObject();
				in.close();
				fileIn.close();
				
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				logger.error(e.toString());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				logger.error(e.toString());
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				logger.error(e.toString());
			}
			
		}
		return prevThreadStatus;
	}
	
	
	public static void main(String[] args) throws IOException {
		HashMap<Long, int[]> prevThreadStatus=deSerialize();
		// TODO Auto-generated method stub
		String fileName="doc/members2.csv";
		HashMap<String,HashMap<String,String>> tableDesc=tableDescription();
		HashMap<String,Integer> csvHeader=csvHeader(fileName);
		HashMap<String,Integer> tableMetaData=tableMetaData();
		LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(fileName));
		lineNumberReader.skip(Long.MAX_VALUE);
        int lines = lineNumberReader.getLineNumber();
      	lineNumberReader.close();
      	start=1;
      	end=lines;
      	threadHashCode=Thread.currentThread().hashCode();
      	threadStatus.put((long)threadHashCode,new int[] {start,end,start-1});
		Instant starttime = Instant.now();
		if(prevThreadStatus!=null)
		{
			System.out.println("kk");
			for(Map.Entry<Long,int[]> entry:prevThreadStatus.entrySet()){    
		        int[] b=entry.getValue();  
		        if(b[2]<b[1])
		        {
		        	start=b[2]+1;
		        	end=b[1];
		        	threadStatus.put((long)threadHashCode,new int[] {start,end,start-1});
		        	fileParseAndStore(fileName,tableDesc,csvHeader,tableMetaData);
					
		        }
		    }
		}
		else
		{
			System.out.println("hh");
			threadStatus.put((long)threadHashCode,new int[] {start,end,start-1});
			fileParseAndStore(fileName,tableDesc,csvHeader,tableMetaData);
				
			}
		
		boolean status=true;
		for(Map.Entry<Long, int[]> entry:threadStatus.entrySet()){    
	        int[] b=entry.getValue();  
	        if(b[2]<b[1]-1)
	        {
	        	System.out.println(b[2]+"yyyy"+b[1]);
	        	status=false;
	        	break;
	        }
	    }
		if(status)
		{
			try {
				if(Files.deleteIfExists(Paths.get("write_record.ser")))
				{
					System.out.println("File Deleted");
					logger.info("Ser File Deleted");
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				logger.error(e.toString());
			}
		}
		Instant endtime = Instant.now();
		
		System.out.println(Duration.between(starttime, endtime));
	}

	@SuppressWarnings("null")
	private static void fileParseAndStore(String fileName,HashMap<String,HashMap<String,String>> tableDesc,HashMap<String,Integer> csvHeader,HashMap<String,Integer> tableMetaData) {
		Instant funcStartTime=Instant.now();
		MutableInt rowcount=new MutableInt();
		HashMap<Integer,Duration> singleTimer=new HashMap<Integer,Duration>();
		long current=start-1;
		String sql=SQL;
		Connection con=null;
		CSVReader csvReader=null;
		long i=0;
		PreparedStatement ps=null;
 		try {
 			Reader reader = Files.newBufferedReader(Paths.get(fileName));
 			csvReader =new CSVReaderBuilder(reader).withSkipLines(start).build();
// 			String[] data = null;
 			con=C3P0DataSource.getInstance().getConnection();
// 			end=400;
 			con.setAutoCommit(false);
 			for(i=start;i<end;i++)
 			{
 				Object[] data=csvReader.readNext();
 				if((i-start+1)%BATCH_SIZE==1)
				{
					ps=con.prepareStatement(sql);
				}
 				for(Map.Entry<String, HashMap<String,String>> mapEntry:tableMappingDesc.entrySet())
		    	{
		    		HashMap<String,String> csvHeaderTemp=mapEntry.getValue();
		    		if(csvHeaderTemp.size()==1)
		    		{
		    			for(Map.Entry<String,String> entry:csvHeaderTemp.entrySet())
			    		{
			    			DbDataTypeEnum var=DbDataTypeEnum.valueOf(entry.getValue());
			    			if(var.getter().equals(BigDecimal.class))
			    			{
			    				ps.setBigDecimal(tableMetaData.get(mapEntry.getKey()), new BigDecimal((String)data[csvHeader.get(entry.getKey())]));
			    			}
			    			else
			    			{
			    				
			    				ps.setObject(tableMetaData.get(mapEntry.getKey()), var.getter().cast((String)data[csvHeader.get(entry.getKey())]));
			    			}
			    		}
		    		}
		    		
		    	}
 				ps.addBatch();
 				
 				
 				if((i-start+1)%BATCH_SIZE==0)
				{
					int[] update=ps.executeBatch();
					current+=update.length;
					for(int k=0;k<update.length;k++)
					{
						logger.info("/****************************************Processed  " +(i-BATCH_SIZE+k+1)+ "th Record********************************************************************************/");	
					}
					con.commit();
					Duration takenTime=Duration.between(funcStartTime, Instant.now());
					rowcount.add(BATCH_SIZE);
					singleTimer.put(rowcount.toInteger(), takenTime);
					int[] recordStatus=threadStatus.get(threadHashCode);
					recordStatus[2]=(int) current;
					threadStatus.put(threadHashCode, recordStatus);
					serialize();
					ps.close();
				}
 			}
 			int[] update=ps.executeBatch();
			current+=update.length;
			for(int k=0;k<update.length;k++)
			{
				logger.info("/****************************************Processed  " +(i-update.length+k+1)+ "th Record********************************************************************************/");
			}
			con.commit();
			int[] recordStatus=threadStatus.get(threadHashCode);
			recordStatus[2]=(int) current;
			threadStatus.put(threadHashCode, recordStatus);
			serialize();
 		}
 		 catch (FileNotFoundException e) {
 			// TODO Auto-generated catch block
 			errorinfo(con,e,(int) i);
     		logger.error(e.toString());
 		}
 		catch (BatchUpdateException buex) {
 			errorinfo(con,buex,(int) i-BATCH_SIZE+1);
     		logger.error(buex.toString());
 			try {
 				con.rollback();
 			} catch (SQLException e) {
 				// TODO Auto-generated catch block
 				e.printStackTrace();
 			}
 		}
 		catch ( SQLException e) {
 			// TODO Auto-generated catch block
 			errorinfo(con,e,(int) i);
     		logger.error(e.toString());
 		} catch (IOException e) {
 			// TODO Auto-generated catch block
 			errorinfo(con,e,(int) i);
     		logger.error(e.toString());
 		}
 		finally {
 			try {
 			
 				con.close();
 				csvReader.close();
 			} catch (SQLException | IOException e) {
 				// TODO Auto-generated catch block
 				errorinfo(con,e,(int) i);
 	    		logger.error(e.toString());
 			}
 		}
 		serializeSingleTimer(singleTimer);		
	}
	private static void serializeSingleTimer(HashMap<Integer,Duration> singleTimer)
	{
		FileOutputStream fileOut;
		try {
			fileOut = new FileOutputStream("D:/single_timer.ser");
			 ObjectOutputStream out = new ObjectOutputStream(fileOut);
			 out.writeObject(singleTimer);
			 out.close();
			 fileOut.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
