
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

public class Main {
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
	private static HashMap<String,String[]> tableDescription()
	{
		HashMap<String,String[]> tableMappingDesc=new HashMap<String,String[]>();
		Connection con=C3P0DataSource.getInstance().getConnection();
		try {
			PreparedStatement pStatement=con.prepareStatement("select * from descPG ");
			ResultSet rs=pStatement.executeQuery();
			
			while(rs.next())
			{
				tableMappingDesc.put(rs.getString(1),new String[]{rs.getString(2),rs.getString(3)});
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
	private static String colTypeModifier(String colDatabaseDataType,String csvColData)
	{
		String result="";
		switch(colDatabaseDataType)
		{
			case "VARCHAR2":
			case "CHAR":
			case "NVARCHAR2":
			case "CLOB":
			case "NLOB":
			case "NCHAR":
			case "DATE":
				result="'"+csvColData+"'";
				break;
			default:
				result=csvColData;
		}
		return result;
	}
	private static String sqlBuilder(String[] data,HashMap<String,String[] > tableMappingDesc,HashMap<String,Integer> header)
	{
		String sql="insert into PG";
		String col="(";
		String coldata="values(";
		int i=0;
		String[] desc;
		for(Map.Entry<String, String[]> entry:tableMappingDesc.entrySet())
		{
			if(i>0)
			{
				col+=",";
				coldata+=",";
			}
			desc=entry.getValue();
			col+=desc[0];
			coldata+=colTypeModifier(desc[1],data[header.get(entry.getKey().toUpperCase())]);
			i++;
		}
		col=col+")";
		coldata=coldata+")";
		sql=sql+col+coldata;
		return sql;
	}
	final static Logger logger = Logger.getLogger("Global Logger");
	private static void errorinfo(Connection con,Exception e,int rownum)
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
			e1.printStackTrace();
		}
		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub	
		String fileName="doc/pg5.csv";
		HashMap<String,String[]> tableDesc=tableDescription();
		HashMap<String,Integer> csvHeader=csvHeader(fileName);
		Instant starttime = Instant.now();
		fileParseAndStore(fileName,tableDesc,csvHeader);
		Instant endtime = Instant.now();
		System.out.println(Duration.between(starttime, endtime));
	}

	private static void fileParseAndStore(String fileName,HashMap<String,String[]> tableDesc,HashMap<String,Integer> csvHeader) {
		Connection con=null;
		CSVReader csvReader=null;
 		try {
 			Reader reader = Files.newBufferedReader(Paths.get(fileName));		 		    
 			csvReader =new CSVReaderBuilder(reader).withSkipLines(1).build();
 			String[] data;
 			con=C3P0DataSource.getInstance().getConnection();
 			PreparedStatement ps=null;
 			int i;
 			for(i=0;(data=csvReader.readNext())!=null;i++)
 			{  
			    try 
			    {
			    	String sql=sqlBuilder(data, tableDesc,csvHeader);
			    	ps=con.prepareStatement(sql);
				    int r=ps.executeUpdate();
				    if(r==1) {	
				    	logger.info("/****************************************Processed  " +i+ "th Record********************************************************************************/");
				    				    	}
			    }
			    catch (SQLException e) {
					// TODO Auto-generated catch block
			    	errorinfo(con,e, i);
			    	
			    	logger.error(e.toString());
				}
			    finally {
			    	try {
					   if(ps!=null)
					   {
						   ps.close();
						   
					   }
					} 
			    	catch (SQLException e) {
						// TODO Auto-generated catch block
			    		errorinfo(con,e, i);
			    		
			    		logger.error(e.toString());
					}
   				} 
			}
	   	}catch (Exception e) {
			// TODO Auto-generated catch block
	   		errorinfo(con,e, 0);
	   		
	   		logger.error(e.toString());
   		}
 		finally 
 		{
 			try {
 				if(con!=null)
 				{
 					con.close();
 				}
 				if(csvReader!=null)
 				{
 					csvReader.close();
 				}
 			} catch (SQLException e) {
 				// TODO Auto-generated catch block
 				errorinfo(con,e, 0);
 				logger.error(e.toString());
 				
 			} catch (IOException e) {
 				// TODO Auto-generated catch block
 				errorinfo(con,e, 0);
 				logger.error(e.toString());
 				
 			}
	   }
	}
}