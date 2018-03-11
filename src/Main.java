
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

public class Main {	
	public static void main(String[] args) {
		// TODO Auto-generated method stub	
		Instant starttime = Instant.now();
		String fileName="doc/pg5.csv";
		fileParseAndStore(fileName);
		Instant endtime = Instant.now();
		System.out.println(Duration.between(starttime, endtime));
	}
	public static void fileParseAndStore(String fileName) {
		Connection con=null;
		CSVReader csvReader=null;
 		try {
 			Reader reader = Files.newBufferedReader(Paths.get(fileName));		 		    
 			csvReader =new CSVReaderBuilder(reader).withSkipLines(1).build();  		 		     
 			con=C3P0DataSource.getInstance().getConnection();
 			PreparedStatement ps=null;
 			String[] data;
 			for(int i=0;(data=csvReader.readNext())!=null;i++)
 			{  
			    try 
			    {
			    	ps=con.prepareStatement("insert into pg values(?,?,?,?,?)");
				    ps.setInt(1,Integer.parseInt(data[0]));
				    ps.setString(2,(data[1]));
				    ps.setString(3,(data[2]));
				    ps.setString(4,(data[3]));
				    ps.setInt(5,Integer.parseInt(data[4]));
				    int r=ps.executeUpdate();
				    if(r==1) {
				    	
				    	System.out.println("/****************************************Processed  " +i+ "th Record********************************************************************************/");
			    	}
			    }
			    catch (SQLException e) {
					// TODO Auto-generated catch block
			    	e.printStackTrace();
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
				   		e.printStackTrace();
					}
			    	
   				} 
			}
	   	}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
 				e.printStackTrace();
 			} catch (IOException e) {
 				// TODO Auto-generated catch block
 				e.printStackTrace();
 			}
	   }
	}
}