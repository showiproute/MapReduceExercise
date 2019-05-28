package HdfsClientDemo_myself;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class HdfsClient_Test {

	FileSystem fs=null;
	
	@Before
	public void init() throws Exception{
		Configuration conf = new Configuration();
		conf.set("dfs.replication", "3");
		conf.set("dfs.blocksize", "64m");
		
		fs=FileSystem.get(new URI("hdfs://namenode:9000"), conf, "root");
		
	}
	
	@Test
	public void testGet() throws Exception{
		
		fs.copyToLocalFile(new Path("/hdfspath"),new Path( "f://Linuxsource/mrdata/hdfsdata/"));
		
		fs.close();
		
	}
	
	@Test
	public void testRename() throws Exception{
		
		fs.rename(new Path("/hdfspath/data1"), new Path("/hdfspath/data2"));
		
		fs.close();
	}
	
	@Test
	public void testMkdirs() throws Exception{
		
		fs.mkdirs(new Path("/hdfspath2/"));
		
		fs.close();
	}
	
	@Test
	public void testRmdirs() throws Exception{
		fs.delete(new Path("/hdfspath2"), true);
		fs.close();
		
	}
	
	@Test
	public void testLS() throws Exception{
		
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/hdfspath"),true);
		
		while(listFiles.hasNext()) {
			LocatedFileStatus file = listFiles.next();

			System.out.println("BlockSize:"+file.getBlockSize());
			System.out.println("Replication:"+file.getReplication());
			System.out.println("Is directory?"+(file.isDirectory()?"directory":"not directory"));
			System.out.println("BlockDetailInfo"+file.getBlockLocations());
			
		}
		
		fs.close();
		
	}
	
	@Test
	public void testReadData() throws Exception{
		
		FSDataInputStream in = fs.open(new Path("/hdfspath/data1"));
		
		InputStreamReader inputStreamReader = new InputStreamReader(in, "utf-8");
		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
		
		String line=null;
		while((line=bufferedReader.readLine())!=null) {
			
			System.out.println(line);
			
		}
		
		bufferedReader.close();
		inputStreamReader.close();
		fs.close();
		
	}

	@Test
	public void testWriteData() throws Exception{
		
		FSDataOutputStream out= fs.create(new Path("/hdfspath/1.png"));
		FileInputStream in=new FileInputStream("F://Linuxsource/mrdata/test.png");
		
		int read=0;
		byte[] buf=new byte[1024];
		while((read=in.read(buf))!=-1) {
			out.write(buf, 0, read);
			
		}
		
		in.close();
		out.close();
		fs.close();
		
	}
	
	
	
}
