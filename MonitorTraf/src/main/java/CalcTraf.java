import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.pcap4j.core.NotOpenException;
import org.pcap4j.core.PacketListener;
import org.pcap4j.core.PcapHandle;
import org.pcap4j.core.PcapNativeException;
import org.pcap4j.core.PcapNetworkInterface;
import org.pcap4j.core.PcapNetworkInterface.PromiscuousMode;
import org.pcap4j.packet.Packet;
import org.pcap4j.util.NifSelector;


public class CalcTraf {
	
	private static Integer q = 0;

	public static void main(String[] args) throws PcapNativeException, IOException, NotOpenException, InterruptedException {
		//sudo java -cp MonitorTraf-0.0.1-SNAPSHOT-jar-with-dependencies.jar CalcTraf

     	PcapNetworkInterface nif = new NifSelector().selectNetworkInterface();
     	if (nif == null) {
     	System.exit(1);
     	}
     	 
     	final PcapHandle handle = nif.openLive(65536, PromiscuousMode.PROMISCUOUS, 10);
     	
     	PacketListener listener = new PacketListener() {
     		
     	@Override
     	public void gotPacket(Packet packet) {
				
	     		Pattern p = Pattern.compile("Total length: (\\d+)");
	    		Matcher m = p.matcher(packet.toString());
	    		m.find();
	    		Integer z = new Integer(m.group(1));
	    		q += z;
	    		System.out.println("pack");
	    		System.out.println(z);
	    		System.out.println("total");
	    		System.out.println(q);
	    		SendToKafka.sendToKafka("traffic", z.toString());
	    		
     	}
     	};
     	
     	handle.loop(-1, listener);
   	 
		}
	
	}
