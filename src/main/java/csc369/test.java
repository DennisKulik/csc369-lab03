import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.regex.*;

public class test {
    public static void main(String[] args) {
        String line1 = "64.242.88.10 - - [07/Mar/2004:16:10:02 -0800] \"GET /mailman/listinfo/hsdivision HTTP/1.1\" 200 6291";
        String line2 = "h24-71-249-14.ca.shawcable.net - - [07/Mar/2004:22:29:41 -0800] \"POST /mailman/options/cnc_notice HTTP/1.1\" 200 3533";
        String line3 = "127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326 \"http://www.example.com/start.html\" \"Mozilla/4.08 [en] (Win98; I ;Nav)\"";
        String line4 = "0x503e4fce.virnxx2.adsl-dhcp.tele.dk,Denmark";
        String line5 = "Australia,1	/mailman/listinfo/cnc_notice";

        String[] parts1 = line1.split(" ");
        String[] parts2 = line4.split(",");

        ArrayList<String> list1 = new ArrayList<>(Arrays.asList(parts1));
        ArrayList<String> list2 = new ArrayList<>(Arrays.asList(parts2));
        List<Object> list3 = new ArrayList<>(List.of(parts2[0], List.of(parts2[1], "2")));
        List<Object> list4 = new ArrayList<>(List.of(parts1[0], List.of("1", "1")));
        List<Object> list5 = new ArrayList<>(List.of(parts1[0], List.of(parts1[6], "2")));
        System.out.println(line5.replace("\t", "\\t"));

        String[] parts = line5.split("\t");
        
        // list1.remove(1);
        // list1.remove(1);
        // list1.set(1, list1.get(1).substring(1));
        // list1.set(2, list1.get(2).substring(1, list1.get(2).length() - 1));
        // list1.set(3, list1.get(3).substring(4, list1.get(3).length() - 9));
        // list1.set(5, list1.get(5).substring(0, list1.get(5).length() - 1));
        
        System.out.println(parts[0]);
    }
// [64.242.88.10, -, -, [07/Mar/2004:16:10:02, -0800], "GET, /mailman/listinfo/hsdivision, HTTP/1.1", 200, 6291]
// [64.242.88.10, (1, 1)]
// [0x503e4fce.virnxx2.adsl-dhcp.tele.dk, Denmark]
// [0x503e4fce.virnxx2.adsl-dhcp.tele.dk, (Denmark, 2)]
}
