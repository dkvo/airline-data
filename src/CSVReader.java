import java.io.*;
import java.util.ArrayList;

/**
 * Created by dangkhoavo on 2/15/17.
 */
public class CSVReader {

    public ArrayList<String> readFile(String filePath) {
        String currentLine = null;
        ArrayList<String> lines = new ArrayList<String>();

        File datafile = new File(filePath);
        // read file line by line excluding the first line
        try {
            BufferedReader in = new BufferedReader(new FileReader(datafile));

            in.readLine();
            while ((currentLine = in.readLine()) != null) {
                lines.add(new String(currentLine));
            }
            return lines;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
