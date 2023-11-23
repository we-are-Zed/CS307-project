import org.apache.commons.csv.*;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class File_IO {

    private String filePath1 = "C:\\Users\\lenovo\\Desktop\\学习资料（非常滴珍贵）\\大二上\\DataBase_project1\\source\\data\\users.csv";
    private String filePath2 = "C:\\Users\\lenovo\\Desktop\\学习资料（非常滴珍贵）\\大二上\\DataBase_project1\\source\\data\\videos.csv";
    private String filePath3 = "C:\\Users\\lenovo\\Desktop\\学习资料（非常滴珍贵）\\大二上\\DataBase_project1\\source\\data\\danmu.csv";
    private String[] header;

    public static void main(String[] args) {
        File_IO fileIO = new File_IO();
        fileIO.processFile(fileIO.filePath1);
    }

    public void processFile(String filePath) {
        long startTime = System.nanoTime(); // 开始计时

        List<String[]> records = readCSV(filePath);
        List<String[]> modifiedRecords = modifyRecords(records);
        writeCSV(filePath, modifiedRecords);

        long endTime = System.nanoTime(); // 结束计时

        // 计算所用时间并转换为秒
        double elapsedTimeInSeconds = (endTime - startTime) / 1_000_000_000.0;
        System.out.println("操作数据花费的时间: " + elapsedTimeInSeconds + " 秒");
    }

    public List<String[]> readCSV(String filePath) {
        List<String[]> records = new ArrayList<>();
        try (Reader in = new FileReader(filePath)) {
            CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);
            header = parser.getHeaderMap().keySet().toArray(new String[0]);  // 捕获头部
            for (CSVRecord record : parser) {
                String[] recordArray = new String[record.size()];
                for (int i = 0; i < record.size(); i++) {
                    recordArray[i] = record.get(i);
                }
                records.add(recordArray);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return records;
    }

    public List<String[]> modifyRecords(List<String[]> records) {
        for (int i = 0; i < records.size(); i++) {
            records.get(i)[0] = "1";
        }
        return records;
    }

    public void writeCSV(String filePath, List<String[]> records) {
        try (Writer out = new FileWriter(filePath)) {
            CSVPrinter printer = CSVFormat.DEFAULT.print(out);
            printer.printRecord(header);  // 首先写入头部
            for (String[] record : records) {
                printer.printRecord((Object[]) record);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
