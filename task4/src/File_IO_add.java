import org.apache.commons.csv.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class File_IO_add {

    private String filePath1 = "C:\\Users\\lenovo\\Desktop\\学习资料（非常滴珍贵）\\大二上\\DataBase_project1\\source\\data\\users.csv";
    private String generatedFilePath = "C:\\Users\\lenovo\\Desktop\\generated_users.csv"; // Path to the generated users CSV

    public static void main(String[] args) {
        File_IO_add fileIOAdd = new File_IO_add();
        fileIOAdd.appendGeneratedUsers(fileIOAdd.generatedFilePath, fileIOAdd.filePath1);
    }

    public void appendGeneratedUsers(String generatedFilePath, String existingFilePath) {
        long startTime = System.nanoTime(); // 开始计时

        List<String[]> newUsersRecords = readCSV(generatedFilePath);
        appendCSV(existingFilePath, newUsersRecords);

        long endTime = System.nanoTime(); // 结束计时

        // 计算所用时间并转换为秒
        double elapsedTimeInSeconds = (endTime - startTime) / 1_000_000_000.0;
        System.out.println("将数据追加到users.csv文件花费的时间: " + elapsedTimeInSeconds + " 秒");
    }

    public List<String[]> readCSV(String filePath) {
        List<String[]> records = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] data = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1); // Split on comma, ignore commas in quotes
                records.add(data);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return records;
    }

    public void appendCSV(String filePath, List<String[]> newRecords) {
        // Use try-with-resources to ensure the writer is closed after use.
        try (Writer out = new FileWriter(filePath, true);
             CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT)) {
            for (String[] record : newRecords) {
                printer.printRecord((Object[]) record);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
