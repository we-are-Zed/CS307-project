import org.apache.commons.csv.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class File_IO_seek {

    private static final String USER_FILE_PATH = "C:\\Users\\lenovo\\Desktop\\学习资料（非常滴珍贵）\\大二上\\DataBase_project1\\source\\data\\users.csv";

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        try {
            List<String[]> users = readCSV(USER_FILE_PATH);

            // 检查用户数量是否满足要求
            if (users.size() <= 10000) {
                System.out.println("Not enough users to select 10,000.");
                return;
            }

            // 随机选取10000个用户
            Collections.shuffle(users);
            List<String[]> selectedUsers = users.subList(0, 10000);

            // 输出这10000个用户的信息
            for (String[] user : selectedUsers) {
                System.out.println(Arrays.toString(user));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        System.out.println("Time taken: " + (endTime - startTime) + "ms");
    }

    private static List<String[]> readCSV(String filePath) throws IOException {
        List<String[]> records = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePath))) {
            CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim());
            for (CSVRecord record : parser) {
                String[] recordArray = new String[record.size()];
                for (int i = 0; i < record.size(); i++) {
                    recordArray[i] = record.get(i);
                }
                records.add(recordArray);
            }
        }
        return records;
    }
}
