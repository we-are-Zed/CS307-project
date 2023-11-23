import org.apache.commons.csv.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class File_IO_change {

    private static final String USER_FILE_PATH = "C:\\Users\\lenovo\\Desktop\\学习资料（非常滴珍贵）\\大二上\\DataBase_project1\\source\\data\\users.csv";

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        try {
            List<String[]> updatedUsers = new ArrayList<>();

            // 读取CSV文件
            try (BufferedReader reader = Files.newBufferedReader(Paths.get(USER_FILE_PATH))) {
                CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim());
                List<CSVRecord> records = parser.getRecords();

                // 添加表头
                updatedUsers.add(parser.getHeaderNames().toArray(new String[0]));

                // 更新个性签名
                for (CSVRecord record : records) {
                    String[] currentRecord = new String[record.size()];
                    for (int i = 0; i < record.size(); i++) {
                        if (i == 5) { // 第六列是个性签名
                            currentRecord[i] = generateRandomString(5, 20);
                        } else {
                            currentRecord[i] = record.get(i);
                        }
                    }
                    updatedUsers.add(currentRecord);
                }
            }

            // 写回CSV文件
            try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(USER_FILE_PATH));
                 CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(updatedUsers.get(0)))) {
                for (int i = 1; i < updatedUsers.size(); i++) { // 跳过表头
                    csvPrinter.printRecord((Object[]) updatedUsers.get(i));
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        System.out.println("Time taken: " + (endTime - startTime) + "ms");
    }

    private static String generateRandomString(int minLength, int maxLength) {
        Random random = new Random();
        int length = random.nextInt(maxLength - minLength + 1) + minLength;
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            char randomChar = (char) ('a' + random.nextInt(26));
            sb.append(randomChar);
        }
        return sb.toString();
    }
}
