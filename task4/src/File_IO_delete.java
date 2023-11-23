import org.apache.commons.csv.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;

public class File_IO_delete {

    private static final String USER_FILE_PATH = "C:\\Users\\lenovo\\Desktop\\学习资料（非常滴珍贵）\\大二上\\DataBase_project1\\source\\data\\users.csv";
    private static final String VIDEO_FILE_PATH = "C:\\Users\\lenovo\\Desktop\\学习资料（非常滴珍贵）\\大二上\\DataBase_project1\\source\\data\\videos.csv";
    private static final String DANMU_FILE_PATH = "C:\\Users\\lenovo\\Desktop\\学习资料（非常滴珍贵）\\大二上\\DataBase_project1\\source\\data\\danmu.csv";

    public static void main(String[] args) {
        try {
            long startTime = System.currentTimeMillis(); // 记录开始时间

            // 随机选择100个用户并删除
            Set<String> midsToDelete = selectRandomUsers(USER_FILE_PATH, 10000);
            deleteUsers(USER_FILE_PATH, midsToDelete);
            deleteRecordsWithMid(VIDEO_FILE_PATH, 2, midsToDelete); // Owner Mid 在第三列，索引为2
            deleteRecordsWithMid(DANMU_FILE_PATH, 1, midsToDelete); // Mid 在第二列，索引为1

            long endTime = System.currentTimeMillis(); // 记录结束时间
            long duration = endTime - startTime;
            System.out.println("删除操作完成，共花费时间：" + duration + "毫秒");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Set<String> selectRandomUsers(String filePath, int numUsers) throws IOException {
        List<String> mids = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePath))) {
            CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());
            for (CSVRecord record : parser) {
                mids.add(record.get(0)); // Assuming MID is the first column
            }
        }
        Collections.shuffle(mids);
        return new HashSet<>(mids.subList(0, numUsers));
    }

    private static void deleteUsers(String filePath, Set<String> midsToDelete) throws IOException {
        File inputFile = new File(filePath);
        File tempFile = new File(filePath + ".tmp");

        // 检查文件是否存在以及是否可写
        if (!inputFile.exists()) {
            throw new FileNotFoundException("输入文件不存在: " + inputFile.getAbsolutePath());
        }
        if (!inputFile.canWrite()) {
            throw new IOException("没有写权限: " + inputFile.getAbsolutePath());
        }

        try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePath));
             BufferedWriter writer = Files.newBufferedWriter(tempFile.toPath())) {
            CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());
            CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(parser.getHeaderNames().toArray(new String[0])));

            for (CSVRecord record : parser) {
                if (!midsToDelete.contains(record.get(0))) {
                    printer.printRecord(record);
                }
            }
        }
        if (tempFile.exists() && tempFile.canRead()) {
            Files.move(tempFile.toPath(), inputFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } else {
            throw new IOException("临时文件创建失败或不可读: " + tempFile.getAbsolutePath());
        }
    }

    private static void deleteRecordsWithMid(String filePath, int midColumnIndex, Set<String> midsToDelete) throws IOException {
        File tempFile = new File(filePath + ".tmp");
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePath));
             BufferedWriter writer = Files.newBufferedWriter(tempFile.toPath())) {
            CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());
            CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(parser.getHeaderNames().toArray(new String[0])));

            for (CSVRecord record : parser) {
                if (!midsToDelete.contains(record.get(midColumnIndex))) {
                    printer.printRecord(record);
                }
            }
        }
        Files.move(tempFile.toPath(), Paths.get(filePath), StandardCopyOption.REPLACE_EXISTING);
    }
}
