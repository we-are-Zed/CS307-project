import org.apache.commons.csv.*;
import java.io.*;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class UserDataGenerator {

    private static final String[] EXISTING_USER_FILES_PATHS = {
            "C:\\Users\\lenovo\\Desktop\\学习资料（非常滴珍贵）\\大二上\\DataBase_project1\\source\\data\\users.csv",
            "C:\\Users\\lenovo\\Desktop\\学习资料（非常滴珍贵）\\大二上\\SUSTech_CS307_2023F_Project1-main\\SUSTech_CS307_2023F_Project1-main\\DBMS\\data\\generated_users.csv",
            "C:\\Users\\lenovo\\Desktop\\学习资料（非常滴珍贵）\\大二上\\SUSTech_CS307_2023F_Project1-main\\SUSTech_CS307_2023F_Project1-main\\DBMS\\data\\generated_users1.csv"
    };
    private static final String NEW_USER_FILE_PATH = "C:\\Users\\lenovo\\Desktop\\generated_users2.csv";
    private static final int NUMBER_OF_USERS = 200000;

    public static void main(String[] args) throws IOException {
        Set<BigInteger> existingMids = new HashSet<>();
        for (String filePath : EXISTING_USER_FILES_PATHS) {
            existingMids.addAll(readExistingMids(filePath));
        }
        generateAndWriteUsers(NEW_USER_FILE_PATH, NUMBER_OF_USERS, existingMids);
    }

    private static Set<BigInteger> readExistingMids(String filePath) throws IOException {
        Set<BigInteger> mids = new HashSet<>();
        try (Reader reader = Files.newBufferedReader(Paths.get(filePath))) {
            CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreSurroundingSpaces().withTrim());
            mids = parser.getRecords().stream()
                    .map(record -> new BigInteger(record.get(0)))
                    .collect(Collectors.toSet());
        }
        return mids;
    }

    private static void generateAndWriteUsers(String filePath, int numberOfUsers, Set<BigInteger> existingMids) throws IOException {
        Random rand = new Random();

        // 创建 FileWriter 实例，并写入表头
        try (Writer writer = new FileWriter(filePath);
             CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("Mid", "Name", "Sex", "Birthday", "Level", "Sign", "following", "identity"))) {
            // 生成用户并写入 CSV
            for (int i = 0; i < numberOfUsers; i++) {
                BigInteger id;
                do {
                    id = new BigInteger(30, rand); // 生成一个随机的正BigInteger，大约对应9位数的整数
                } while (existingMids.contains(id));
                existingMids.add(id); // 将新生成的mid添加到集合中，避免重复

                String[] newUser = generateRandomUser(id, rand);
                csvPrinter.printRecord((Object[]) newUser);
            }
        }
    }

    private static String[] generateRandomUser(BigInteger mid, Random rand) {
        String name = "user" + mid;
        String sex = rand.nextBoolean() ? "男" : "女";
        String birthday = ""; // 生日可以留空或生成一个随机日期
        int level = rand.nextInt(6); // 假设 level 从 0 到 5
        String sign = "This is a random signature";
        String identity = rand.nextBoolean() ? "user" : "superuser";
        String followings = generateRandomFollowings(rand);

        return new String[] {
                mid.toString(),
                name,
                sex,
                birthday,
                String.valueOf(level),
                sign,
                followings,
                identity
        };
    }

    private static String generateRandomFollowings(Random rand) {
        int followingCount = 1 + rand.nextInt(100);
        int[] followingIds = rand.ints(followingCount, 100000, 1000000).toArray();
        StringBuilder followingsBuilder = new StringBuilder("['");
        for (int i = 0; i < followingIds.length; i++) {
            followingsBuilder.append(followingIds[i]);
            if (i < followingIds.length - 1) {
                followingsBuilder.append("', '");
            }
        }
        followingsBuilder.append("']");
        return followingsBuilder.toString();
    }
}
