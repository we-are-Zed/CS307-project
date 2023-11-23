import com.csvreader.CsvReader;

import javax.xml.crypto.Data;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.Random;

public class DatabaseIO {
    private Connection con = null;

    private void connect() throws ClassNotFoundException, SQLException {
        Class.forName("org.postgresql.Driver");
        String url = "jdbc:postgresql://localhost:5433/project1";
        con = DriverManager.getConnection(url, "postgres", "wangzhuo123");
        con.setAutoCommit(false);
    }

    private void close() throws SQLException {
        if (con != null) {
            con.close();
            con = null;
        }
    }

    private void insert(int num) throws IOException, SQLException {
        System.out.println("[+] Inserting");
        long startTime = System.currentTimeMillis();
        int cnt = 0;
        CsvReader csvReader = new CsvReader("data/users.csv", ',', Charset.forName("UTF-8"));
        csvReader.readHeaders();
        PreparedStatement userstmt = con.prepareStatement("insert into usr.users values (?,?,?,?,?,?,?)");
        PreparedStatement followstmt = con.prepareStatement("insert into usr.followings(follower_id, followed_id) values (?,?)");
        for(int t = 0; t < num; t++) {
            csvReader.readRecord();
            BigInteger mid = new BigInteger(csvReader.get(0));
            String name = csvReader.get("Name");
            String sex = csvReader.get("Sex");
            String birth = csvReader.get("Birthday");
            int level = Integer.parseInt(csvReader.get("Level"));
            String sign = csvReader.get("Sign");
            String identity = csvReader.get("identity");
            userstmt.setObject(1, mid, Types.BIGINT);
            userstmt.setString(2, name);
            userstmt.setString(3, sex);
            userstmt.setString(4, birth);
            userstmt.setInt(5, level);
            userstmt.setString(6, sign);
            userstmt.setString(7, identity);
            userstmt.executeUpdate();
            cnt++;

            String following = csvReader.get("following");
            following = following.substring(1, following.length() - 1);
            String[] followArray = following.split(", ");
            for (int i = 0; i < followArray.length; i++) {
                followstmt.setObject(1, mid, Types.BIGINT);
                if(followArray[i].length() > 0) {
                    BigInteger followed = new BigInteger(followArray[i].substring(1, followArray[i].length() - 1));
                    followstmt.setObject(2, followed, Types.BIGINT);
                    followstmt.executeUpdate();
                    cnt++;
                }
            }
        }
        con.commit();
        long endTime = System.currentTimeMillis();
        System.out.println("[+] Users Runtime：" + (endTime - startTime) + "ms");
        System.out.println("[+] Users Insertion Speed：" + (cnt * 1000L) /(endTime - startTime) + "records/s");
    }

    private void remove(int num) throws SQLException {
        System.out.println("[+] Removing");
        long startTime = System.currentTimeMillis();
        int cnt = 0;

        // 开启事务
        con.setAutoCommit(false);

        // 使用子查询直接删除最后num个用户
        String deleteSql = "DELETE FROM usr.users WHERE mid IN (SELECT mid FROM usr.users ORDER BY mid DESC LIMIT ?)";
        PreparedStatement deleteStmt = con.prepareStatement(deleteSql);
        deleteStmt.setInt(1, num);
        cnt = deleteStmt.executeUpdate();

        // 提交事务
        con.commit();

        long endTime = System.currentTimeMillis();
        System.out.println("[+] Removal Runtime：" + (endTime - startTime) + "ms");
        System.out.println("[+] Users Deletion Speed：" + (cnt * 1000L) / (endTime - startTime) + "records/s");
    }



    private void update() throws SQLException {
        System.out.println("[+] Updating");
        long startTime = System.currentTimeMillis();
        int cnt = 0;

        // 开启事务
        con.setAutoCommit(false);

        // 预先准备随机字符数组
        char[] chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toCharArray();
        Random random = new Random();

        // 准备更新语句
        PreparedStatement updateStmt = con.prepareStatement("UPDATE usr.users SET sign = ? WHERE mid = ?");

        // 获取所有用户的mid
        PreparedStatement getMidsStmt = con.prepareStatement("SELECT mid FROM usr.users");
        ResultSet mids = getMidsStmt.executeQuery();

        // 遍历所有用户ID并更新签名
        while (mids.next()) {
            BigInteger mid = mids.getBigDecimal("mid").toBigInteger();

            // 生成随机长度的字符串
            int length = 5 + random.nextInt(16); // 5~20位字符
            StringBuilder signBuilder = new StringBuilder(length);
            for (int i = 0; i < length; i++) {
                signBuilder.append(chars[random.nextInt(chars.length)]);
            }
            String newSign = signBuilder.toString();

            // 设置更新语句的参数并执行
            updateStmt.setString(1, newSign);
            updateStmt.setObject(2, mid, Types.BIGINT);
            updateStmt.executeUpdate();
            cnt++;
        }

        // 提交事务
        con.commit();

        long endTime = System.currentTimeMillis();
        System.out.println("[+] Update Runtime：" + (endTime - startTime) + "ms");
        System.out.println("[+] Users Update Speed：" + (cnt * 1000L) / (endTime - startTime) + "records/s");
    }


    private void findUser() throws SQLException {
        System.out.println("[+] Finding Users");
        long startTime = System.currentTimeMillis();

        // 准备随机选择用户的查询语句
        String findSql = "SELECT mid, name, sex, birthday, level, sign, identity FROM usr.users ORDER BY RANDOM() LIMIT 10000";
        PreparedStatement findStmt = con.prepareStatement(findSql);

        // 执行查询
        ResultSet rs = findStmt.executeQuery();

        // 遍历结果集并打印用户信息
        while(rs.next()) {
            BigInteger mid = rs.getBigDecimal("mid").toBigInteger();
            String name = rs.getString("name");
            String sex = rs.getString("sex");
            String birthday = rs.getString("birthday");
            int level = rs.getInt("level");
            String sign = rs.getString("sign");
            String identity = rs.getString("identity");

            // 打印用户信息
            System.out.println("User ID: " + mid + ", Name: " + name + ", Sex: " + sex + ", Birthday: " + birthday
                    + ", Level: " + level + ", Sign: " + sign + ", Identity: " + identity);
        }

        long endTime = System.currentTimeMillis();
        System.out.println("[+] Find Users Runtime：" + (endTime - startTime) + "ms");
    }


    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
        DatabaseIO Main = new DatabaseIO();
        Main.connect();
        Main.findUser();
        Main.close();
    }
}
