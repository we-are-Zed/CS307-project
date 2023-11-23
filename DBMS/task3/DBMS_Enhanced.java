import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.math.BigInteger;
import com.csvreader.CsvReader;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class DBMS_Enhanced {
    final private int BATCH = 1000;

    private static Connection connect() throws ClassNotFoundException, SQLException {
        Class.forName("org.postgresql.Driver");
        String url = "jdbc:postgresql://localhost:5433/project1";
        Connection con = DriverManager.getConnection(url, "postgres", "wangzhuo123");
        con.setAutoCommit(false);
        return con;
    }

    private static void close(Connection con) throws SQLException {
        if (con != null) {
            con.close();
        }
    }

    private void insertUser() throws IOException, SQLException, InterruptedException, ClassNotFoundException {
        // init
        System.out.println("[+] Inserting Users");
        long startTime = System.currentTimeMillis();
        AtomicInteger cnt = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(2);
        Connection con1 = connect();
        Connection con2 = connect();
        PreparedStatement userstmt = con1.prepareStatement("insert into usr.users values (?,?,?,?,?,?,?)");
        PreparedStatement followstmt = con2.prepareStatement("insert into usr.followings(follower_id, followed_id) values (?,?)");

        new Thread(() -> {
            try {
                int count = 0;
                CsvReader csvReader = new CsvReader("data/users.csv", ',', StandardCharsets.UTF_8);
                csvReader.readHeaders();
                while (csvReader.readRecord()) {
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
                    userstmt.addBatch();
                    count++;
                    cnt.getAndIncrement();
                    if (count == BATCH) {
                        userstmt.executeBatch();
                        userstmt.clearBatch();
                        count = 0;
                    }
                }
                if(count> 0) userstmt.executeBatch();
                con1.commit();
                latch.countDown();
            } catch (SQLException | IOException e) {
                throw new RuntimeException(e);
            }
        }).start();

        new Thread(() -> {
            try {
                int count = 0;
                CsvReader csvReader = new CsvReader("dat" +
                        "a/users.csv", ',', StandardCharsets.UTF_8);
                csvReader.readHeaders();
                while (csvReader.readRecord()) {
                    BigInteger mid = new BigInteger(csvReader.get(0));
                    String following = csvReader.get("following");
                    following = following.substring(1, following.length() - 1);
                    String[] followArray = following.split(", ");

                    for (int i = 0; i < followArray.length; i++) {
                        followstmt.setObject(1, mid, Types.BIGINT);
                        if (followArray[i].length() > 0) {
                            BigInteger followed = new BigInteger(followArray[i].substring(1, followArray[i].length() - 1));
                            followstmt.setObject(2, followed, Types.BIGINT);
                            followstmt.addBatch();
                            count++;
                            cnt.getAndIncrement();
                            if (count == BATCH) {
                                followstmt.executeBatch();
                                followstmt.clearBatch();
                                count = 0;
                            }
                        }
                    }
                }
                if (count > 0) followstmt.executeBatch();
                con2.commit();
                latch.countDown();
            } catch (SQLException | IOException e) {
                throw new RuntimeException(e);
            }
        }).start();

        latch.await();
        userstmt.close();
        followstmt.close();
        close(con1);
        close(con2);

        long endTime = System.currentTimeMillis();
        System.out.println("[+] Users Runtime：" + (endTime - startTime) + "ms");
        System.out.println("[+] Users Insertion Speed：" + (cnt.get() * 1000L) /(endTime - startTime) + "records/s");
    }

    private void insertVideo() throws IOException, SQLException, InterruptedException, ClassNotFoundException {
        // init
        System.out.println("[+] Inserting Videos");
        long startTime = System.currentTimeMillis();
        AtomicInteger cnt = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(5);
        Connection con1 = connect();
        Connection con2 = connect();
        Connection con3 = connect();
        Connection con4 = connect();
        Connection con5 = connect();
        PreparedStatement videostmt = con1.prepareStatement("insert into video.videos values (?,?,?,?,?,?,?::interval,?,?)");
        PreparedStatement likestmt = con2.prepareStatement("insert into video.likes(who_likes,BV) values (?,?)");
        PreparedStatement coinstmt = con3.prepareStatement("insert into video.coin(who_coins,BV) values (?,?)");
        PreparedStatement favstmt = con4.prepareStatement("insert into video.favorite(who_favorites,BV) values (?,?)");
        PreparedStatement viewstmt = con5.prepareStatement("insert into video.view(who_views,BV,last_time) values (?,?,?::interval)");

        // inserting
        new Thread(() -> {
            try {
                int count = 0;
                CsvReader csvReader = new CsvReader("data/videos.csv", ',', StandardCharsets.UTF_8);
                csvReader.setSafetySwitch(false);
                csvReader.readHeaders();
                while (csvReader.readRecord()) {
                    String BV = csvReader.get(0);
                    String title = csvReader.get("Title");
                    BigInteger owner = new BigInteger(csvReader.get("Owner Mid"));
                    String committime = csvReader.get("Commit Time");
                    String reviewtime = csvReader.get("Review Time");
                    String publictime = csvReader.get("Public Time");
                    int time = Integer.parseInt(csvReader.get("Duration"));
                    String duration = time / 3600 + " hours " + (time % 3600) / 60 + " minutes " + (time % 60) + " seconds";
                    String description = csvReader.get("Description");
                    BigInteger reviewer = new BigInteger(csvReader.get("Reviewer"));
                    videostmt.setString(1, BV);
                    videostmt.setString(2, title);
                    videostmt.setObject(3, owner, Types.BIGINT);
                    videostmt.setObject(4, committime, Types.TIMESTAMP);
                    videostmt.setObject(5, reviewtime, Types.TIMESTAMP);
                    videostmt.setObject(6, publictime, Types.TIMESTAMP);
                    videostmt.setObject(7, duration, Types.OTHER);
                    videostmt.setString(8, description);
                    videostmt.setObject(9, reviewer, Types.BIGINT);
                    videostmt.addBatch();
                    count++;
                    cnt.getAndIncrement();
                    if (count == BATCH) {
                        videostmt.executeBatch();
                        videostmt.clearBatch();
                        count = 0;
                    }
                }
                if (count > 0) videostmt.executeBatch();
                con1.commit();
                latch.countDown();
            } catch (SQLException | IOException e) {
                throw new RuntimeException(e);
            }
        }).start();

        new Thread(() -> {
            try {
                CsvReader csvReader = new CsvReader("data/videos.csv", ',', StandardCharsets.UTF_8);
                csvReader.setSafetySwitch(false);
                csvReader.readHeaders();
                int count = 0;
                while (csvReader.readRecord()) {
                    String BV = csvReader.get(0);
                    String like = csvReader.get("Like");
                    like = like.substring(1, like.length() - 1);
                    String[] likeArray = like.split(", ");
                    likestmt.setString(2, BV);
                    for (int i = 0; i < likeArray.length; i++) {
                        if (likeArray[i].length() > 0) {
                            BigInteger likeid = new BigInteger(likeArray[i].substring(1, likeArray[i].length() - 1));
                            likestmt.setObject(1, likeid, Types.BIGINT);
                            likestmt.addBatch();
                            count++;
                            cnt.getAndIncrement();
                            if (count == BATCH) {
                                likestmt.executeBatch();
                                likestmt.clearBatch();
                                count = 0;
                            }
                        }
                    }
                }
                if (count > 0) likestmt.executeBatch();
                con2.commit();
                latch.countDown();
            } catch (SQLException | IOException e) {
                throw new RuntimeException(e);
            }
        }).start();

        new Thread(() -> {
            try {
                CsvReader csvReader = new CsvReader("data/videos.csv", ',', StandardCharsets.UTF_8);
                csvReader.setSafetySwitch(false);
                csvReader.readHeaders();
                int count = 0;
                while (csvReader.readRecord()) {
                    String BV = csvReader.get(0);
                    String coin = csvReader.get("Coin");
                    coin = coin.substring(1, coin.length() - 1);
                    String[] coinArray = coin.split(", ");
                    coinstmt.setString(2, BV);
                    for (int i = 0; i < coinArray.length; i++) {
                        if (coinArray[i].length() > 0) {
                            BigInteger coinid = new BigInteger(coinArray[i].substring(1, coinArray[i].length() - 1));
                            coinstmt.setObject(1, coinid, Types.BIGINT);
                            coinstmt.addBatch();
                            count++;
                            cnt.getAndIncrement();
                            if (count == BATCH) {
                                coinstmt.executeBatch();
                                coinstmt.clearBatch();
                                count = 0;
                            }
                        }
                    }
                }
                if (count > 0) coinstmt.executeBatch();
                con3.commit();
                latch.countDown();
            } catch (SQLException | IOException e) {
                throw new RuntimeException(e);
            }
        }).start();

        new Thread(() -> {
            try {
                CsvReader csvReader = new CsvReader("data/videos.csv", ',', StandardCharsets.UTF_8);
                csvReader.setSafetySwitch(false);
                csvReader.readHeaders();
                int count = 0;
                while (csvReader.readRecord()) {
                    String BV = csvReader.get(0);
                    String favo = csvReader.get("Favorite");
                    favo = favo.substring(1, favo.length() - 1);
                    String[] favoArray = favo.split(", ");
                    for (int i = 0; i < favoArray.length; i++) {
                        favstmt.setString(2, BV);
                        if (favoArray[i].length() > 0) {
                            BigInteger favid = new BigInteger(favoArray[i].substring(1, favoArray[i].length() - 1));
                            favstmt.setObject(1, favid, Types.BIGINT);
                            favstmt.addBatch();
                            count++;
                            cnt.getAndIncrement();
                            if (count == BATCH) {
                                favstmt.executeBatch();
                                favstmt.clearBatch();
                                count = 0;
                            }
                        }
                    }
                }
                if (count > 0) favstmt.executeBatch();
                con4.commit();
                latch.countDown();
            } catch (SQLException | IOException e) {
                throw new RuntimeException(e);
            }
        }).start();

        new Thread(() -> {
            try {
                CsvReader csvReader = new CsvReader("data/videos.csv", ',', StandardCharsets.UTF_8);
                csvReader.setSafetySwitch(false);
                csvReader.readHeaders();
                int count = 0;
                while (csvReader.readRecord()) {
                    String BV = csvReader.get(0);
                    String view = csvReader.get("View");
                    view = view.substring(2, view.length() - 2);
                    String[] viewArray = view.split("\\), \\(");
                    viewstmt.setString(2, BV);
                    for (int i = 0; i < viewArray.length; i++) {
                        String viewer = viewArray[i].split(", ")[0];
                        BigInteger viewid = new BigInteger(viewer.substring(1, viewer.length() - 1));
                        int viewtime = Integer.parseInt(viewArray[i].split(", ")[1]);
                        String last_time = viewtime / 3600 + " hours " + (viewtime % 3600) / 60 + " minutes " + (viewtime % 60) + " seconds";
                        viewstmt.setObject(1, viewid, Types.BIGINT);
                        viewstmt.setObject(3, last_time, Types.OTHER);
                        viewstmt.addBatch();
                        count++;
                        cnt.getAndIncrement();
                        if (count == BATCH) {
                            viewstmt.executeBatch();
                            viewstmt.clearBatch();
                            count = 0;
                        }
                    }
                }
                if (count > 0) viewstmt.executeBatch();
                con5.commit();
                latch.countDown();
            } catch (SQLException | IOException e) {
                throw new RuntimeException(e);
            }
        }).start();

        latch.await();
        videostmt.close();
        likestmt.close();
        coinstmt.close();
        favstmt.close();
        viewstmt.close();
        close(con1);
        close(con2);
        close(con3);
        close(con4);
        close(con5);

        long endTime = System.currentTimeMillis();
        System.out.println("[+] Videos Runtime：" + (endTime - startTime) + "ms");
        System.out.println("[+] Videos Insertion Speed：" + (cnt.get() * 1000L) /(endTime - startTime) + "records/s");
    }

    private void insertDanmu() throws IOException, SQLException, ClassNotFoundException {
        System.out.println("[+] Inserting Danmu");
        CsvReader csvReader = new CsvReader("data/danmu.csv", ',', StandardCharsets.UTF_8);
        csvReader.setSafetySwitch(false);
        csvReader.readHeaders();
        Connection con = connect();
        PreparedStatement danstmt = con.prepareStatement("insert into danmu.danmu(BV,mid,time,content) values (?,?,?::interval,?)");
        int count = 0, cnt = 0;
        long startTime = System.currentTimeMillis();

        while(csvReader.readRecord()) {
            String bv = csvReader.get(0);
            BigInteger mid = new BigInteger(csvReader.get("Mid"));
            String time = csvReader.get("Time") + " seconds";
            String content = csvReader.get("Content");
            danstmt.setString(1, bv);
            danstmt.setObject(2, mid, Types.BIGINT);
            danstmt.setObject(3, time, Types.OTHER);
            danstmt.setString(4, content);
            danstmt.addBatch();
            count++;
            cnt++;
            if(count == BATCH){
                danstmt.executeBatch();
                danstmt.clearBatch();
                count = 0;
            }
        }
        if(count > 0) danstmt.executeBatch();
        con.commit();
        danstmt.close();

        long endTime = System.currentTimeMillis();
        System.out.println("[+] Danmu Runtime：" + (endTime - startTime) + "ms");
        System.out.println("[+] Danmu Speed：" + (cnt * 1000L) / (endTime - startTime) + "records/s");
    }

    public static void main(String[] args) {
        try {
            System.out.println("[+] Start Inserting");
            long startTime = System.currentTimeMillis();
            DBMS_Enhanced Main = new DBMS_Enhanced();
            final CountDownLatch latch = new CountDownLatch(3);
            new Thread(() -> {
                try {
                    Main.insertUser();
                    latch.countDown();
                } catch (IOException | InterruptedException | SQLException | ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }).start();
            new Thread(() -> {
                try {
                    Main.insertVideo();
                    latch.countDown();
                } catch (IOException | SQLException | InterruptedException | ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }).start();
            new Thread(() -> {
                try {
                    Main.insertDanmu();
                    latch.countDown();
                } catch (IOException | SQLException | ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }).start();
            latch.await();
            long endTime = System.currentTimeMillis();
            System.out.println("[+] Inserting Runtime：" + (endTime - startTime) + "ms");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}