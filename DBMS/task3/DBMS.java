import java.io.*;
import java.sql.*;
import java.math.BigInteger;
import com.csvreader.CsvReader;
import java.nio.charset.Charset;

public class DBMS {
    private Connection con = null;

    private void connect() throws ClassNotFoundException, SQLException {
        Class.forName("org.postgresql.Driver");
        String url = "jdbc:postgresql://localhost:5432/project1";
        con = DriverManager.getConnection(url, "chanben", "");
    }

    private void close() throws SQLException {
        if (con != null) {
            con.close();
            con = null;
        }
    }

    private void insertUser() throws IOException, SQLException {
        CsvReader csvReader = new CsvReader("data/users.csv", ',', Charset.forName("UTF-8"));
        csvReader.readHeaders();
        PreparedStatement userstmt = con.prepareStatement("insert into usr.users values (?,?,?,?,?,?,?)");
        PreparedStatement followstmt = con.prepareStatement("insert into usr.followings(follower_id, followed_id) values (?,?)");
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
            userstmt.executeUpdate();

            String following = csvReader.get("following");
            following = following.substring(1, following.length() - 1);
            String[] followArray = following.split(", ");
            for (int i = 0; i < followArray.length; i++) {
                followstmt.setObject(1, mid, Types.BIGINT);
                if(followArray[i].length() > 0) {
                    BigInteger followed = new BigInteger(followArray[i].substring(1, followArray[i].length() - 1));
                    followstmt.setObject(2, followed, Types.BIGINT);
                    followstmt.executeUpdate();
                }
            }
        }
    }

    private void insertVideo() throws IOException, SQLException {
        CsvReader csvReader = new CsvReader("data/videos.csv", ',', Charset.forName("UTF-8"));
        csvReader.setSafetySwitch(false);
        csvReader.readHeaders();

        PreparedStatement videostmt = con.prepareStatement("insert into video.videos values (?,?,?,?,?,?,?::interval,?,?)");
        PreparedStatement likestmt = con.prepareStatement("insert into video.likes(who_likes,BV) values (?,?)");
        PreparedStatement coinstmt = con.prepareStatement("insert into video.coin(who_coins,BV) values (?,?)");
        PreparedStatement favstmt = con.prepareStatement("insert into video.favorite(who_favorites,BV) values (?,?)");
        PreparedStatement viewstmt = con.prepareStatement("insert into video.view(who_views,BV,last_time) values (?,?,?::interval)");
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
            videostmt.setString(1,BV);
            videostmt.setString(2,title);
            videostmt.setObject(3, owner, Types.BIGINT);
            videostmt.setObject(4, committime, Types.TIMESTAMP);
            videostmt.setObject(5, reviewtime, Types.TIMESTAMP);
            videostmt.setObject(6, publictime, Types.TIMESTAMP);
            videostmt.setObject(7, duration, Types.OTHER);
            videostmt.setString(8, description);
            videostmt.setObject(9, reviewer, Types.BIGINT);
            videostmt.executeUpdate();

            String like = csvReader.get("Like");
            like = like.substring(1, like.length() - 1);
            String[] likeArray = like.split(", ");
            for (int i = 0; i < likeArray.length; i++) {
                likestmt.setString(2, BV);
                if(likeArray[i].length() > 0) {
                    BigInteger likeid = new BigInteger(likeArray[i].substring(1, likeArray[i].length() - 1));
                    likestmt.setObject(1, likeid, Types.BIGINT);
                    likestmt.executeUpdate();
                }
            }

            String coin = csvReader.get("Coin");
            coin = coin.substring(1, coin.length() - 1);
            String[] coinArray = coin.split(", ");
            for (int i = 0; i < coinArray.length; i++) {
                coinstmt.setString(2, BV);
                if(coinArray[i].length() > 0) {
                    BigInteger coinid = new BigInteger(coinArray[i].substring(1, coinArray[i].length() - 1));
                    coinstmt.setObject(1, coinid, Types.BIGINT);
                    coinstmt.executeUpdate();
                }
            }

            String favo = csvReader.get("Favorite");
            favo = favo.substring(1, favo.length() - 1);
            String[] favoArray = favo.split(", ");
            for (int i = 0; i < favoArray.length; i++) {
                favstmt.setString(2, BV);
                if(favoArray[i].length() > 0) {
                    BigInteger favid = new BigInteger(favoArray[i].substring(1, favoArray[i].length() - 1));
                    favstmt.setObject(1, favid, Types.BIGINT);
                    favstmt.executeUpdate();
                }
            }

            String view = csvReader.get("View");
            view = view.substring(2, view.length() - 2);
            String[] viewArray = view.split("\\), \\(");
            viewstmt.setString(2, BV);
            for (int i = 0; i < viewArray.length; i++) {
                String viewer = viewArray[i].split(", ")[0];
                BigInteger viewid = new BigInteger(viewer.substring(1,viewer.length() - 1));
                int viewtime = Integer.parseInt(viewArray[i].split(", ")[1]);
                String last_time = viewtime / 3600 + " hours " + (viewtime % 3600) / 60 + " minutes " + (viewtime % 60) + " seconds";
                viewstmt.setObject(1, viewid, Types.BIGINT);
                viewstmt.setObject(3, last_time, Types.OTHER);
                viewstmt.executeUpdate();
            }
        }
    }

    private void insertDanmu() throws IOException, SQLException {
        CsvReader csvReader = new CsvReader("data/danmu.csv", ',', Charset.forName("UTF-8"));
        csvReader.setSafetySwitch(false);
        csvReader.readHeaders();

        PreparedStatement danstmt = con.prepareStatement("insert into danmu.danmu(BV,mid,time,content) values (?,?,?::interval,?)");
        while(csvReader.readRecord()) {
            String bv = csvReader.get(0);
            BigInteger mid = new BigInteger(csvReader.get("Mid"));
            String time = csvReader.get("Time") + " seconds";
            String content = csvReader.get("Content");
            danstmt.setString(1, bv);
            danstmt.setObject(2, mid, Types.BIGINT);
            danstmt.setObject(3, time, Types.OTHER);
            danstmt.setString(4, content);
            danstmt.executeUpdate();
        }
    }

    public static void main(String[] args) {
        DBMS Main = new DBMS();
        try {
            Main.connect();
            System.out.println("[+] Inserting Users");
            long ustartTime = System.currentTimeMillis();
            Main.insertUser();
            long uendTime = System.currentTimeMillis();
            System.out.println("[+] Runtime：" + (uendTime - ustartTime) + "ms");
//
//            System.out.println("[+] Inserting Video");
//            long vstartTime = System.currentTimeMillis();
//            Main.insertVideo();
//            long vendTime = System.currentTimeMillis();
//            System.out.println("[+] Runtime：" + (vendTime - vstartTime) + "ms");
//
//            System.out.println("[+] Inserting Danmu");
//            long dstartTime = System.currentTimeMillis();
//            Main.insertDanmu();
//            long dendTime = System.currentTimeMillis();
//            System.out.println("[+] Runtime：" + (dendTime - dstartTime) + "ms");
            Main.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}