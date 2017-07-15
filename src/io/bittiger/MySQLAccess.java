package io.bittiger;


import java.sql.*;
import java.util.Arrays;

public class MySQLAccess {
    private Connection dConnection = null;
    private String dUserName;
    private String dPassword;
    private String dServerName;
    private String dDbName;

    public void close() throws SQLException {
        System.out.println("Close database");
        try {
            if(dConnection != null)
                dConnection.close();
        } catch (Exception e) {
            throw e;
        }
    }

    public MySQLAccess(String dUserName, String dPassword, String dServerName, String dDbName) throws SQLException, ClassNotFoundException {
        this.dUserName = dUserName;
        this.dPassword = dPassword;
        this.dServerName = dServerName;
        this.dDbName = dDbName;

        try {
            Class.forName("com.mysql.jdbc.Driver");
            String conn = "jdbc:mysql://" + dServerName + "/" + dDbName + "?user=" + dUserName + "&password=" + dPassword;
            System.out.println("Connecting to database: " + conn);

            dConnection = DriverManager.getConnection(conn);
            System.out.println("Connected to the database");
        } catch (Exception e) {
            throw e;
        }
    }

    private Boolean isRecordExist(String sqlQuery) throws SQLException {
        PreparedStatement existStatement = null;
        boolean isExist = false;

        try {
            existStatement = dConnection.prepareStatement(sqlQuery);
            ResultSet resultSet = existStatement.executeQuery();
            if (resultSet.next()) {
                isExist = true;
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            throw e;
        }
        finally {
            if (existStatement != null) {
                existStatement.close();
            }
        }
        return isExist;
    }

    public Ad getAdData(Long adId) throws Exception {

        PreparedStatement adStatement = null;
        ResultSet resultSet = null;
        Ad ad = new Ad();
        String sqlString = "select * from " + dDbName + ".ad where adId=" + adId;

        try {
            adStatement = dConnection.prepareStatement(sqlString);
            resultSet = adStatement.executeQuery();

            while (resultSet.next()) {
                ad.adId = resultSet.getLong("adId");
                ad.campaignId = resultSet.getLong("campaignId");
                String keywords = resultSet.getString("keyWords");
                String[] keyWordsList = keywords.split(",");
                ad.keyWords = Arrays.asList(keyWordsList);
                ad.bidPrice = resultSet.getDouble("bidPrice");
                ad.price = resultSet.getDouble("price");
                ad.thumbnail = resultSet.getString("thumbnail");
                ad.description = resultSet.getString("description");
                ad.brand = resultSet.getString("brand");
                ad.detail_url = resultSet.getString("detail_url");
                ad.category = resultSet.getString("category");
                ad.title = resultSet.getString("title");
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            throw e;
        } finally {
            if (adStatement != null) {
                adStatement.close();
            }
            if (resultSet != null) {
                resultSet.close();
            }
        }
        return ad;
    }

    public void addAdData (Ad ad) throws SQLException {
        Boolean isExist = false;
        String sqlString = "select adId from " + dDbName+ ".ads where adId=" + ad.adId;
        PreparedStatement adInfo = null;
        try {
            isExist = isRecordExist(sqlString);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            throw e;
        }
        if (isExist) {
            return;
        }

        sqlString = "insert into " + dDbName +".ads values(?,?,?,?,?,?,?,?,?,?,?)";

        try {
            adInfo = dConnection.prepareStatement(sqlString);
            adInfo.setLong(1, ad.adId);
            adInfo.setLong(2, ad.campaignId);
            String keyWords = Utility.strJoin(ad.keyWords, ",");
            adInfo.setString(3, keyWords);
            adInfo.setDouble(4, ad.bidPrice);
            adInfo.setDouble(5, ad.price);
            adInfo.setString(6, ad.thumbnail);
            adInfo.setString(7, ad.description);
            adInfo.setString(8, ad.brand);
            adInfo.setString(9, ad.detail_url);
            adInfo.setString(10, ad.category);
            adInfo.setString(11, ad.title);
            adInfo.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            throw e;
        } finally {
            if (adInfo != null) {
                adInfo.close();
            }
        }
    }

    public void addCampaignData(Campaign campaign) throws Exception{
        boolean isExist = false;
        String sql_string = "select campaignId from " + dDbName + ".campaign where campaignId=" + campaign.campaignId;
        try
        {
            isExist = isRecordExist(sql_string);
        }
        catch(SQLException e )
        {
            System.out.println(e.getMessage());
            throw e;
        }

        if(isExist) {
            return;
        }
        PreparedStatement camp_info = null;
        sql_string = "insert into " + dDbName +".campaign values(?,?)";
        try {
            camp_info = dConnection.prepareStatement(sql_string);
            camp_info.setLong(1, campaign.campaignId);
            camp_info.setDouble(2, campaign.budget);
            camp_info.executeUpdate();
        }
        catch(SQLException e )
        {
            System.out.println(e.getMessage());
            throw e;
        }
        finally
        {
            if (camp_info != null) {
                camp_info.close();
            };
        }
    }

    public Double getBudget(Long campaignId)  throws Exception {
        PreparedStatement selectStatement = null;
        ResultSet result_set = null;
        Double budget = 0.0;
        String sql_string= "select budget from " + dDbName + ".campaign where campaignId=" + campaignId;
        System.out.println("sql: " + sql_string);
        try
        {
            selectStatement = dConnection.prepareStatement(sql_string);
            result_set = selectStatement.executeQuery();
            while (result_set.next()) {
                budget = result_set.getDouble("budget");
            }
        }
        catch(SQLException e )
        {
            System.out.println(e.getMessage());
            throw e;
        }
        finally
        {
            if(selectStatement != null) {
                selectStatement.close();
            }
            if(result_set != null) {
                result_set.close();
            }
        }
        return budget;
    }

    public void updateCampaignData(Long campaignId,Double newBudget) throws Exception {
        PreparedStatement updateStatement = null;
        String sql_string= "update " + dDbName + ".campaign set budget=" + newBudget +" where campaignId=" + campaignId;
        System.out.println("sql: " + sql_string);
        try
        {
            updateStatement = dConnection.prepareStatement(sql_string);
            updateStatement.executeUpdate();
        }
        catch(SQLException e )
        {
            System.out.println(e.getMessage());
            throw e;
        }
        finally
        {
            if(updateStatement != null) {
                updateStatement.close();
            }
        }

    }

}
