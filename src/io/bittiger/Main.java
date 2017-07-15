package io.bittiger;

import java.sql.SQLException;

public class Main {

    public static void main(String[] args) throws SQLException, ClassNotFoundException {

        String mMemcachedServer = "127.0.0.1";
        int mMemcachePortal = 11211;
        String mysqlHost = "127.0.0.1:3306";
        String mysqlDatabase = "searchads";
        String mysqlUser = "root";
        String mysqlPassword = "yufeiMYSQL";
        String adsDataFilePath = "/Users/wuyufei/IdeaProjects/IndexBuilder/resources/ads_0502.txt";
        String budgetDataFilePath = "/Users/wuyufei/IdeaProjects/IndexBuilder/resources/budget.txt";

        AdsEngine adsEngine = new AdsEngine(
                adsDataFilePath,
                budgetDataFilePath,
                mMemcachedServer,
                mMemcachePortal,
                mysqlUser,
                mysqlPassword,
                mysqlHost,
                mysqlDatabase
                );
        adsEngine.init();

        //MySQLAccess mysql = new MySQLAccess(mysqlUser, mysqlPassword, mMemcachedServer, mysqlDatabase);
    }
}
