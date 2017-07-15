package io.bittiger;


import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class IndexBuilder {
    private int EXP = 0;
    private String mMemcachedServer;
    private int mMemcachePortal;
    private String mysqlHost;
    private String mysqlDatabase;
    private String mysqlUser;
    private String mysqlPassword;
    private MySQLAccess mysql;
    private MemcachedClient memcachedClient;

    public void Close() {
        if (mysql != null) {
            try {
                mysql.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public IndexBuilder(String mMemcachedServer, int mMemcachePortal, String mysqlHost, String mysqlDatabase, String mysqlUser, String mysqlPassword) {
        this.mMemcachedServer = mMemcachedServer;
        this.mMemcachePortal = mMemcachePortal;
        this.mysqlHost = mysqlHost;
        this.mysqlDatabase = mysqlDatabase;
        this.mysqlUser = mysqlUser;
        this.mysqlPassword = mysqlPassword;

        try {
            mysql = new MySQLAccess(mysqlHost, mysqlDatabase, mysqlUser, mysqlPassword);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        String address = mMemcachedServer + ":" + mMemcachePortal;

        try {
            memcachedClient = new MemcachedClient(new ConnectionFactoryBuilder().setDaemon(true).setFailureMode(FailureMode.Retry).build(),
                                                    AddrUtil.getAddresses(address));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Boolean buildInvertIndex(Ad ad) {
        String keyWords = Utility.strJoin(ad.keyWords, ",");
        List<String> tokens = Utility.cleanedTokenize(keyWords);

        for(int i = 0; i < tokens.size();i++) {
            String key = tokens.get(i);
            if (memcachedClient.get(key) instanceof Set) {
                Set<Long>  adIdList = (Set<Long>) memcachedClient.get(key);
                adIdList.add(ad.adId);
                memcachedClient.set(key, EXP, adIdList);
            } else {
                Set<Long>  adIdList = new HashSet<Long>();
                adIdList.add(ad.adId);
                memcachedClient.set(key, EXP, adIdList);
            }
        }

        return true;
    }

    public Boolean buildForwardIndex(Ad ad) {
        try {
            mysql.addAdData(ad);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public Boolean updateBudget(Campaign camp)
    {
        try
        {
            mysql.addCampaignData(camp);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
