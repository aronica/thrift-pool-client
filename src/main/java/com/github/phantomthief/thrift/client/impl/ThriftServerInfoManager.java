package com.github.phantomthief.thrift.client.impl;

import com.github.phantomthief.thrift.client.pool.ThriftConnectionPoolProvider;
import com.github.phantomthief.thrift.client.pool.ThriftServerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Created by fafu on 2017/4/18.
 */
public class ThriftServerInfoManager extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(ThriftServerInfoManager.class);
    private CopyOnWriteArrayList<ThriftServerInfo> storage;


    private CopyOnWriteArrayList<ThriftServerInfo> invalidStorage = new CopyOnWriteArrayList<>();

    private AtomicInteger inc = new AtomicInteger(0);

    private Function<ThriftServerInfo,Boolean> validator;

    public ThriftServerInfoManager(ThriftConnectionPoolProvider provider,Function<ThriftServerInfo,Boolean> validator) {
        setDaemon(true);
        storage = new CopyOnWriteArrayList<>(storage);
        this.validator = validator;
    }

    public ThriftServerInfoManager(List<ThriftServerInfo> list,ThriftConnectionPoolProvider provider,Function<ThriftServerInfo,Boolean> validator) {
        storage = new CopyOnWriteArrayList<>(list);
        setDaemon(true);
        this.validator = validator;
    }

    public void add(ThriftServerInfo info) {
        storage.add(info);
    }

    public synchronized void remove(ThriftServerInfo info){
        storage.remove(info);
        invalidStorage.remove(info);
    }

    public synchronized void invalid(ThriftServerInfo info) {
        storage.remove(info);
        invalidStorage.add(info);
    }

    public synchronized void valid(ThriftServerInfo info) {
        invalidStorage.remove(info);
        storage.add(info);
    }

    public ThriftServerInfo get() {
        try {
            if (storage.size() > 0) {
                int index = inc.getAndIncrement();
                if (index < 0) index = 0;
                return storage.get(index % storage.size());
            }
            return null;
        }catch (Exception e){
            logger.error("",e);
            return null;
        }
    }

    @Override
    public void run() {
        while(true){
            List<ThriftServerInfo> becomeValid = new ArrayList<>();
            if(invalidStorage != null){
                for(ThriftServerInfo serverInfo:invalidStorage){
                    try {
                        boolean ret = validator.apply(serverInfo);
                        if(ret){
                            becomeValid.add(serverInfo);
                        }
                    } catch (Exception e) {
                        logger.error("ThriftServerInfo '{}:{}' is still invalid.",
                                serverInfo.getHost(),serverInfo.getPort());
                    }
                }
                becomeValid.forEach(i->{
                    valid(i);
                });
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.error("",e);
                return;
            }
        }
    }

    public List<ThriftServerInfo> getAll() {
        return storage;
    }
}
