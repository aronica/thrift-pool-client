/**
 * 
 */
package com.github.phantomthief.thrift.client.impl;

import com.github.phantomthief.thrift.client.ThriftClient;
import com.github.phantomthief.thrift.client.exception.NoBackendException;
import com.github.phantomthief.thrift.client.pool.ThriftConnectionPoolProvider;
import com.github.phantomthief.thrift.client.pool.ThriftServerInfo;
import com.github.phantomthief.thrift.client.pool.impl.DefaultThriftConnectionPoolImpl;
import com.github.phantomthief.thrift.client.utils.ThriftClientUtils;
import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyFactory;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Function;

public class ThriftClientImpl implements ThriftClient {

    private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(getClass());

    private ThriftConnectionPoolProvider poolProvider;

    private ThriftServerInfoManager serverInfoManager ;

    /**
     * <p>
     * Constructor for ThriftClientImpl.
     * </p>
     *
     */
    public ThriftClientImpl(ThriftServerInfoManager manager) {
        poolProvider = DefaultThriftConnectionPoolImpl.getInstance();
        this.serverInfoManager = manager;
        this.serverInfoManager.start();
    }

    public ThriftClientImpl(ThriftServerInfoManager manager,
                            ThriftConnectionPoolProvider poolProvider) {
        this.poolProvider = poolProvider;
        this.serverInfoManager = manager;
        serverInfoManager.start();
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * iface.
     * </p>
     */
    @Override
    public <X extends TServiceClient> X iface(Class<X> ifaceClass) {
        return iface(ifaceClass, ThriftClientUtils.randomNextInt());
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * iface.
     * </p>
     */
    @Override
    public <X extends TServiceClient> X iface(Class<X> ifaceClass, int hash) {
        return iface(ifaceClass, TCompactProtocol::new, hash);
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * iface.
     * </p>
     */
    @SuppressWarnings("unchecked")
    @Override
    public <X extends TServiceClient> X iface(Class<X> ifaceClass,
            Function<TTransport, TProtocol> protocolProvider, int hash) {
        ThriftServerInfo server = null;
        TTransport transport = null;
        TProtocol protocol = null;
        while(transport == null){
            server = serverInfoManager.get();
            if(server == null)throw new NoBackendException();
            try {
                transport = poolProvider.getConnection(server);
                break;
            } catch (Exception e) {
                logger.error("Create TTransport connecting to {} throws exception, invalid it.",server,e);
                serverInfoManager.invalid(server);
            }
        }
        protocol = protocolProvider.apply(transport);
        ProxyFactory factory = new ProxyFactory();
        factory.setSuperclass(ifaceClass);
        factory.setFilter(m -> ThriftClientUtils.getInterfaceMethodNames(ifaceClass).contains(
                m.getName()));
        try {
            X x = (X) factory.create(new Class[] { TProtocol.class },
                    new Object[] { protocol });
            ThriftServerInfo finalServer = server;
            TTransport finalTransport = transport;
            ((Proxy) x).setHandler((self, thisMethod, proceed, args) -> {
                boolean success = false;
                try {
                    Object result = proceed.invoke(self, args);
                    success = true;
                    return result;
                } finally {
                    if (success) {
                        poolProvider.returnConnection(finalServer, finalTransport);
                    } else {
                        poolProvider.returnBrokenConnection(finalServer, finalTransport);
                    }
                }
            });
            return x;
        } catch (NoSuchMethodException | IllegalArgumentException | InstantiationException
                | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("fail to create proxy.", e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * mpiface.
     * </p>
     */
    @Override
    public <X extends TServiceClient> X mpiface(Class<X> ifaceClass, String serviceName) {
        return mpiface(ifaceClass, serviceName, ThriftClientUtils.randomNextInt());
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * mpiface.
     * </p>
     */
    @Override
    public <X extends TServiceClient> X mpiface(Class<X> ifaceClass, String serviceName, int hash) {
        return mpiface(ifaceClass, serviceName, TCompactProtocol::new, hash);
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * mpiface.
     * </p>
     */
    @Override
    public <X extends TServiceClient> X mpiface(Class<X> ifaceClass, String serviceName,
        Function<TTransport, TProtocol> protocolProvider, int hash) {
        return iface(ifaceClass,
            protocolProvider.andThen((p) -> new TMultiplexedProtocol(p, serviceName)), hash);
    }

}
