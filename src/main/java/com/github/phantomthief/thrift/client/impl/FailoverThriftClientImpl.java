/**
 * 
 */
package com.github.phantomthief.thrift.client.impl;

import com.github.phantomthief.thrift.client.ThriftClient;
import com.github.phantomthief.thrift.client.pool.ThriftConnectionPoolProvider;
import com.github.phantomthief.thrift.client.pool.ThriftServerInfo;
import com.github.phantomthief.thrift.client.pool.impl.DefaultThriftConnectionPoolImpl;
import com.github.phantomthief.thrift.client.utils.FailoverCheckingStrategy;
import com.github.phantomthief.thrift.client.utils.ThriftClientUtils;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toList;

/**
 * <p>
 * FailoverThriftClientImpl class.
 * </p>
 *
 * @author w.vela
 * @version $Id: $Id
 */
public class FailoverThriftClientImpl implements ThriftClient {

    private final ThriftClient thriftClient;

    /**
     * <p>
     * Constructor for FailoverThriftClientImpl.
     * </p>
     *
     * @param manager a {@link ThriftServerInfoManager}
     *        object.
     */
    public FailoverThriftClientImpl(ThriftServerInfoManager manager) {
        this(new FailoverCheckingStrategy<>(), manager, DefaultThriftConnectionPoolImpl
                .getInstance());
    }

    /**
     * <p>
     * Constructor for FailoverThriftClientImpl.
     * </p>
     *
     * @param failoverCheckingStrategy a
     *        {@link com.github.phantomthief.thrift.client.utils.FailoverCheckingStrategy}
     *        object.
     * @param manager a
     *        object.
     * @param poolProvider a
     *        {@link com.github.phantomthief.thrift.client.pool.ThriftConnectionPoolProvider}
     *        object.
     */
    public FailoverThriftClientImpl(
            FailoverCheckingStrategy<ThriftServerInfo> failoverCheckingStrategy,
            ThriftServerInfoManager manager,
            ThriftConnectionPoolProvider poolProvider) {
        FailoverStategy failoverStategy = new FailoverStategy( manager, poolProvider,
                failoverCheckingStrategy);
        this.thriftClient = new ThriftClientImpl(manager, failoverStategy);
    }

    /** {@inheritDoc} */
    @Override
    public <X extends TServiceClient> X iface(Class<X> ifaceClass) {
        return thriftClient.iface(ifaceClass);
    }

    /* (non-Javadoc)
     * @see com.github.phantomthief.thrift.client.ThriftClient#iface(java.lang.Class)
     */

    /** {@inheritDoc} */
    @Override
    public <X extends TServiceClient> X iface(Class<X> ifaceClass, int hash) {
        return thriftClient.iface(ifaceClass, hash);
    }

    /* (non-Javadoc)
     * @see com.github.phantomthief.thrift.client.ThriftClient#iface(java.lang.Class, int)
     */

    /** {@inheritDoc} */
    @Override
    public <X extends TServiceClient> X iface(Class<X> ifaceClass,
            Function<TTransport, TProtocol> protocolProvider, int hash) {
        return thriftClient.iface(ifaceClass, protocolProvider, hash);
    }

    /** {@inheritDoc} */
    @Override
    public <X extends TServiceClient> X mpiface(Class<X> ifaceClass, String serviceName) {
        return thriftClient.mpiface(ifaceClass, serviceName, ThriftClientUtils.randomNextInt());
    }

    /** {@inheritDoc} */
    @Override
    public <X extends TServiceClient> X mpiface(Class<X> ifaceClass, String serviceName, int hash) {
        return thriftClient.mpiface(ifaceClass, serviceName, TBinaryProtocol::new, hash);
    }

    /** {@inheritDoc} */
    @Override
    public <X extends TServiceClient> X mpiface(Class<X> ifaceClass, String serviceName,
        Function<TTransport, TProtocol> protocolProvider, int hash) {
        return thriftClient.mpiface(ifaceClass, serviceName, protocolProvider, hash);
    }

    /* (non-Javadoc)
     * @see com.github.phantomthief.thrift.client.ThriftClient#iface(java.lang.Class, java.util.function.Function, int)
     */

    private class FailoverStategy implements
                                 Supplier<List<ThriftServerInfo>>,
                                 ThriftConnectionPoolProvider {

        private final ThriftServerInfoManager manager;

        private final ThriftConnectionPoolProvider connectionPoolProvider;

        private final FailoverCheckingStrategy<ThriftServerInfo> failoverCheckingStrategy;

        private FailoverStategy(ThriftServerInfoManager manager,
                ThriftConnectionPoolProvider connectionPoolProvider,
                FailoverCheckingStrategy<ThriftServerInfo> failoverCheckingStrategy) {
            this.manager = manager;
            this.connectionPoolProvider = connectionPoolProvider;
            this.failoverCheckingStrategy = failoverCheckingStrategy;
        }

        /* (non-Javadoc)
         * @see java.util.function.Supplier#get()
         */
        @Override
        public List<ThriftServerInfo> get() {
            Set<ThriftServerInfo> failedServers = failoverCheckingStrategy.getFailed();
            return manager.getAll().stream()
                    .filter(i -> !failedServers.contains(i)).collect(toList());
        }

        /* (non-Javadoc)
         * @see com.github.phantomthief.thrift.client.pool.ThriftConnectionPoolProvider#getConnection(com.github.phantomthief.thrift.client.pool.ThriftServerInfo)
         */
        @Override
        public TTransport getConnection(ThriftServerInfo thriftServerInfo)throws Exception {
            return connectionPoolProvider.getConnection(thriftServerInfo);
        }

        /* (non-Javadoc)
         * @see com.github.phantomthief.thrift.client.pool.ThriftConnectionPoolProvider#returnConnection(com.github.phantomthief.thrift.client.pool.ThriftServerInfo, org.apache.thrift.transport.TTransport)
         */
        @Override
        public void returnConnection(ThriftServerInfo thriftServerInfo, TTransport transport) {
            connectionPoolProvider.returnConnection(thriftServerInfo, transport);
        }

        /* (non-Javadoc)
         * @see com.github.phantomthief.thrift.client.pool.ThriftConnectionPoolProvider#returnBrokenConnection(com.github.phantomthief.thrift.client.pool.ThriftServerInfo, org.apache.thrift.transport.TTransport)
         */
        @Override
        public void returnBrokenConnection(ThriftServerInfo thriftServerInfo, TTransport transport) {
            failoverCheckingStrategy.fail(thriftServerInfo);
            connectionPoolProvider.returnBrokenConnection(thriftServerInfo, transport);
        }
    }

}
