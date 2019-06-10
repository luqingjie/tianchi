package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.AtomicPositiveInteger;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author daofeng.xjf
 * <p>
 * 负载均衡扩展接口
 * 必选接口，核心接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 选手需要基于此类实现自己的负载均衡算法
 */
public class UserLoadBalance implements LoadBalance {

    //    @Override
//    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
//        return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
//    }
    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        if (invokers == null || invokers.size() == 0)
            return null;
        if (invokers.size() == 1)
            return invokers.get(0);
        return doSelect(invokers, url, invocation);
    }

    public static final String NAME = "roundrobin";

    private final ConcurrentMap<String, AtomicPositiveInteger> sequences = new ConcurrentHashMap<String, AtomicPositiveInteger>();

    private final ConcurrentMap<String, AtomicPositiveInteger> weightSequences = new ConcurrentHashMap<String, AtomicPositiveInteger>();

    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        String key = invokers.get(0).getUrl().getServiceKey() + "." + invocation.getMethodName();
        int length = invokers.size(); // 总个数
        int maxWeight = 0; // 最大权重
        int minWeight = Integer.MAX_VALUE; // 最小权重
        for (int i = 0; i < length; i++) {
            int weight = getWeight(invokers.get(i), invocation);
            maxWeight = Math.max(maxWeight, weight); // 累计最大权重
            minWeight = Math.min(minWeight, weight); // 累计最小权重
        }
        if (maxWeight > 0 && minWeight < maxWeight) { // 权重不一样
            AtomicPositiveInteger weightSequence = weightSequences.get(key);
            if (weightSequence == null) {
                weightSequences.putIfAbsent(key, new AtomicPositiveInteger());
                weightSequence = weightSequences.get(key);
            }
            int currentWeight = weightSequence.getAndIncrement() % maxWeight;
            List<Invoker<T>> weightInvokers = new ArrayList<Invoker<T>>();
            for (Invoker<T> invoker : invokers) { // 筛选权重大于当前权重基数的Invoker
                if (getWeight(invoker, invocation) > currentWeight) {
                    weightInvokers.add(invoker);
                }
            }
            int weightLength = weightInvokers.size();
            if (weightLength == 1) {
                return weightInvokers.get(0);
            } else if (weightLength > 1) {
                invokers = weightInvokers;
                length = invokers.size();
            }
        }
        AtomicPositiveInteger sequence = sequences.get(key);
        if (sequence == null) {
            sequences.putIfAbsent(key, new AtomicPositiveInteger());
            sequence = sequences.get(key);
        }
        // 取模轮循
        return invokers.get(sequence.getAndIncrement() % length);
    }

    protected int getWeight(Invoker<?> invoker, Invocation invocation) {
        int weight = invoker.getUrl().getMethodParameter(invocation.getMethodName(), "weight", 100);
        if (weight > 0) {
            long timestamp = invoker.getUrl().getParameter("timestamp", 0L);
            if (timestamp > 0L) {
                int uptime = (int) (System.currentTimeMillis() - timestamp);
                int warmup = invoker.getUrl().getParameter("warmup", 10 * 60 * 1000);
                if (uptime > 0 && uptime < warmup) {
                    weight = calculateWarmupWeight(uptime, warmup, weight);
                }
            }
        }
        return weight;
    }

    static int calculateWarmupWeight(int uptime, int warmup, int weight) {
        int ww = (int) ((float) uptime / ((float) warmup / (float) weight));
        return ww < 1 ? 1 : (ww > weight ? weight : ww);
    }
}
