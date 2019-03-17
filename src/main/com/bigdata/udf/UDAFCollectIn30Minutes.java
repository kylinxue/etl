package com.bigdata.etl.udf;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UDAFCollectIn30Minutes extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        if (info.length != 2) {
            throw new UDFArgumentTypeException(info.length - 1, "Exactly two arguments is expected.");
        }
        if (info[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted.");
        }
        if (info[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted.");
        }
        return new CollectActiveNameUDAFEvaluator();
    }

    public static class CollectActiveNameUDAFEvaluator extends GenericUDAFEvaluator {

        protected PrimitiveObjectInspector inputKeyOI;                         // 输入参数0
        protected PrimitiveObjectInspector inputValueOI;                       // 输入参数1
        protected StandardMapObjectInspector internalMergeOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            if (m == Mode.PARTIAL1) {
                inputKeyOI = (PrimitiveObjectInspector) parameters[0];
                inputValueOI = (PrimitiveObjectInspector) parameters[1];
                return ObjectInspectorFactory.getStandardMapObjectInspector(ObjectInspectorUtils.getStandardObjectInspector(inputKeyOI), ObjectInspectorUtils.getStandardObjectInspector(inputValueOI));
            } else if (m == Mode.PARTIAL2) {
                internalMergeOI = (StandardMapObjectInspector) parameters[0];
                inputKeyOI = (PrimitiveObjectInspector) internalMergeOI.getMapKeyObjectInspector();
                inputValueOI = (PrimitiveObjectInspector) internalMergeOI.getMapValueObjectInspector();
                return ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
            } else if (m == Mode.FINAL) {
                internalMergeOI = (StandardMapObjectInspector) parameters[0];
                inputKeyOI = (PrimitiveObjectInspector) internalMergeOI.getMapKeyObjectInspector();
                inputValueOI = (PrimitiveObjectInspector) internalMergeOI.getMapValueObjectInspector();
                return ObjectInspectorFactory.getStandardListObjectInspector(inputValueOI);
            } else {   // COMPLETE阶段  直接输入 timeTag和active_name
                inputKeyOI = (PrimitiveObjectInspector) parameters[0];
                inputValueOI = (PrimitiveObjectInspector) parameters[1];
                return ObjectInspectorFactory.getStandardListObjectInspector(inputValueOI);
            }
        }

        static class activeNameMapTimeAgg extends AbstractAggregationBuffer {
            Map<Object, Object> container = Maps.newHashMap();
        }

        public AbstractAggregationBuffer getNewAggregationBuffer() {
            activeNameMapTimeAgg ret = new activeNameMapTimeAgg();
            return ret;
        }

        public void reset(AggregationBuffer agg) {
            ((activeNameMapTimeAgg) agg).container.clear();
        }

        public void iterate(AggregationBuffer agg, Object[] parameters) {
            assert (parameters.length == 2);
            Object key = parameters[0];
            Object value = parameters[1];
            if (key != null && value != null) {
                activeNameMapTimeAgg my_agg = (activeNameMapTimeAgg) agg;
                Object kCopy = ObjectInspectorUtils.copyToStandardObject(key, this.inputKeyOI);
                Object vCopy = ObjectInspectorUtils.copyToStandardObject(value, this.inputValueOI);
                my_agg.container.put(kCopy, vCopy);
            }
        }

        public Object terminatePartial(AggregationBuffer agg) {
            activeNameMapTimeAgg my_agg = (activeNameMapTimeAgg) agg;
            Map<Object, Object> ret = Maps.newHashMap(my_agg.container);
            return ret;
        }

        public void merge(AggregationBuffer agg, Object partial) {
            assert (partial != null);
            activeNameMapTimeAgg my_agg = (activeNameMapTimeAgg) agg;
            Map<Object, Object> partialResult = (Map<Object, Object>) internalMergeOI.getMap(partial);
            for (Map.Entry<Object, Object> entry : partialResult.entrySet()) {
                Object kCopy = ObjectInspectorUtils.copyToStandardObject(entry.getKey(), this.inputKeyOI);
                Object vCopy = ObjectInspectorUtils.copyToStandardObject(entry.getValue(), this.inputValueOI);
                my_agg.container.put(kCopy, vCopy);
            }
        }

        public Object terminate(AggregationBuffer agg) {
            activeNameMapTimeAgg my_agg = (activeNameMapTimeAgg) agg;
            Map map = new HashMap(my_agg.container.size());
            map.putAll(my_agg.container);

            List<Map.Entry<LongWritable, Text>> listData = Lists.newArrayList(map.entrySet());
            Collections.sort(listData, new Comparator<Map.Entry<LongWritable, Text>>() {
                public int compare(Map.Entry<LongWritable, Text> o1, Map.Entry<LongWritable, Text> o2) {
                    return (o1.getKey().compareTo(o2.getKey()));
                }
            });

            List<Text> result = Lists.newArrayList();
            LongWritable currTime = listData.get(listData.size() - 1).getKey();
            for (Map.Entry<LongWritable, Text> entry : listData) {
                Long timeInterval = (currTime.get() - entry.getKey().get()) / 60000;
                if (timeInterval <= 30) {
                    result.add(entry.getValue());
                }
            }

            return result;
        }
    }
}
