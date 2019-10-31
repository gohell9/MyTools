package com.xsh.HBaseMR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @program: MyTools
 * @description:
 * @author: Mr.X Shaw
 * @create: 2019-10-31 17:03
 * goal:  将fruit表中的一部分数据，通过MR迁入到fruit_mr表中。
 * run:
 * yarn jar HBaseAPI-1.0-SNAPSHOT.jar com.xsh.HBaseMR.Fruit2FruitMRRunner
 **/

public class Fruit2FruitMRRunner extends Configured implements Tool {

    //组装Job
    public int run(String[] args) throws Exception {
        //得到Configuration
        Configuration conf = this.getConf();
        //创建Job任务
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(Fruit2FruitMRRunner.class);

        //配置Job
        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(500);

        //设置Mapper，注意导入的是mapreduce包下的，不是mapred包下的，后者是老版本
        TableMapReduceUtil.initTableMapperJob(
                "fruit", //数据源的表名
                scan, //scan扫描控制器
                ReadFruitMapper.class,//设置Mapper类
                ImmutableBytesWritable.class,//设置Mapper输出key类型
                Put.class,//设置Mapper输出value值类型
                job//设置给哪个JOB
        );
        //设置Reducer ;  "fruit_mr"  The output table
        TableMapReduceUtil.initTableReducerJob("fruit_mr", WriteFruitMRReducer.class, job);
        //设置Reduce数量，最少1个
        job.setNumReduceTasks(1);

        boolean isSuccess = job.waitForCompletion(true);
        if(!isSuccess){
            throw new IOException ("Job running with error");
        }
        return isSuccess ? 0 : 1;
    }

    public static void main( String[] args ) throws Exception{
        Configuration conf = HBaseConfiguration.create();
        int status = ToolRunner.run(conf, new Fruit2FruitMRRunner(), args);
        System.exit(status);
    }


}
