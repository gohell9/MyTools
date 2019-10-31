package com.xsh.HBase_MapReduce2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * @program: MyTools
 * @description:
 * @author: Mr.X Shaw
 * @create: 2019-10-31 17:13
 * run:
 * yarn jar hbase-0.0.1-SNAPSHOT.jar com.xsh.HBase_MapReduce2.Txt2FruitRunner
 **/

public class Txt2FruitRunner extends Configured implements Tool {
    public int run(String[] args) throws Exception {
//得到Configuration
        Configuration conf = this.getConf();

//创建Job任务
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(Txt2FruitRunner.class);
        Path inPath = new Path("hdfs://hadoop102:9000/input_fruit/fruit.tsv");
        FileInputFormat.addInputPath(job, inPath);

//设置Mapper
        job.setMapperClass(ReadFruitFromHDFSMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

//设置Reducer
        TableMapReduceUtil.initTableReducerJob("fruit_mr", WriteFruitMRFromTxtReducer.class, job);

//设置Reduce数量，最少1个
        job.setNumReduceTasks(1);

        boolean isSuccess = job.waitForCompletion(true);
        if(!isSuccess){
            throw new IOException ("Job running with error");
        }

        return isSuccess ? 0 : 1;
    }


}

