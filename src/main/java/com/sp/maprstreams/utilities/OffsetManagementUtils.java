package com.sp.maprstreams.utilities;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.JobConf;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.kafka09.OffsetRange;

public class OffsetManagementUtils {

	/** LOG handle */
	private static final Logger LOGGER = Logger.getLogger(OffsetManagementUtils.class);

	/**
	 * 
	 * Save the offset values from the streams to MapR DB table.
	 * 
	 * @param topic
	 * @param groupID
	 * @param offsets
	 * @param offsetTableName
	 * @param batchTime
	 */
	public static void saveOffsets(String topic, String groupID, OffsetRange[] offsets, String offsetTableName,
			Long batchTime) {
		JobConf jobConf = null;
		try {
			jobConf = getHbaseConf(offsetTableName);
			Connection connection = ConnectionFactory.createConnection(jobConf);
			Table table = connection.getTable(TableName.valueOf(offsetTableName));
			String rowKey = topic + ":" + groupID + ":" + String.valueOf(Long.MAX_VALUE - batchTime);
			Put put = new Put(Bytes.toBytes(rowKey));
			for (OffsetRange offset : offsets) {

				putColumnValue(put, "offsets", String.valueOf(offset.partition()),
						String.valueOf(offset.untilOffset()));

			}
			table.put(put);
			table.close();
			connection.close();

		} catch (IOException e) {
			LOGGER.error("ERROR while saving offset data to offset table" + e.getMessage());
		}

	}

	/**
	 * Used to retrieve the latest committed offset from MapR DB table. This will be
	 * used to start the Stream from the last completed run.
	 * 
	 * @param topics
	 * @param groupID
	 * @param offsetTableName
	 * @param totalNumberOfPartitionsForTopic
	 * @return
	 */
	public static Map<TopicPartition, Long> getLastCommitedOffsets(List<String> topics, String groupID,
			String offsetTableName, int totalNumberOfPartitionsForTopic) {

		Map<TopicPartition, Long> fromOffsets = new HashMap<>();
		JobConf jobConf = null;
		try {
			jobConf = getHbaseConf(offsetTableName);
			Connection connection = ConnectionFactory.createConnection(jobConf);
			Table table = connection.getTable(TableName.valueOf(offsetTableName));

			for (String topic : topics) {

				String stopRow = topic + ":" + groupID + ":" + String.valueOf(Long.MAX_VALUE);
				String startRow = topic + ":" + groupID + ":" + String.valueOf(0);

				// Set the number of partitions discovered for a topic in MapRDB to 0
				int mapRDBNumberOfPartitionsForTopic = 0;

				Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow));
				ResultScanner scanner = table.getScanner(scan);
				Result result = null;
				if (scanner.next(1) != null) {

					result = scanner.next(1)[0];
				}

				if (null != result) {
					// If the result from hbase scanner is not null, set number of partitions from
					// MapRDB to the number of cells

					mapRDBNumberOfPartitionsForTopic = result.listCells().size();
				}

				if (mapRDBNumberOfPartitionsForTopic == 0) {

					// initialize fromOffsets to beginning
					for (int partition = 0; partition <= (totalNumberOfPartitionsForTopic - 1); partition++) {

						fromOffsets.put(new TopicPartition(topic, partition), 0L);
					}
				} else if (totalNumberOfPartitionsForTopic > mapRDBNumberOfPartitionsForTopic) {

					for (int partition = 0; partition <= (mapRDBNumberOfPartitionsForTopic - 1); partition++) {

						Long fromOffset = Bytes.toLong(
								result.getValue(Bytes.toBytes("offsets"), Bytes.toBytes(String.valueOf(partition))));
						fromOffsets.put(new TopicPartition(topic, partition), fromOffset);

					}
					// handle scenario where new partitions have been added to existing streams
					// topic
					for (int partition = mapRDBNumberOfPartitionsForTopic; partition <= (totalNumberOfPartitionsForTopic
							- 1); partition++) {

						fromOffsets.put(new TopicPartition(topic, partition), 0L);
					}
				} else {
					// initialize fromOffsets from last run
					for (int partition = 0; partition <= (mapRDBNumberOfPartitionsForTopic - 1); partition++) {

						byte[] family = "offsets".getBytes();
						byte[] column = Bytes.toBytes(String.valueOf(partition));
						String fromOffset = Bytes.toString(result.getValue(family, column));

						fromOffsets.put(new TopicPartition(topic, partition), Long.valueOf(fromOffset));

					}
				}
				scanner.close();
				table.close();
				connection.close();

			}
		} catch (IOException e) {

			LOGGER.error("got error while getting the offsets from offset table" + e);
		}

		return fromOffsets;
	}

	/**
	 * Retrieves @JobConf object to submit MaprDB bulk put job.
	 * 
	 * @param targetTablePath
	 *            the table name with name-space to which data will be loaded.
	 * @return jobConf the JobConf object.
	 * @throws IOException
	 */
	public static JobConf getHbaseConf(String targetTablePath) throws IOException {
		JobConf jobConf = null;
		Configuration hbaseConfiguration = HBaseConfiguration.create();
		if (null != hbaseConfiguration) {
			Job job = Job.getInstance(hbaseConfiguration);
			if (null != job) {
				jobConf = (JobConf) job.getConfiguration();
				if (null != jobConf) {
					jobConf.setOutputFormat(TableOutputFormat.class);
					jobConf.set(TableOutputFormat.OUTPUT_TABLE, targetTablePath);
				}

			}
		}

		return jobConf;
	}

	/**
	 * Map the values in binary form into the @Put object.
	 * 
	 * @param put
	 *            the put object to which the column value to be added.
	 * @param cfName
	 *            the name of the Column Family.
	 * @param cqName
	 *            the name of the Column Qualifier.
	 * @param value
	 *            the value to be set for the Column Qualifier.
	 */
	public static void putColumnValue(Put put, String cfName, String cqName, String value) {
		if (StringUtils.isNotBlank(value)) {
			put.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(cqName), Bytes.toBytes(value.trim()));
		}

	}

}
