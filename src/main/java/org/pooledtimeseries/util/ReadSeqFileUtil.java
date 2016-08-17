/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pooledtimeseries.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Logger;

import org.apache.hadoop.io.Text;
import org.pooledtimeseries.FeatureVector;
import org.pooledtimeseries.MeanChiSquareDistanceCalculation;
import org.pooledtimeseries.PoT;

public class ReadSeqFileUtil {
	private static final Logger LOG = Logger.getLogger(MeanChiSquareDistanceCalculation.class.getName());

	/**
	 * This method computes pair of FeatureVector for each par of videos<br/>
	 * It attempts to find FeatureVector in Redis if Redis is enabled else stick
	 * back to value provide in parameter
	 * 
	 * @param key
	 *            - pair of video files
	 * @param value
	 *            - value of of and og.txt vectors
	 * @return
	 */
	public static List<FeatureVector> computeFeatureFromSeries(Text key, Text value) {

		String[] videoVectors = value.toString().split(PoTConstants.FILE_SEPERATOR);
		String[] videoName = getFileNames(key);

		ArrayList<double[]> tws = PoT.getTemporalWindows(4);
		ArrayList<FeatureVector> fvList = new ArrayList<FeatureVector>();

		for (int index = 0; index < videoVectors.length; index++) {
			ArrayList<double[][]> multiSeries = new ArrayList<double[][]>();
			long startIoTime = System.currentTimeMillis();
			Object cachedFeatureVec = RedisUtil.getObjectFromRedis(videoName[index]);
			
			if (cachedFeatureVec != null) {
				fvList.add((FeatureVector) cachedFeatureVec);
				LOG.info("Read both series from cache in - " + (System.currentTimeMillis() - startIoTime));
			
			} else {
				String video = videoVectors[index];
				String[] vectors = video.split(PoTConstants.VECTOR_SEPERATOR_REGEX);

				multiSeries.add(PoT.loadTimeSeries(new Scanner(vectors[0])));
				multiSeries.add(PoT.loadTimeSeries(new Scanner(vectors[1])));

				LOG.info("Read both series in - " + (System.currentTimeMillis() - startIoTime));

				FeatureVector fv = new FeatureVector();
				for (int i = 0; i < multiSeries.size(); i++) {
					fv.feature.add(PoT.computeFeaturesFromSeries(multiSeries.get(i), tws, 1));
					fv.feature.add(PoT.computeFeaturesFromSeries(multiSeries.get(i), tws, 2));
					fv.feature.add(PoT.computeFeaturesFromSeries(multiSeries.get(i), tws, 5));
				}
				RedisUtil.setObjectInRedisAsync(videoName[index],fv);
				fvList.add(fv);
			}
		}

		return fvList;

	}

	public static String[] getFileNames(Text key) {

		return key.toString().split(PoTConstants.FILE_SEPERATOR);
	}

}
