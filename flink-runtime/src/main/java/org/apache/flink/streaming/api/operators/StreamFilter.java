/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link StreamOperator} for executing {@link FilterFunction FilterFunctions}.
 */
@Internal
public class StreamFilter<IN> extends AbstractUdfStreamOperator<IN, FilterFunction<IN>> implements OneInputStreamOperator<IN, IN> {

	private static final Logger LOG = LoggerFactory.getLogger(StreamFilter.class);

	private static final long serialVersionUID = 1L;

	public StreamFilter(FilterFunction<IN> filterFunction) {
		super(filterFunction);
		chainingStrategy = ChainingStrategy.ALWAYS;
		LOG.info("StreamFilter INIT");
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {

		LOG.info("StreamFilter received {}", element);

		if (userFunction.filter(element.getValue())) {
			LOG.info("Sending element: {}", element);

			output.collect(element);
		}
	}

	@Override
	public String toString() {
		return getClass().getSimpleName();
	}
}
