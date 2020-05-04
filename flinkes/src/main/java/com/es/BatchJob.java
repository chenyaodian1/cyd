/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.es;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.types.Row;
import org.apache.http.HttpHost;
import org.drools.core.io.impl.ClassPathResource;
import org.drools.core.util.StringUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.kie.api.KieBase;
import org.kie.api.runtime.StatelessKieSession;
import org.kie.internal.utils.KieHelper;
import com.es.ElasticsearchOutputFormat.Builder;
import com.alibaba.fastjson.JSONObject;
/**
 * Skeleton for a Flink Batch Job.
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class BatchJob {

	@SuppressWarnings("rawtypes")
	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		//System.out.println("start batch drools*************:");
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<String> source=env.readTextFile("hdfs://15.119.6.153:8020/test/user.txt");
		source.print();
		DataSet<Person> input=source.map(new RichMapFunction<String, Person>() {

			@Override
			public Person map(String arg0) throws Exception {
				// TODO Auto-generated method stub
				Person s =new Person();
				if(!StringUtils.isEmpty(arg0))
				{
				String[] strs =arg0.split("-");
				
				if(strs!=null && strs.length>1)
				{
					s.setId(strs[0]);
					s.setName(strs[0]);
					s.setSalary(Long.valueOf(strs[2]));
					s.setAge(Long.valueOf(strs[1]));
				}
				}
				return s;
			}
		});
		 DataSet<Person> input1 =input.map(new RichMapFunction<Person, Person>() {
				public Person map(Person s) throws Exception {
					//System.out.println("start**************stream2:");
					KieHelper kieHelper = new KieHelper();
					// kieHelper.addContent()
					kieHelper.kfs.write(new ClassPathResource("rules/Test.drl"));
					KieBase kbase = kieHelper.build();
					StatelessKieSession statelessKieSession = kbase.newStatelessKieSession();
					statelessKieSession.execute(s);
					return s;
				}
			});
		 
		 final ElasticsearchSinkFunction<Person> elasticsearchSinkFunction = new ElasticsearchSinkFunction<Person>() {
			 
			   @Override
			   public void process(Person element, RuntimeContext ctx, RequestIndexer indexer) {
			     indexer.add(createIndexRequest(element));
			   }
			 
			   private IndexRequest createIndexRequest(Person element) {
			     //JSONObject o = JSONObject.parseObject(element);
				   Map<String, String> json = new HashMap<String, String>();
			    	json.put("id", element.getName());
			 	    json.put("name", element.getName());
			 	    json.put("age", String.valueOf(element.getAge()));
			 	    json.put("salary", String.valueOf(element.getSalary()));
			 	    json.put("adult", String.valueOf(element.getIsAdult()));
			        return Requests.indexRequest()
			             .index("mdms2")
			             .type("user2")
			             .source(json);
			   }
			 };
		 
		 
		 String esHttpHosts = "myhost1.example.com,myhost2.example.com,myhost3.example.com";
		 int esHttpPort = 9200;
		 final List<HttpHost> httpHosts = Arrays.asList(esHttpHosts.split(","))
		         .stream()
		         .map(host -> new HttpHost(host, esHttpPort, "http"))
		         .collect(Collectors.toList());
		  
		 int bulkFlushMaxSizeMb =10;
		 int bulkFlushIntervalMillis = 10000;
		 int bulkFlushMaxActions = 1;
		 
		 final ElasticsearchOutputFormat outputFormat = new Builder<>(httpHosts, elasticsearchSinkFunction)
			        .setBulkFlushBackoff(true)
			        .setBulkFlushBackoffRetries(2)
			        .setBulkFlushBackoffType(ElasticsearchApiCallBridge.FlushBackoffType.EXPONENTIAL)
			        .setBulkFlushMaxSizeMb(bulkFlushMaxSizeMb)
			        .setBulkFlushInterval(bulkFlushIntervalMillis)
			        .setBulkFlushMaxActions(bulkFlushMaxActions)
			        .build();
		 
		 input1.output(outputFormat);
		 
		 input1.map(new RichMapFunction<Person, String>() {
				public String map(Person s) throws Exception {
					return s.getName()+"-"+s.getAge()+"-"+s.getSalary()+"-"+s.getIsAdult();
				}
		 }).print();
		 
		 env.execute("flink-batch-es");
		
		/**input1.map(new RichMapFunction<Person, String>() {
				public String map(Person s) throws Exception {
					return s.getName()+"-"+s.getAge()+"-"+s.getSalary()+"-"+s.getIsAdult();
				}
		 }).print();
		 Map<String, String> config = new HashMap<String, String>();
		 config.put("bulk.flush.max.actions", "10");
		 config.put("cluster.name", "mdms");
		 String hosts = "myhost1.example.com,myhost2.example.com,myhost3.example.com";
		 List<InetSocketAddress> list = Lists.newArrayList();
		 for (String host : hosts.split(",")) {
		     list.add(new InetSocketAddress(InetAddress.getByName(host), 9300));
		 }*/
		 //input1.output((OutputFormat<Person>) new ElasticsearchSink.Builder(list, new NumberOfTransactionsByBlocks()));
		/** input1.output(new ElasticSearchOutputFormat<Person>(config, list, new ElasticsearchSinkFunction<Person>() {
			    public void process(Person element, RuntimeContext ctx, RequestIndexer indexer) {
			        indexer.add(createIndexRequest(element));
			    }

			    private IndexRequest createIndexRequest(Person element) {
			    	Map<String, String> json = new HashMap<String, String>();
			    	json.put("id", element.getName());
			 	    json.put("name", element.getName());
			 	    json.put("age", String.valueOf(element.getAge()));
			 	    json.put("salary", String.valueOf(element.getSalary()));
			 	    json.put("adult", String.valueOf(element.getIsAdult()));
			        return Requests.indexRequest().index("mdms2").type("user2").source(json);
			    }
			}));*/
	}
	

}
