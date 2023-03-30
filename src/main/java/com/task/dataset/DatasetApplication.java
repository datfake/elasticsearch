package com.task.dataset;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DatasetApplication {

	public static void main(String[] args) throws IOException {

		ElasticsearchClient client = getElasticsearchClient();

		String rootPath = new File("").getAbsolutePath();
		String modelData = FileUtils.readFileToString(new File(rootPath +"/zyft_test_data.txt"), Charset.defaultCharset());
		String[] tkArr = StringUtils.splitByWholeSeparator(modelData, ",");
		List<String> tks =  Arrays.asList(tkArr);

		SearchRequest.Builder searchRequest = new SearchRequest.Builder();
		searchRequest.i
		searchRequest.types("doc");

		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.termsQuery("values", 0.0, 1.02, 1.03));
		searchSourceBuilder.sort(SortBuilders.fieldSort("some_field").order(SortOrder.DESC));
		searchSourceBuilder.fetchSource(new FetchSourceContext(true, new String[]{"field1", "field2"}));

		searchRequest.source(searchSourceBuilder);

		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		System.out.println(searchResponse);

//		int partitionSize = (int) Math.ceil(tks.size() / (double) 10);
//		for (int i = 0; i < tks.size(); i += partitionSize) {
//		   BulkRequest.Builder br = new BulkRequest.Builder();
//		   final List<String> sublist = tks.subList(i, Math.min(i + partitionSize, tks.size()));
//			for (String tk : sublist) {
////				Data data = new Data();
////				data.setValue(tk);
//				br.operations(op -> op
//						.index(idx -> idx
//								.index("my-data")
//								.document(tk)
//						)
//				);
//				client.bulk(br.build());
//			}
//		}

		System.out.println("done");
	}

	private static ElasticsearchClient getElasticsearchClient() {
		RestClientBuilder builder = RestClient.builder(new HttpHost("b788583f672543918144aed9bc5f3ab9.australia-southeast1.gcp.elastic-cloud.com", 443, "https"));

		RestClientBuilder.HttpClientConfigCallback httpClientConfigCallback = new RestClientBuilder.HttpClientConfigCallback() {
			@Override
			public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {

				final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

				AuthScope authScope = new AuthScope("b788583f672543918144aed9bc5f3ab9.australia-southeast1.gcp.elastic-cloud.com", 443, "https");
				UsernamePasswordCredentials usernamePasswordCredentials = new UsernamePasswordCredentials("elastic",
						"SYTS1wMn3XXEm8TDpwOJiQUT");

				credentialsProvider.setCredentials(authScope, usernamePasswordCredentials);

				return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
			}
		};

		builder.setHttpClientConfigCallback(httpClientConfigCallback);

		RestClient restClient = builder.build();

		ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());

		return new ElasticsearchClient(transport);
	}

	public static <T> List<List<T>> splitList(List<T> list, int numParts) {
		int size = list.size();
		int chunkSize = size / numParts;
		int remainder = size % numParts;
		int idx = 0;

		List<List<T>> result = new ArrayList<>();
		for (int i = 0; i < numParts; i++) {
			int partSize = chunkSize + (i < remainder ? 1 : 0);
			result.add(list.subList(idx, idx + partSize));
			idx += partSize;
		}

		return result;
	}
}
