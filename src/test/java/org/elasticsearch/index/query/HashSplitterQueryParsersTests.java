/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.query;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.node.Node;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.client.Requests.countRequest;
import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.client.Requests.deleteIndexRequest;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.client.Requests.putMappingRequest;
import static org.elasticsearch.client.Requests.refreshRequest;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.HashSplitterTermFilterBuilder.hashSplitterTermFilter;
import static org.elasticsearch.index.query.HashSplitterTermQueryBuilder.hashSplitterTermQuery;
import static org.elasticsearch.index.query.HashSplitterWildcardFilterBuilder.hashSplitterWildcardFilter;
import static org.elasticsearch.index.query.HashSplitterWildcardQueryBuilder.hashSplitterWildcardQuery;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@Test
public class HashSplitterQueryParsersTests {

    private final ESLogger logger = Loggers.getLogger(getClass());

    private Node node;

    @BeforeClass
    public void setupServer() {
        node = nodeBuilder().local(true).settings(settingsBuilder()
                .put("path.data", "target/data")
                .put("cluster.name", "test-cluster-" + NetworkUtils.getLocalAddress())
                .put("gateway.type", "none")).node();
    }

    @AfterClass
    public void closeServer() {
        node.close();
    }

    @BeforeMethod
    private void createIndex() {
        logger.info("creating index [test]");
        node.client().admin().indices().create(createIndexRequest("test").settings(settingsBuilder().put("index.numberOfReplicas", 0).put("index.numberOfShards", 1))).actionGet();
        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = node.client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.status());
        assertThat(clusterHealth.timedOut(), equalTo(false));
        assertThat(clusterHealth.status(), equalTo(ClusterHealthStatus.GREEN));
    }

    @AfterMethod
    private void deleteIndex() {
        logger.info("deleting index [test]");
        node.client().admin().indices().delete(deleteIndexRequest("test")).actionGet();
    }

    @Test
    public void testTermQuery() throws Exception {
        String mapping = copyToStringFromClasspath("/chunklength4-prefixesLowercasedAlphabet-mapping.json");

        node.client().admin().indices().putMapping(putMappingRequest("test").type("splitted_hashes").source(mapping)).actionGet();

        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "0000111122223333").endObject())).actionGet();
        node.client().admin().indices().refresh(refreshRequest()).actionGet();

        CountResponse countResponse;

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(hashSplitterTermQuery("hash", "b1111"))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("term query", countResponse.count(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(hashSplitterTermQuery("hash", "a000"))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("term query on a prefix", countResponse.count(), equalTo(0l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(hashSplitterTermQuery("hash", "z9999"))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("term query on inexistent term", countResponse.count(), equalTo(0l));
    }

    @Test
    public void testTermFilter() throws Exception {
        String mapping = copyToStringFromClasspath("/chunklength4-prefixesLowercasedAlphabet-mapping.json");

        node.client().admin().indices().putMapping(putMappingRequest("test").type("splitted_hashes").source(mapping)).actionGet();

        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "0000111122223333").endObject())).actionGet();
        node.client().admin().indices().refresh(refreshRequest()).actionGet();

        CountResponse countResponse;

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(filteredQuery(matchAllQuery(), hashSplitterTermFilter("hash", "b1111")))).actionGet();
        assertThat("term filter registered", countResponse.failedShards(), equalTo(0));
        assertThat("term filter", countResponse.count(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(filteredQuery(matchAllQuery(), hashSplitterTermFilter("hash", "a000")))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("term filter on a prefix", countResponse.count(), equalTo(0l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(filteredQuery(matchAllQuery(), hashSplitterTermFilter("hash", "z9999")))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("term filter on inexistent term", countResponse.count(), equalTo(0l));
    }

    @Test
    public void testWildcardQueryVariableSize() throws Exception {
        String mapping = copyToStringFromClasspath("/chunklength4-prefixesLowercasedAlphabet-mapping.json");

        node.client().admin().indices().putMapping(putMappingRequest("test").type("splitted_hashes").source(mapping)).actionGet();

        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "0000111122223333").endObject())).actionGet();
        node.client().admin().indices().refresh(refreshRequest()).actionGet();

        CountResponse countResponse;

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(hashSplitterWildcardQuery("hash", "????1111*"))).actionGet();
        assertThat("wildcard query registered", countResponse.failedShards(), equalTo(0));
        assertThat("wildcard query", countResponse.count(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(hashSplitterWildcardQuery("hash", "000*"))).actionGet();
        assertThat("wildcard query on a prefix", countResponse.count(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(hashSplitterWildcardQuery("hash", "*3333"))).actionGet();
        assertThat("wildcard query on a suffix with default variable size", countResponse.count(), equalTo(0l)); // no match because of variable size

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(hashSplitterWildcardQuery("hash", "000*3"))).actionGet();
        assertThat("wildcard query on a prefix and suffix", countResponse.count(), equalTo(0l)); // no match because of variable size

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(hashSplitterWildcardQuery("hash", "99*99"))).actionGet();
        assertThat("wildcard query on inexistent term", countResponse.count(), equalTo(0l));
    }

    @Test
    public void testWildcardQueryVariableSizeAlternate() throws Exception {
        String mapping = copyToStringFromClasspath("/chunklength4-prefixesLowercasedAlphabet-SqlWildcards-mapping.json");

        node.client().admin().indices().putMapping(putMappingRequest("test").type("splitted_hashes").source(mapping)).actionGet();

        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "0000111122223333").endObject())).actionGet();
        node.client().admin().indices().refresh(refreshRequest()).actionGet();

        CountResponse countResponse;

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(hashSplitterWildcardQuery("hash", "____1111%"))).actionGet();
        assertThat("wildcard query registered", countResponse.failedShards(), equalTo(0));
        assertThat("wildcard query with SQL-flavoured wildcards", countResponse.count(), equalTo(1l));
    }

    @Test
    public void testWildcardFilterVariableSize() throws Exception {
        String mapping = copyToStringFromClasspath("/chunklength4-prefixesLowercasedAlphabet-mapping.json");

        node.client().admin().indices().putMapping(putMappingRequest("test").type("splitted_hashes").source(mapping)).actionGet();

        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "0000111122223333").endObject())).actionGet();
        node.client().admin().indices().refresh(refreshRequest()).actionGet();

        CountResponse countResponse;

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(filteredQuery(matchAllQuery(), hashSplitterWildcardFilter("hash", "????1111*")))).actionGet();
        assertThat("wildcard filter registered", countResponse.failedShards(), equalTo(0));
        assertThat("wildcard filter", countResponse.count(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(filteredQuery(matchAllQuery(), hashSplitterWildcardFilter("hash", "000*")))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("wildcard filter on a prefix", countResponse.count(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(filteredQuery(matchAllQuery(), hashSplitterWildcardFilter("hash", "*3333")))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("wildcard filter on a suffix with default variable size", countResponse.count(), equalTo(0l)); // no match because of variable size

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(filteredQuery(matchAllQuery(), hashSplitterWildcardFilter("hash", "000*3")))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("wildcard filter on a prefix and suffix", countResponse.count(), equalTo(0l)); // no match because of variable size

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(filteredQuery(matchAllQuery(), hashSplitterWildcardFilter("hash", "99*99")))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("wildcard filter on inexistent term", countResponse.count(), equalTo(0l));
    }

    @Test
    public void testWildcardFilterVariableSizeAlternate() throws Exception {
        String mapping = copyToStringFromClasspath("/chunklength4-prefixesLowercasedAlphabet-SqlWildcards-mapping.json");

        node.client().admin().indices().putMapping(putMappingRequest("test").type("splitted_hashes").source(mapping)).actionGet();

        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "0000111122223333").endObject())).actionGet();
        node.client().admin().indices().refresh(refreshRequest()).actionGet();

        CountResponse countResponse;

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(filteredQuery(matchAllQuery(), hashSplitterWildcardFilter("hash", "____1111%")))).actionGet();
        assertThat("wildcard query registered", countResponse.failedShards(), equalTo(0));
        assertThat("wildcard query with SQL-flavoured wildcards", countResponse.count(), equalTo(1l));
    }

    @Test
    public void testWildcardQueryFixedSize() throws Exception {
        String mapping = copyToStringFromClasspath("/chunklength4-prefixesLowercasedAlphabet-size16Fixed-mapping.json");

        node.client().admin().indices().putMapping(putMappingRequest("test").type("splitted_hashes").source(mapping)).actionGet();

        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "0000111122223333").endObject())).actionGet();
        node.client().admin().indices().refresh(refreshRequest()).actionGet();

        CountResponse countResponse;

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(hashSplitterWildcardQuery("hash", "????1111*"))).actionGet();
        assertThat("wildcard query registered", countResponse.failedShards(), equalTo(0));
        assertThat("wildcard query", countResponse.count(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(hashSplitterWildcardQuery("hash", "000*"))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("wildcard query on a prefix", countResponse.count(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(hashSplitterWildcardQuery("hash", "*3333"))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("wildcard query on a suffix with fixed size", countResponse.count(), equalTo(1l)); // matches because of fixed size

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(hashSplitterWildcardQuery("hash", "000*3"))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("wildcard query on a prefix and suffix", countResponse.count(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(hashSplitterWildcardQuery("hash", "99*99"))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("wildcard query on inexistent term", countResponse.count(), equalTo(0l));
    }

    @Test
    public void testWildcardFilterFixedSize() throws Exception {
        String mapping = copyToStringFromClasspath("/chunklength4-prefixesLowercasedAlphabet-size16Fixed-mapping.json");

        node.client().admin().indices().putMapping(putMappingRequest("test").type("splitted_hashes").source(mapping)).actionGet();

        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "0000111122223333").endObject())).actionGet();
        node.client().admin().indices().refresh(refreshRequest()).actionGet();

        CountResponse countResponse;

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(filteredQuery(matchAllQuery(), hashSplitterWildcardFilter("hash", "????1111*")))).actionGet();
        assertThat("wildcard filter registered", countResponse.failedShards(), equalTo(0));
        assertThat("wildcard filter", countResponse.count(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(filteredQuery(matchAllQuery(), hashSplitterWildcardFilter("hash", "000*")))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("wildcard filter on a prefix", countResponse.count(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(filteredQuery(matchAllQuery(), hashSplitterWildcardFilter("hash", "*3333")))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("wildcard filter on a suffix with fixed size", countResponse.count(), equalTo(1l)); // matches because of fixed size

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(filteredQuery(matchAllQuery(), hashSplitterWildcardFilter("hash", "000*3")))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("wildcard filter on a prefix and suffix", countResponse.count(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(filteredQuery(matchAllQuery(), hashSplitterWildcardFilter("hash", "99*99")))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("wildcard filter on inexistent term", countResponse.count(), equalTo(0l));
    }

    @Test
    public void testCoverage() throws Exception {
        String mapping = copyToStringFromClasspath("/chunklength4-prefixesLowercasedAlphabet-size16Fixed-mapping.json");

        node.client().admin().indices().putMapping(putMappingRequest("test").type("splitted_hashes").source(mapping)).actionGet();

        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "0000111122223333").endObject())).actionGet();
        node.client().admin().indices().refresh(refreshRequest()).actionGet();

        CountResponse countResponse;

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(hashSplitterTermQuery("hash", "a0000").boost(2.0f))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat(countResponse.count(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(filteredQuery(matchAllQuery(), hashSplitterTermFilter("hash", "a0000").filterName("hash:a0000").cache(true).cacheKey("hash:a0000")))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat(countResponse.count(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(filteredQuery(matchAllQuery(), hashSplitterWildcardFilter("hash", "000*").filterName("hash:0000*").cache(true).cacheKey("hash:000*")))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat(countResponse.count(), equalTo(1l));
    }

}
