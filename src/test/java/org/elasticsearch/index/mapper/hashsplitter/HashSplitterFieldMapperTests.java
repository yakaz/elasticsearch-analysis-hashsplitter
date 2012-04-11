/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.mapper.hashsplitter;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.PrefixFilterBuilder;
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
import static org.elasticsearch.index.query.QueryBuilders.fieldQuery;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@Test
public class HashSplitterFieldMapperTests {

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
    public void testBasicMapping() throws Exception {
        String mapping = copyToStringFromClasspath("/basic-mapping.json");

        node.client().admin().indices().putMapping(putMappingRequest("test").type("splitted_hashes").source(mapping)).actionGet();

        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "0011223344556677").endObject())).actionGet();
        node.client().admin().indices().refresh(refreshRequest()).actionGet();

        CountResponse countResponse;

        countResponse = node.client().count(countRequest("test").query(fieldQuery("hash", "0011223344556677"))).actionGet();
        assertThat("field query on exact value", countResponse.count(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").query(fieldQuery("hash", "0011223344556688"))).actionGet();
        assertThat("field query on different value, same prefix", countResponse.count(), equalTo(0l));
    }

    @Test
    public void testPrefixQueries() throws Exception {
        String mapping = copyToStringFromClasspath("/chunklength2-mapping.json");

        node.client().admin().indices().putMapping(putMappingRequest("test").type("splitted_hashes").source(mapping)).actionGet();

        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "0011223344556677").endObject())).actionGet();
        node.client().admin().indices().refresh(refreshRequest()).actionGet();

        CountResponse countResponse;

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(prefixQuery("hash", "00112233445566"))).actionGet();
        assertThat("prefix query", countResponse.count(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(prefixQuery("hash", "0011223344556"))).actionGet();
        assertThat("prefix query with incomplete last chunk", countResponse.count(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(prefixQuery("hash", "00112233445567"))).actionGet();
        assertThat("prefix query with unexisting prefix", countResponse.count(), equalTo(0l));
    }

    private static FilterBuilder prefixFilter(String name, String prefix) {
        return new PrefixFilterBuilder(name, prefix);
    }

    @Test
    public void testPrefixFilters() throws Exception {
        String mapping = copyToStringFromClasspath("/chunklength2-mapping.json");

        node.client().admin().indices().putMapping(putMappingRequest("test").type("splitted_hashes").source(mapping)).actionGet();

        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "0011223344556677").endObject())).actionGet();
        node.client().admin().indices().refresh(refreshRequest()).actionGet();

        CountResponse countResponse;

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(filteredQuery(matchAllQuery(), prefixFilter("hash", "00112233445566")))).actionGet();
        assertThat("prefix filter", countResponse.count(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(filteredQuery(matchAllQuery(), prefixFilter("hash", "0011223344556")))).actionGet();
        assertThat("prefix filter with incomplete last chunk", countResponse.count(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(filteredQuery(matchAllQuery(), prefixFilter("hash", "00112233445567")))).actionGet();
        assertThat("prefix filter with unexisting prefix", countResponse.count(), equalTo(0l));
    }

}
