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
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.QueryBuilders.fieldQuery;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryString;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.textQuery;
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
                .source(jsonBuilder().startObject().field("hash", "01234567").endObject())).actionGet();
        node.client().admin().indices().refresh(refreshRequest()).actionGet();

        CountResponse countResponse;

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(fieldQuery("hash", "01234567"))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("field query on exact value", countResponse.count(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(fieldQuery("hash", "0123456"))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("field query on a prefix", countResponse.count(), equalTo(1l)); // should match, unfortunately!

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(fieldQuery("hash", "01234568"))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
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
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("prefix query", countResponse.count(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(prefixQuery("hash", "0011223344556"))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("prefix query with incomplete last chunk", countResponse.count(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(prefixQuery("hash", "00112233445567"))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
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
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("prefix filter", countResponse.count(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(filteredQuery(matchAllQuery(), prefixFilter("hash", "0011223344556")))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("prefix filter with incomplete last chunk", countResponse.count(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(filteredQuery(matchAllQuery(), prefixFilter("hash", "00112233445567")))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("prefix filter with unexisting prefix", countResponse.count(), equalTo(0l));
    }

    @Test
    public void testTermQueries() throws Exception {
        String mapping = copyToStringFromClasspath("/chunklength2-mapping.json");

        node.client().admin().indices().putMapping(putMappingRequest("test").type("splitted_hashes").source(mapping)).actionGet();

        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "0011223344556677").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "00______________").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "__11____________").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "____22__________").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "______33________").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "________44______").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "__________55____").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "____________66__").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "______________77").endObject())).actionGet();
        node.client().admin().indices().refresh(refreshRequest()).actionGet();

        CountResponse countResponse;

        // We would like these to work, but it doesn't seem possible...
        // (ie. having a term query that is *not analyzed*. it instead goes through fieldQuery)
//        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(termQuery("hash", "A00"))).actionGet();
//        assertThat("term query", countResponse.count(), equalTo(2l));
//
//        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(termQuery("hash", "B77"))).actionGet();
//        assertThat("term query with unexisting term", countResponse.count(), equalTo(0l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(termQuery("hash", "0011223344556677"))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("term query on exact value", countResponse.count(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(termQuery("hash", "00112233445566"))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("term query on a prefix", countResponse.count(), equalTo(1l)); // should match, unfortunately!

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(termQuery("hash", "0011223344556"))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("term query on a prefix with incomplete chunk", countResponse.count(), equalTo(0l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(termQuery("hash", "0011223344556688"))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("term query on different value, same prefix", countResponse.count(), equalTo(0l));
    }

    @Test
    public void testTermFilters() throws Exception {
        String mapping = copyToStringFromClasspath("/chunklength2-mapping.json");

        node.client().admin().indices().putMapping(putMappingRequest("test").type("splitted_hashes").source(mapping)).actionGet();

        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "0011223344556677").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "00______________").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "__11____________").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "____22__________").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "______33________").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "________44______").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "__________55____").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "____________66__").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "______________77").endObject())).actionGet();
        node.client().admin().indices().refresh(refreshRequest()).actionGet();

        CountResponse countResponse;

        // We would like these to work, but it doesn't seem possible...
        // (ie. having a term filter that is *not analyzed*. it instead goes through fieldFilter)
//        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(filteredQuery(matchAllQuery(), termFilter("hash", "A00")))).actionGet();
//        assertThat("term filter", countResponse.count(), equalTo(2l));
//
//        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(filteredQuery(matchAllQuery(), termFilter("hash", "B77")))).actionGet();
//        assertThat("term filter with unexisting term", countResponse.count(), equalTo(0l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(filteredQuery(matchAllQuery(), termFilter("hash", "0011223344556677")))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("term filter on exact value", countResponse.count(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(filteredQuery(matchAllQuery(), termFilter("hash", "00112233445566")))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("term filter on a prefix", countResponse.count(), equalTo(1l)); // should match, unfortunately!

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(filteredQuery(matchAllQuery(), termFilter("hash", "0011223344556")))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("term filter on a prefix with incomplete chunk", countResponse.count(), equalTo(0l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(filteredQuery(matchAllQuery(), termFilter("hash", "0011223344556688")))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("term filter on different value, same prefix", countResponse.count(), equalTo(0l));
    }

    @Test
    public void testTextQueries() throws Exception {
        String mapping = copyToStringFromClasspath("/chunklength2-mapping.json");

        node.client().admin().indices().putMapping(putMappingRequest("test").type("splitted_hashes").source(mapping)).actionGet();

        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "0011223344556677").endObject())).actionGet();
        node.client().admin().indices().refresh(refreshRequest()).actionGet();

        CountResponse countResponse;

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(textQuery("hash", "0011223344556677"))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("text query on exact value", countResponse.count(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(textQuery("hash", "00112233445566"))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("text query on a prefix", countResponse.count(), equalTo(1l)); // should match, unfortunately!

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(textQuery("hash", "0011223344556"))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("text query on a prefix with incomplete chunk", countResponse.count(), equalTo(0l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(textQuery("hash", "0011223344556688"))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("text query on different value, same prefix", countResponse.count(), equalTo(0l));
    }

    @Test
    public void testStringQueries() throws Exception {
        String mapping = copyToStringFromClasspath("/chunklength2-mapping.json");

        node.client().admin().indices().putMapping(putMappingRequest("test").type("splitted_hashes").source(mapping)).actionGet();

        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "0011223344556677").endObject())).actionGet();
        node.client().admin().indices().refresh(refreshRequest()).actionGet();

        CountResponse countResponse;

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(queryString("0011223344556677").defaultField("hash"))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("string query on exact value", countResponse.count(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(queryString("00112233445566").defaultField("hash"))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("string query on a prefix", countResponse.count(), equalTo(1l)); // should match, unfortunately!

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(queryString("0011223344556").defaultField("hash"))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("string query on a prefix with incomplete chunk", countResponse.count(), equalTo(0l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(queryString("0011223344556688").defaultField("hash"))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("string query on different value, same prefix", countResponse.count(), equalTo(0l));
    }

    @Test
    public void testRangeQueries() throws Exception {
        String mapping = copyToStringFromClasspath("/chunklength4-prefixesLowercasedAlphabet-size16Fixed-mapping.json");

        node.client().admin().indices().putMapping(putMappingRequest("test").type("splitted_hashes").source(mapping)).actionGet();

        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "0000000000000000").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "0000111099999999").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "0000111100000000").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "0000111100000001").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "0000111100010000").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "0000111122223333").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "0000111199999999").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "0000199900000000").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "0000199999999999").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "0000222200000000").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "0000222200000001").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "1111000000000000").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "1111000000000001").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "2222000000000000").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "2222000000000001").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "3333000000000000").endObject())).actionGet();
        node.client().index(indexRequest("test").type("splitted_hashes")
                .source(jsonBuilder().startObject().field("hash", "3333000000000001").endObject())).actionGet();
        node.client().admin().indices().refresh(refreshRequest()).actionGet();

        CountResponse countResponse;

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(rangeQuery("hash").from("1111000000000000").includeLower(true).to("2222000000000000").includeUpper(true))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("range query no common prefix", countResponse.count(), equalTo(3l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(rangeQuery("hash").from("0000111100000000").includeLower(true).to("0000111100009999").includeUpper(true))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("range query common until last chunk", countResponse.count(), equalTo(2l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(rangeQuery("hash").from("0000111100000000").includeLower(true).to("0000222200000000").includeUpper(true))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("closed range query", countResponse.count(), equalTo(8l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(rangeQuery("hash").from("0000111100000000").includeLower(false).to("0000222200000000").includeUpper(true))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("open start range query", countResponse.count(), equalTo(7l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(rangeQuery("hash").from("0000111100000000").includeLower(true).to("0000222200000000").includeUpper(false))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("open end range query", countResponse.count(), equalTo(7l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(rangeQuery("hash").from("0000111100000000").includeLower(false).to("0000222200000000").includeUpper(false))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("open range query", countResponse.count(), equalTo(6l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(rangeQuery("hash").from("0000111100000000").includeLower(true).to("0000111100000000").includeUpper(true))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("singleton range query", countResponse.count(), equalTo(1l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(rangeQuery("hash").from("0000111100000000").includeLower(true).to("0000111100000000").includeUpper(false))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("empty range query", countResponse.count(), equalTo(0l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(rangeQuery("hash").from(null).includeLower(false).to(null).includeUpper(false))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("-inf;+inf range query", countResponse.count(), equalTo(17l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(rangeQuery("hash").from("000011110000").includeLower(true).to("000022220000").includeUpper(true))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("range query on shorter size complete chunks", countResponse.count(), equalTo(9l));

        countResponse = node.client().count(countRequest("test").types("splitted_hashes").query(rangeQuery("hash").from("00001111000000").includeLower(true).to("00002222000000").includeUpper(true))).actionGet();
        assertThat("successful request", countResponse.failedShards(), equalTo(0));
        assertThat("range query on shorter size incomplete chunks", countResponse.count(), equalTo(7l)); // not 8 because 0000222200000000 is excluded because d0000 is greater that d00
    }

}
