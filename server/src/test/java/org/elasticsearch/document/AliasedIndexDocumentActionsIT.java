/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.document;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;

public class AliasedIndexDocumentActionsIT extends DocumentActionsIT {

    @Override
    protected void createIndex() {
        logger.info("Creating index [test1] with alias [test]");
        try {
            client().admin().indices().prepareDelete("test1").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        logger.info("--> creating index test");
        // CRATE_PATCH
        // CrateDB has by default auto_expand_replicas "0-1"
        // for this test to be deterministic we need to set the ES default "false"
        CreateIndexRequest indexRequest = new CreateIndexRequest("test1",
            Settings.builder().put(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, "false").build()
        );
        client().admin().indices().create(indexRequest
                .mapping("type1", "name", "type=keyword,store=true")
                .alias(new Alias("test"))).actionGet();
    }

    @Override
    protected String getConcreteIndexName() {
        return "test1";
    }
}
