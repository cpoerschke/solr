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
package org.apache.solr.ltr.feature;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.store.rest.ManagedFeatureStore;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class TestPrefetchingFieldValueFeature extends TestRerankBase {
  @Before
  public void before() throws Exception {
    setuptest(false);
  }

  @After
  public void after() throws Exception {
    aftertest();
  }

  @Test
  public void testThatPrefetchingFieldValueFeatureCollectsAllStoredFieldsInFeatureStore() throws Exception {
    ObservingPrefetchingFieldValueFeature.prefetchFieldsObservations.clear();
    final String fstore = "testThatPrefetchingFieldValueFeatureCollectsAllStoredFieldsInFeatureStore";
    loadFeature("storedIntFieldFeature", ObservingPrefetchingFieldValueFeature.class.getName(), fstore,
        "{\"field\":\"storedIntField\"}");

    assertEquals(Set.of(
        Set.of("storedIntField")), ObservingPrefetchingFieldValueFeature.prefetchFieldsObservations);

    ObservingPrefetchingFieldValueFeature.prefetchFieldsObservations.clear();

    loadFeature("storedLongFieldFeature", ObservingPrefetchingFieldValueFeature.class.getName(), fstore,
        "{\"field\":\"storedLongField\"}");

    assertEquals(Set.of(
        Set.of("storedIntField", "storedLongField")), ObservingPrefetchingFieldValueFeature.prefetchFieldsObservations);

    loadFeature("storedFloatFieldFeature", ObservingPrefetchingFieldValueFeature.class.getName(), fstore,
        "{\"field\":\"storedFloatField\"}");

    assertEquals(Set.of(
        Set.of("storedIntField", "storedLongField"),
        Set.of("storedIntField", "storedLongField", "storedFloatField")), ObservingPrefetchingFieldValueFeature.prefetchFieldsObservations);
  }

  @Test
  public void testThatPrefetchingFieldValueFeatureCollectsAllStoredFieldsInItsOwnFeatureStore() throws Exception {
    ObservingPrefetchingFieldValueFeature.prefetchFieldsObservations.clear();
    final String fstore = "testThatPrefetchingFieldValueFeatureCollectsAllStoredFieldsInItsOwnFeatureStore";
    loadFeature("storedIntFieldFeature", ObservingPrefetchingFieldValueFeature.class.getName(), fstore,
        "{\"field\":\"storedIntField\"}");

    assertEquals(Set.of(Set.of("storedIntField")), ObservingPrefetchingFieldValueFeature.prefetchFieldsObservations);

    ObservingPrefetchingFieldValueFeature.prefetchFieldsObservations.clear();

    loadFeature("storedLongFieldFeature", ObservingPrefetchingFieldValueFeature.class.getName(), fstore + "-other",
        "{\"field\":\"storedLongField\"}");

    assertEquals(Set.of(Set.of("storedLongField")), ObservingPrefetchingFieldValueFeature.prefetchFieldsObservations);
  }

  @Test
  public void testThatPrefetchFieldsAreOnlyUpdatedAfterLoadingAllFeatures() throws Exception {
    ObservingPrefetchingFieldValueFeature.prefetchFieldsObservations.clear();
    final String fstore = "feature-store-for-test";
    loadFeature("storedIntField", ObservingPrefetchingFieldValueFeature.class.getName(), fstore,
        "{\"field\":\"storedIntField\"}");

    assertEquals(Set.of(Set.of("storedIntField")), ObservingPrefetchingFieldValueFeature.prefetchFieldsObservations);

    ObservingPrefetchingFieldValueFeature.prefetchFieldsObservations.clear();

    loadFeatures("prefetching_features.json");

    assertEquals(Set.of(Set.of("storedIntField", "storedLongField", "storedFloatField", "storedDoubleField")),
        ObservingPrefetchingFieldValueFeature.prefetchFieldsObservations);
  }

  @Test
  public void testThatPrefetchFieldsAreReturnedInOutputOfFeatureStore() throws Exception {
    final String fstore = "feature-store-for-test";

    // load initial features
    loadFeature("storedIntFieldFeature", ObservingPrefetchingFieldValueFeature.class.getName(), fstore,
        "{\"field\":\"storedIntField\"}");
    loadFeature("storedLongFieldFeature", ObservingPrefetchingFieldValueFeature.class.getName(), fstore,
        "{\"field\":\"storedLongField\"}");

    assertJQ(ManagedFeatureStore.REST_END_POINT,"/featureStores==['"+fstore+"']");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/"+fstore,
        "/features/[0]/name=='storedIntFieldFeature'");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/"+fstore,
        "/features/[0]/params/field=='storedIntField'");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/"+fstore,
        "/features/[0]/params/prefetchFields==['storedLongField']");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/"+fstore,
        "/features/[1]/name=='storedLongFieldFeature'");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/"+fstore,
        "/features/[1]/params/field=='storedLongField'");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/"+fstore,
        "/features/[1]/params/prefetchFields==['storedIntField']");

    // add another feature to test that new field is added to prefetchFields
    loadFeature("storedDoubleFieldFeature", ObservingPrefetchingFieldValueFeature.class.getName(), fstore,
        "{\"field\":\"storedDoubleField\"}");

    assertJQ(ManagedFeatureStore.REST_END_POINT + "/"+fstore,
        "/features/[0]/name=='storedIntFieldFeature'");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/"+fstore,
        "/features/[0]/params/field=='storedIntField'");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/"+fstore,
        "/features/[0]/params/prefetchFields==['storedDoubleField','storedLongField']");

    assertJQ(ManagedFeatureStore.REST_END_POINT + "/"+fstore,
        "/features/[1]/name=='storedLongFieldFeature'");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/"+fstore,
        "/features/[1]/params/field=='storedLongField'");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/"+fstore,
        "/features/[1]/params/prefetchFields==['storedDoubleField','storedIntField']");

    assertJQ(ManagedFeatureStore.REST_END_POINT + "/"+fstore,
        "/features/[2]/name=='storedDoubleFieldFeature'");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/"+fstore,
        "/features/[2]/params/field=='storedDoubleField'");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/"+fstore,
        "/features/[2]/params/prefetchFields==['storedIntField','storedLongField']");
  }

  @Test
  public void testParamsToMapWithoutPrefetchFields() {
    final String fieldName = "field"+random().nextInt(10);
    final Set<String> fieldNameAsSet = Collections.singleton(fieldName);

    final LinkedHashMap<String,Object> params = new LinkedHashMap<>();
    params.put("field", fieldName);

    // create a feature from the parameters
    final PrefetchingFieldValueFeature featureA = (PrefetchingFieldValueFeature) Feature.getInstance(solrResourceLoader,
        PrefetchingFieldValueFeature.class.getName(), "featureName", params);

    // turn the feature back into parameters
    final LinkedHashMap<String,Object> paramsB = featureA.paramsToMap();

    // create feature B from feature A's parameters
    final PrefetchingFieldValueFeature featureB = (PrefetchingFieldValueFeature) Feature.getInstance(solrResourceLoader,
        PrefetchingFieldValueFeature.class.getName(), "featureName", paramsB);

    // do not call equals() because of mismatch between prefetchFields and params
    // (featureA has no prefetchFields-param, featureB has empty prefetchFields-param)
    assertEquals(featureA.getName(), featureB.getName());
    assertEquals(featureA.getField(), featureB.getField());

    assertNull(featureA.getConfiguredPrefetchFields());
    assertTrue(featureB.getConfiguredPrefetchFields().isEmpty());

    assertTrue(featureA.getComputedPrefetchFields().isEmpty());
    assertTrue(featureB.getComputedPrefetchFields().isEmpty());

    assertEquals(fieldNameAsSet, featureA.getEffectivePrefetchFields());
    assertEquals(fieldNameAsSet, featureB.getEffectivePrefetchFields());
  }

  @Test
  public void testParamsToMapWithPrefetchFields() {
    final String fieldName = "field"+random().nextInt(10);
    final Set<String> fieldNameAsSet = Collections.singleton(fieldName);

    final LinkedHashMap<String,Object> params = new LinkedHashMap<>();
    params.put("field", fieldName);

    // create a feature from the parameters
    final PrefetchingFieldValueFeature feature = (PrefetchingFieldValueFeature) Feature.getInstance(solrResourceLoader,
        PrefetchingFieldValueFeature.class.getName(), "featureName", params);

    params.put("prefetchFields", Collections.EMPTY_SET);
    // as yet no prefetch fields have been set (and fieldName is not included in prefetch fields)
    assertEquals(params, feature.paramsToMap());

    // update the feature with the joint prefetch fields of itself and (imaginary) fellow features
    feature.addPrefetchFields(Set.of("foo", fieldName, "bar"));

    params.put("prefetchFields", Set.of("foo", "bar"));
    // fieldName is not included in prefetch fields
    assertEquals(params, feature.paramsToMap());
  }

  @Test
  public void testCompleteParamsToMap() throws Exception {
    {
      final LinkedHashMap<String,Object> params = new LinkedHashMap<>();
      params.put("field", "field"+random().nextInt(10));
      // absent prefetchFields becomes empty prefetchFields
      doTestParamsToMap(PrefetchingFieldValueFeature.class.getName(), params, false /* expectEquals */);
      params.put("prefetchFields", Collections.EMPTY_SET);
      doTestParamsToMap(PrefetchingFieldValueFeature.class.getName(), params, true /* expectEquals */);
    }
    {
      final LinkedHashMap<String,Object> params = new LinkedHashMap<>();
      params.put("field", "field"+random().nextInt(10));
      params.put("prefetchFields", Set.of("field_1", "field_2"));
      // configured prefetchFields are ignored
      doTestParamsToMap(PrefetchingFieldValueFeature.class.getName(), params, false /* expectEquals */);
    }
  }

  /**
   * This class is used to track the content of the prefetchFields and the updates so that we can assert,
   * that the logic to store them works as expected.
   */
  final public static class ObservingPrefetchingFieldValueFeature extends PrefetchingFieldValueFeature {
    public static Set<Set<String>> prefetchFieldsObservations = new HashSet<>();

    public ObservingPrefetchingFieldValueFeature(String name, Map<String, Object> params) {
      super(name, params);
    }

    public void addPrefetchFields(Set<String> fields) {
      super.addPrefetchFields(fields);
      // needed because all feature instances are updated. We just want to track updates with different fields, not all
      // updates of the instances
      prefetchFieldsObservations.add(fields);
    }

    @Override
    public Feature.FeatureWeight createWeight(IndexSearcher searcher, boolean needsScores, SolrQueryRequest request,
                                              Query originalQuery, Map<String, String[]> efi) throws IOException {
      return new PrefetchingFieldValueFeatureWeight(searcher, request, originalQuery, efi);
    }
  }
}
