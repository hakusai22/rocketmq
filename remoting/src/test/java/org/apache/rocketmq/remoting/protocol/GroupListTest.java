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

package org.apache.rocketmq.remoting.protocol;

import java.util.HashSet;
import java.util.UUID;
import org.apache.rocketmq.remoting.protocol.body.GroupList;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by guoyao on 2019/2/18.
 */
public class GroupListTest {

    @Test
    public void testSetGet() throws Exception {
        HashSet<String> firstUniqueSet = createUniqueNewSet();
        HashSet<String> secondUniqueSet = createUniqueNewSet();
        assertThat(firstUniqueSet).isNotEqualTo(secondUniqueSet);
        GroupList gl = new GroupList();
        gl.setGroupList(firstUniqueSet);
        assertThat(gl.getGroupList()).isEqualTo(firstUniqueSet);
        assertThat(gl.getGroupList()).isNotEqualTo(secondUniqueSet);
        gl.setGroupList(secondUniqueSet);
        assertThat(gl.getGroupList()).isNotEqualTo(firstUniqueSet);
        assertThat(gl.getGroupList()).isEqualTo(secondUniqueSet);
    }

    private HashSet<String> createUniqueNewSet() {
        HashSet<String> groups = new HashSet<>();
        groups.add(UUID.randomUUID().toString());
        return groups;
    }
}
