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

package org.apache.nifi.controller.leader.election;

public interface LeaderElectionManager {
    /**
     * Starts managing leader elections for all registered roles
     */
    void start();

    /**
     * Adds a new role for which a leader is required
     *
     * @param roleName the name of the role
     */
    void register(String roleName);

    /**
     * Adds a new role for which a leader is required
     *
     * @param roleName the name of the role
     * @param listener a listener that will be called when the node gains or relinquishes
     *            the role of leader
     */
    void register(String roleName, LeaderElectionStateChangeListener listener);

    /**
     * Removes the role with the given name from this manager. If this
     * node is the elected leader for the given role, this node will relinquish
     * the leadership role
     *
     * @param roleName the name of the role to unregister
     */
    void unregister(String roleName);

    /**
     * Returns a boolean value indicating whether or not this node
     * is the elected leader for the given role
     *
     * @param roleName the name of the role
     * @return <code>true</code> if the node is the elected leader, <code>false</code> otherwise.
     */
    boolean isLeader(String roleName);

    /**
     * @return <code>true</code> if the manager is stopped, false otherwise.
     */
    boolean isStopped();

    /**
     * Stops managing leader elections and relinquishes the role as leader
     * for all registered roles. If the LeaderElectionManager is later started
     * again, all previously registered roles will still be registered.
     */
    void stop();
}
