/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The SF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.aries.events.mongo;

import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

/** Mongo endpoint configuration. (not used yet) */
@ObjectClassDefinition(
        name        = "Adobe Granite Mongo Distribution - MongoDB endpoint",
        description = "Mongodb URI"
)
public @interface MongoEndpoint {

    @AttributeDefinition(
            name        = "Mongo URI",
            description = "Specifies mongodb URI as it is specified here: https://docs.mongodb.com/manual/reference/connection-string/ "
    )
    String mongoUri() default "mongodb://localhost:27017/aem_distribution";

    @AttributeDefinition(
            name        = "Max Age",
            description = "Log retention time expressed in milliseconds"
    )
    long maxAge() default 1000L * 3600 * 24 * 7; // One week in ms

}
