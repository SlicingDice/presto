/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.jdbc;

public class ShannonDBTypes
{
    public final static int UNIQUEID         =   0;

    public final static int NULL            =   1;

    public final static int JAVA_OBJECT         = 2;

    public final static int INTEGER         =   3;

    public final static int STRING         =   4;

    public final static int BOOLEAN         =   5;

    public final static int DECIMAL         =   6;

    public final static int DATE            =  7;

    public final static int DATETIME            =  8;

    public final static int GEOLOCATION = 9;

    // Prevent instantiation
    private ShannonDBTypes() {}
}
