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
    public final static String UNIQUEID = "uniqueid";
    public final static String BITMAP = "bitmap";
    public final static String NUMERIC = "numeric";
    public final static String DATE = "date";
    public final static String INVERTEDINDEX = "invertedindex";
    public final static String BOOLEAN = "boolean";
    public final static String NUMERICTIMESERIES = "numerictimeseries";
    public final static String TIMESERIES = "timeseries";
    public final static String DATETIME = "datetime";
    public final static String GEOLOCATION = "geolocation";
    public final static String FOREIGNKEY = "foreignkey";

    // Prevent instantiation
    private ShannonDBTypes() {}
}
