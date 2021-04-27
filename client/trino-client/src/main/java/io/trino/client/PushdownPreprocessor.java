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

package io.trino.client;

import com.google.common.io.BaseEncoding;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;

public class PushdownPreprocessor
{
    private static String encodeToken = "ENCODE";
    private static String startTag = "<" + encodeToken + ">";
    private static String endTag = "</" + encodeToken + ">";
    private static Pattern encodePattern = Pattern.compile(String.format("(%s.*?%s)", startTag, endTag));
    private static BaseEncoding base32 = BaseEncoding.base32();

    private PushdownPreprocessor()
    {
    }

    //Modernizer will suggest Stringbuilder, which isn't available for appendReplacement until Java 9
    @SuppressModernizer
    public static String preprocess(String query)
    {
        Matcher encodeMatcher = encodePattern.matcher(query);
        StringBuffer sb = new StringBuffer();
        boolean hasMatch = encodeMatcher.find();
        if (!hasMatch) {
            return query;
        }
        int lastGroupEnd = 0;
        List<String> unencodedQueries = new ArrayList<>();
        String queryText = "";
        while (hasMatch) {
            lastGroupEnd = encodeMatcher.end();
            queryText = encodeMatcher.group(1).substring(startTag.length(), encodeMatcher.group(1).length() - endTag.length());
            encodeMatcher.appendReplacement(sb, base32.encode(queryText.getBytes(UTF_8)));
            unencodedQueries.add(queryText);
            hasMatch = encodeMatcher.find();
        }
        sb.append(query, lastGroupEnd, query.length());
        sb.append("\n/* Unencoded Queries");
        for (String unencodeQuery : unencodedQueries) {
            sb.append("\n==============================\n");
            sb.append(unencodeQuery);
        }
        sb.append("\n==============================*/");
        return sb.toString();
    }

    public static String foo(String s)
    {
        return s;
    }
}
