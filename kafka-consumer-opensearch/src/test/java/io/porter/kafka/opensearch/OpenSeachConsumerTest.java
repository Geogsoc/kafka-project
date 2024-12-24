package io.porter.kafka.opensearch;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class OpenSeachConsumerTest {


    @Test
    public void testExtractId() {
        String json = "{\n    \"$schema\": \"/mediawiki/recentchange/1.0.0\",\n    \"meta\":\n    {\n        \"uri\": \"https://commons.wikimedia.org/wiki/Category:All_media_needing_categories_as_of_2024\",\n        \"request_id\": \"4bc32498-590e-47bb-96fa-8aae5c00b91d\",\n        \"id\": \"78f6e1d3-d544-4694-98da-23eec36f0559\",\n        \"dt\": \"2024-12-20T20:19:13Z\",\n        \"domain\": \"commons.wikimedia.org\",\n        \"stream\": \"mediawiki.recentchange\",\n        \"topic\": \"codfw.mediawiki.recentchange\",\n        \"partition\": 0,\n        \"offset\": 1338206791\n    },\n    \"id\": 2690670741,\n    \"type\": \"categorize\",\n    \"namespace\": 14,\n    \"title\": \"Category:All media needing categories as of 2024\",\n    \"title_url\": \"https://commons.wikimedia.org/wiki/Category:All_media_needing_categories_as_of_2024\",\n    \"comment\": \"[[:File:FOSSMeet'2006 23.jpg]] added to category\",\n    \"timestamp\": 1734725953,\n    \"user\": \"Fenton324\",\n    \"bot\": false,\n    \"notify_url\": \"https://commons.wikimedia.org/w/index.php?diff=973185913&oldid=0&rcid=2690670741\",\n    \"server_url\": \"https://commons.wikimedia.org\",\n    \"server_name\": \"commons.wikimedia.org\",\n    \"server_script_path\": \"/w\",\n    \"wiki\": \"commonswiki\",\n    \"parsedcomment\": \"<a href=\\\"/wiki/File:FOSSMeet%272006_23.jpg\\\" title=\\\"File:FOSSMeet&#039;2006 23.jpg\\\">File:FOSSMeet&#039;2006 23.jpg</a> added to category\"\n}";

        String id = OpenSeachConsumer.extractId(json);

        assertEquals("78f6e1d3-d544-4694-98da-23eec36f0559", id);
    }



}