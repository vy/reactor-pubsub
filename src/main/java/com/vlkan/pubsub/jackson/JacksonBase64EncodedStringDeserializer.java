/*
 * Copyright 2019-2020 Volkan Yazıcı
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permits and
 * limitations under the License.
 */

package com.vlkan.pubsub.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;
import com.fasterxml.jackson.databind.node.TextNode;

import java.io.IOException;
import java.util.Base64;

public class JacksonBase64EncodedStringDeserializer extends StdScalarDeserializer<byte[]> {

    protected JacksonBase64EncodedStringDeserializer() {
        super(byte[].class);
    }

    @Override
    public byte[] deserialize(JsonParser parser, DeserializationContext context) throws IOException {
        TextNode base64EncodedTextNode = parser.getCodec().readTree(parser);
        String base64EncodedText = base64EncodedTextNode.asText();
        return Base64.getDecoder().decode(base64EncodedText);
    }

}
