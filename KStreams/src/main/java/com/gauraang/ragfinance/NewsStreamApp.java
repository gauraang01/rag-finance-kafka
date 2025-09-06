package com.gauraang.ragfinance;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.text.TextObjectFactory;
import com.optimaize.langdetect.text.CommonTextObjectFactories;
import com.optimaize.langdetect.text.TextObject;
import com.optimaize.langdetect.i18n.LdLocale;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import io.github.cdimascio.dotenv.Dotenv;
import org.jsoup.Jsoup;
import com.google.common.base.Optional;

import java.util.HashSet;
import java.util.List;
import java.util.Properties;

public class NewsStreamApp {

    public static void main(String[] args) throws Exception {
        // Load .env
        Dotenv dotenv = Dotenv.configure().directory("../").filename(".env").load();
        String bootstrap = dotenv.get("KAFKA_BOOTSTRAP");

        // Kafka Streams properties
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "news-clean-stream-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        ObjectMapper mapper = new ObjectMapper();

        // Setup language detector
        List<LanguageProfile> languageProfiles = new LanguageProfileReader().readAllBuiltIn();
        LanguageDetector detector = LanguageDetectorBuilder.create(NgramExtractors.standard())
                .withProfiles(languageProfiles)
                .build();
        TextObjectFactory textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText();

        // Input stream
        KStream<String, String> rawStream = builder.stream("news-headlines");

        HashSet<String> seenIds = new HashSet<>();

        KStream<String, String> cleanStream = rawStream
                .mapValues(value -> {
                    try {
                        var node = mapper.readTree(value);
                        String id = node.get("id").asText();

                        // Deduplicate
                        if (seenIds.contains(id)) return null;
                        seenIds.add(id);

                        // Strip HTML
                        String content = Jsoup.parse(node.get("content").asText("")).text();
                        String description = Jsoup.parse(node.get("description").asText("")).text();

                        // Detect language
                        TextObject textObject = textObjectFactory.forText(content);
                        Optional<LdLocale> langOpt = detector.detect(textObject);
                        String lang = langOpt.isPresent() ? langOpt.get().getLanguage() : "unknown";

                        // Build cleaned JSON
                        var cleanNode = mapper.createObjectNode();
                        cleanNode.put("schema_version", "1.0");
                        cleanNode.put("id", id);
                        cleanNode.put("source", node.get("source").asText("newsapi"));
                        cleanNode.put("query", node.get("query").asText(""));
                        cleanNode.put("title", node.get("title").asText(""));
                        cleanNode.put("description", description);
                        cleanNode.put("content", content);
                        cleanNode.put("url", node.get("url").asText(""));
                        cleanNode.put("published_at", node.get("published_at").asText(""));
                        cleanNode.put("fetched_at", node.get("fetched_at").asText(""));
                        cleanNode.put("language", lang);

                        return mapper.writeValueAsString(cleanNode);
                    } catch (Exception e) {
                        e.printStackTrace();
                        return null;
                    }
                })
                .filter((key, value) -> value != null);

        // Output cleaned stream to new topic
        cleanStream.to("news-clean");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        System.out.println("NewsCleanStreamApp started and processing messages...");
    }
}
