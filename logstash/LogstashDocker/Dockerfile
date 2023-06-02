FROM docker.elastic.co/logstash/logstash:8.7.0

# Add your logstash plugins setup here
RUN logstash-plugin install logstash-output-http
RUN logstash-plugin install logstash-filter-json_encode
RUN logstash-plugin install logstash-filter-json



