FROM public.ecr.aws/docker/library/openjdk:11
WORKDIR /app
COPY ./build/libs/debezium-aws-embedded.jar /app/debezium-aws-embedded.jar
CMD java $JAVA_OPTS -jar /app/debezium-aws-embedded.jar $OPTIONS