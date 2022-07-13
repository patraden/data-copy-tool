# Project packaging container
# Results in fat jar at:
# /data_copy_tool_build/target/scala-2.13/data-copy-tool-assembly-<version>.jar

FROM java:8-jdk
ENV SBT_VERSION 1.6.2
RUN curl -L -o sbt-$SBT_VERSION.zip https://github.com/sbt/sbt/releases/download/v$SBT_VERSION/sbt-$SBT_VERSION.zip
RUN unzip sbt-$SBT_VERSION.zip -d ops
WORKDIR /data_copy_tool_build
ADD ./build.sbt /data_copy_tool_build
ADD ./project/plugins.sbt /data_copy_tool_build/project/plugins.sbt
ADD ./project/build.properties /data_copy_tool_build/project/build.properties
ADD ./src /data_copy_tool_build/src/
RUN /ops/sbt/bin/sbt assembly