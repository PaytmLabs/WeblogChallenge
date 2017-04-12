#!/bin/bash

# Workaround https://issues.apache.org/jira/browse/PIG-3953

PIG_USER_CLASSPATH_FIRST=true PIG_CLASSPATH=/usr/lib/pig/pig-0.12.0-cdh5.4.2-withouthadoop.jar pig