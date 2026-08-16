#!/bin/sh
# Entry point for a DFS node.
#   - Always applies fast-startup JVM flags.
#   - If ENABLE_FAKETIME=1, preloads libfaketime so the chaos harness can inject
#     clock skew at runtime by writing an offset (e.g. "+30s") into
#     FAKETIME_TIMESTAMP_FILE via `docker exec`. Off by default so normal runs
#     are unaffected.
set -e

JVM_OPTS="-XX:TieredStopAtLevel=1 -XX:+UseSerialGC"

if [ "${ENABLE_FAKETIME}" = "1" ]; then
    FAKETIME_LIB=$(find / -name 'libfaketime.so.1' 2>/dev/null | head -1)
    if [ -n "$FAKETIME_LIB" ]; then
        : "${FAKETIME_TIMESTAMP_FILE:=/etc/faketimerc}"
        [ -f "$FAKETIME_TIMESTAMP_FILE" ] || echo "+0" > "$FAKETIME_TIMESTAMP_FILE"
        export LD_PRELOAD="$FAKETIME_LIB"
        export FAKETIME_TIMESTAMP_FILE
        export FAKETIME_NO_CACHE=1
        echo "[entrypoint] libfaketime enabled ($FAKETIME_LIB), skew file $FAKETIME_TIMESTAMP_FILE"
    else
        echo "[entrypoint] ENABLE_FAKETIME=1 but libfaketime.so.1 not found; continuing without it"
    fi
fi

exec java $JVM_OPTS -jar app.jar
