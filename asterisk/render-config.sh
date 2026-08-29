#!/bin/sh
set -eu

for template in /opt/hermes-asterisk/*.template; do
    target="/etc/asterisk/$(basename "${template%.template}")"
    envsubst < "$template" > "$target"
done

for config in /opt/hermes-asterisk/*.conf; do
    cp "$config" "/etc/asterisk/$(basename "$config")"
done

exec /usr/local/bin/entrypoint.sh "$@"
