FROM andrius/asterisk:23.4.1_debian-trixie

COPY config /opt/hermes-asterisk
COPY render-config.sh /usr/local/bin/render-hermes-config.sh
RUN chmod 0755 /usr/local/bin/render-hermes-config.sh

ENTRYPOINT ["/usr/local/bin/render-hermes-config.sh"]
CMD ["/usr/sbin/asterisk", "-vvvdddf", "-T", "-W", "-U", "asterisk", "-p"]
