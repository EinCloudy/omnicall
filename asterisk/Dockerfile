FROM andrius/asterisk:23.4.1_debian-trixie

COPY config /opt/omnicall-asterisk
COPY render-config.sh /usr/local/bin/render-omnicall-config.sh
RUN chmod 0755 /usr/local/bin/render-omnicall-config.sh

ENTRYPOINT ["/usr/local/bin/render-omnicall-config.sh"]
CMD ["/usr/sbin/asterisk", "-vvvdddf", "-T", "-W", "-U", "asterisk", "-p"]
